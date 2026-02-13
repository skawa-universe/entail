use super::super::*;

use google_datastore1::api::{
    AllocateIdsRequest, BeginTransactionRequest, CommitRequest, LookupRequest, ReadOptions,
    ReadWrite, ReserveIdsRequest, RollbackRequest, RunQueryRequest, TransactionOptions,
};
use google_datastore1::yup_oauth2::{
    ApplicationDefaultCredentialsAuthenticator, ApplicationDefaultCredentialsFlowOpts,
    authenticator::ApplicationDefaultCredentialsTypes,
};
use google_datastore1::{Datastore, common::NoToken};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use std::borrow::Borrow;
use std::error::Error;
use std::sync::Arc;

/// A shell around google_datastore1's Datastore service that simplifies access to the
/// Cloud Datastore API.
///
/// A `DatastoreShell` instance can operate in one of two modes:
/// 1. **Standalone:** It handles a single, implicit transaction for each operation.
/// 2. **Transactional:** It is tied to a specific, ongoing transaction. These
///    instances are created by calling `begin_transaction` on a standalone shell and
///    are used to perform a series of related operations within a single atomic unit.
///
/// You cannot directly create a transactional `DatastoreShell` instance.
#[derive(Clone)]
pub struct DatastoreShell {
    pub project_id: String,
    pub hub: Arc<Datastore<HttpsConnector<HttpConnector>>>,
    pub database_id: Option<String>,
    pub transaction: Option<Vec<u8>>,
}

fn simple_error<T>(
    kind: EntailErrorKind,
    s: impl Into<Cow<'static, str>>,
    error: google_datastore1::Error,
) -> Result<T, EntailError> {
    Err(EntailError {
        kind,
        message: s.into(),
        ds_error: Some(error),
    })
}

impl DatastoreShell {
    /// Initializes a new `DatastoreShell` instance.
    ///
    /// The shell's behavior depends on the `in_cloud` parameter:
    /// - If `in_cloud` is `true`, it assumes a Cloud Run environment and uses the
    ///   associated service account for authentication.
    /// - If `in_cloud` is `false`, it assumes a local Datastore emulator is running
    ///   and omits the authorization header.
    ///
    /// ## Parameters
    /// - `project_id`: The ID of the Google Cloud project.
    /// - `in_cloud`: A boolean indicating whether the application is running in a
    ///   cloud environment (e.g., Cloud Run) or locally.
    /// - `database_id`: An optional database ID.
    ///
    /// ## Returns
    /// A `Result` containing the initialized `DatastoreShell` or an error.
    pub async fn new(
        project_id: &str,
        in_cloud: bool,
        database_id: Option<String>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let https = HttpsConnectorBuilder::new()
            .with_native_roots()
            .unwrap()
            .https_or_http()
            .enable_all_versions()
            .build();
        let hyper_client = Client::builder(TokioExecutor::new()).build(https);

        let hub = if in_cloud {
            let opts = ApplicationDefaultCredentialsFlowOpts::default();
            let auth = match ApplicationDefaultCredentialsAuthenticator::builder(opts).await {
                ApplicationDefaultCredentialsTypes::InstanceMetadata(auth) => auth
                    .build()
                    .await
                    .expect("Unable to create instance metadata authenticator"),
                ApplicationDefaultCredentialsTypes::ServiceAccount(auth) => auth
                    .build()
                    .await
                    .expect("Unable to create service account authenticator"),
            };
            Datastore::new(hyper_client, auth)
        } else {
            let mut hub = Datastore::new(hyper_client, NoToken);
            let emulator_host = std::env::var("DATASTORE_EMULATOR_HOST")
                .unwrap_or_else(|_| "http://localhost:8393".to_string());
            hub.base_url(format!("{}/", emulator_host));
            hub.root_url(format!("{}/", emulator_host));
            hub
        };

        Ok(DatastoreShell {
            project_id: project_id.to_string(),
            hub: Arc::new(hub),
            database_id,
            transaction: None,
        })
    }

    fn build_read_options(&self) -> ReadOptions {
        ReadOptions {
            read_consistency: if self.transaction.is_none() {
                Some("STRONG".into())
            } else {
                None
            },
            transaction: self.transaction.clone(),
            ..Default::default()
        }
    }

    /// Fetches a single entity from Datastore by its key.
    ///
    /// ## Parameters
    /// - `key`: The `Key` of the entity to retrieve.
    ///
    /// ## Returns
    /// A `Result` containing `Some(Entity)` if found, `None` if not found,
    /// or an `EntailError` if the operation fails.
    pub async fn get_single(&self, key: ds::Key) -> Result<Option<ds::Entity>, EntailError> {
        let native_key = key.into();
        let lookup = LookupRequest {
            database_id: self.database_id.clone(),
            keys: Some(vec![native_key]),
            read_options: Some(self.build_read_options()),
            ..Default::default()
        };
        let response = self
            .hub
            .projects()
            .lookup(lookup, &self.project_id)
            .doit()
            .await;
        match response {
            Ok((_, result)) => {
                let e: Option<ds::Entity> = result
                    .found
                    .and_then(|e| e.into_iter().next())
                    .and_then(|er| er.entity.map(|e| e.into()));
                Ok(e)
            }
            Err(err) => simple_error(EntailErrorKind::RequestFailure, "Lookup error", err),
        }
    }

    /// Fetches multiple entities from Datastore by a list of keys.
    ///
    /// This method is more efficient than fetching entities one by one.
    ///
    /// ## Parameters
    /// - `keys`: A collection of complete `Key`s to retrieve. This parameter is highly
    ///   flexible:
    ///   * You can pass a **container of keys** (e.g., `Vec<Key>`) to consume the container and all
    ///     keys within it.
    ///   * You can pass an **address of a container** (e.g., `&[Key]`) to keep the container and
    ///     the keys.
    ///   * You can pass a **container of key references** (e.g., `Vec<&Key>`) where the container
    ///     itself is consumed, but the referenced key objects are retained by the caller.
    ///
    /// ## Returns
    /// A `Result` containing a `Vec<Entity>` corresponding to the input keys.
    /// The order of the entities in the returned vector is not guaranteed to match
    /// the order of the keys in the input slice. If an entity is not found,
    /// it's omitted from the vector.
    pub async fn get_all<I>(&self, keys: I) -> Result<Vec<ds::Entity>, EntailError>
    where
        I: IntoIterator,
        I::Item: Borrow<ds::Key>,
    {
        let mut native_keys: Vec<google_datastore1::api::Key> =
            keys.into_iter().map(|key| key.borrow().to_api()).collect();
        if native_keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut rest = if native_keys.len() > 1000 {
            native_keys.split_off(1000)
        } else {
            Vec::new()
        };
        let mut result = Vec::new();
        loop {
            if !rest.is_empty() && native_keys.len() < 1000 {
                let space = 1000 - native_keys.len();
                let start = rest.len().saturating_sub(space);
                native_keys.extend(rest.drain(start..));
            }
            let lookup = LookupRequest {
                database_id: self.database_id.clone(),
                read_options: Some(self.build_read_options()),
                keys: Some(native_keys),
                ..Default::default()
            };
            let response = self
                .hub
                .projects()
                .lookup(lookup, &self.project_id)
                .doit()
                .await;
            match response {
                Ok((_, lr)) => {
                    let deferred = lr.deferred.unwrap_or_default();
                    result.extend(lr
                        .found
                        .unwrap_or_default()
                        .into_iter()
                        .map(|er| er.entity.unwrap().into()));
                    if deferred.is_empty() && rest.is_empty() {
                        return Ok(result);
                    } else {
                        native_keys = deferred;
                    }
                }
                Err(err) => {
                    return simple_error(EntailErrorKind::RequestFailure, "Lookup error", err);
                }
            }
        }
    }

    /// Runs a Datastore query.
    ///
    /// This method executes a user-defined query against the Datastore.
    ///
    /// ## Parameters
    /// - `query`: The `Query` object specifying the kind, filters, and projections.
    ///
    /// ## Returns
    /// A `Result` containing a `QueryResult<Entity>` which holds the fetched
    /// entities and cursor information, or an `EntailError` on failure.
    pub async fn run_query(
        &self,
        query: ds::Query,
    ) -> Result<ds::QueryResult<ds::Entity>, EntailError> {
        let request = RunQueryRequest {
            database_id: self.database_id.clone(),
            read_options: Some(self.build_read_options()),
            query: Some(query.into()),
            ..Default::default()
        };
        let response = self
            .hub
            .projects()
            .run_query(request, &self.project_id)
            .doit()
            .await;
        match response {
            Ok((_, result)) => Ok(result.batch.unwrap_or_default().into()),
            Err(err) => simple_error(EntailErrorKind::RequestFailure, "Query error", err),
        }
    }

    /// Commits a batch of mutations to the Datastore.
    ///
    /// This method applies a set of inserts, updates, upserts, or deletes.
    ///
    /// The operation is executed as either a single atomic operation or with a
    /// best-effort approach, depending on whether the instance is tied to a transaction.
    ///
    /// **Note:** If this `DatastoreShell` instance is tied to a transaction, this
    /// operation will automatically end that transaction.
    ///
    /// ## Parameters
    /// - `batch`: A `MutationBatch` containing the mutations to be applied.
    ///
    /// ## Returns
    /// A `Result` containing a `MutationResponse` with the results of the commit,
    /// or an `EntailError` on failure.
    pub async fn commit(
        &self,
        batch: ds::MutationBatch,
    ) -> Result<ds::MutationResponse, EntailError> {
        let request = CommitRequest {
            database_id: self.database_id.clone(),
            mode: Some(
                self.transaction
                    .as_ref()
                    .map(|_| "TRANSACTIONAL")
                    .unwrap_or("NON_TRANSACTIONAL")
                    .to_string(),
            ),
            mutations: Some(batch.into()),
            transaction: self.transaction.clone(),
            ..Default::default()
        };
        if request
            .mutations
            .as_ref()
            .filter(|mutations| !mutations.is_empty())
            .is_none()
        {
            return Ok(ds::MutationResponse::default());
        }
        let response = self
            .hub
            .projects()
            .commit(request, &self.project_id)
            .doit()
            .await;
        match response {
            Ok((_, result)) => Ok(result.into()),
            Err(err) => simple_error(EntailErrorKind::RequestFailure, "Commit error", err),
        }
    }

    /// Begins a new transaction.
    ///
    /// This method creates a new transaction and returns a new `DatastoreShell`
    /// instance tied to it. All subsequent operations on the returned instance
    /// will be part of this transaction.
    ///
    /// ## Parameters
    /// - `previous`: An optional byte vector representing a previous transaction ID
    ///   to be retried. Use `None` for a new transaction.
    ///
    /// ## Returns
    /// A `Result` containing a new `DatastoreShell` instance for the transaction,
    /// or an `EntailError` if the transaction could not be started.
    pub async fn begin_transaction(&self, previous: &Option<Vec<u8>>) -> Result<Self, EntailError> {
        let request = BeginTransactionRequest {
            database_id: self.database_id.clone(),
            transaction_options: Some(TransactionOptions {
                read_write: Some(ReadWrite {
                    previous_transaction: previous.clone(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let response = self
            .hub
            .projects()
            .begin_transaction(request, &self.project_id)
            .doit()
            .await;
        match response {
            Ok((_, result)) => Ok(Self {
                transaction: result.transaction,
                ..self.clone()
            }),
            Err(err) => simple_error(
                EntailErrorKind::RequestFailure,
                "Begin transaction error",
                err,
            ),
        }
    }

    /// Rolls back an ongoing transaction.
    ///
    /// ## Parameters
    /// - `transaction`: An optional byte vector representing the transaction ID to
    ///   rollback. `None` will roll back the current transaction associated with
    ///   the `DatastoreShell` instance.
    ///
    /// ## Returns
    /// A `Result` indicating success (`()`) or an `EntailError` on failure.
    pub async fn rollback(&self, transaction: &Option<Vec<u8>>) -> Result<(), EntailError> {
        let request = RollbackRequest {
            database_id: self.database_id.clone(),
            transaction: transaction.clone().or_else(|| self.transaction.clone()),
            ..Default::default()
        };
        if request.transaction.is_none() {
            return Ok(());
        }
        let response = self
            .hub
            .projects()
            .rollback(request, &self.project_id)
            .doit()
            .await;
        match response {
            Ok(_) => Ok(()),
            Err(err) => simple_error(EntailErrorKind::RequestFailure, "Rollback error", err),
        }
    }

    /// Allocates unique numeric IDs for a batch of incomplete Keys.
    ///
    /// This is useful for obtaining IDs for new entities before performing the actual insert
    /// or for creating a complete key path that can be referenced by other entities.
    ///
    /// ## Parameters
    /// - `incomplete_keys`: A collection of `Key`s for which to allocate IDs. Each `Key` **must** be
    ///   incomplete (i.e., lacking an ID or name component) and must not be reserved/read-only.
    ///   This parameter is highly flexible:
    ///   * You can pass a **container of keys** (e.g., `Vec<Key>`) to consume the container and all
    ///     keys within it.
    ///   * You can pass an **address of a container** (e.g., `&[Key]`) to keep the container and
    ///     the keys.
    ///   * You can pass a **container of key references** (e.g., `Vec<&Key>`) where the container
    ///     itself is consumed, but the referenced key objects are retained by the caller.
    ///
    /// ## Returns
    /// A [`Result`] containing a `Vec` of **complete** [`ds::Key`]s, where each element
    /// corresponds to the input Key but now includes a newly allocated ID.
    /// The result is wrapped in an [`EntailError`] if the underlying API call fails.
    pub async fn allocate_ids<I>(&self, incomplete_keys: I) -> Result<Vec<ds::Key>, EntailError>
    where
        I: IntoIterator,
        I::Item: Borrow<ds::Key>,
    {
        let keys: Vec<google_datastore1::api::Key> = incomplete_keys
            .into_iter()
            .map(|key| key.borrow().to_api())
            .collect();
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let request = AllocateIdsRequest {
            database_id: self.database_id.clone(),
            keys: Some(keys),
        };
        let response = self
            .hub
            .projects()
            .allocate_ids(request, &self.project_id)
            .doit()
            .await;
        match response {
            Ok((_, result)) => Ok(result
                .keys
                .unwrap_or_default()
                .into_iter()
                .map(ds::Key::from)
                .collect()),
            Err(err) => simple_error(EntailErrorKind::RequestFailure, "Allocate IDs error", err),
        }
    }

    /// Reserves a batch of Keys with numeric IDs, preventing them from being
    /// automatically allocated by Cloud Datastore.
    ///
    /// This is typically used to ensure that a set of known, numeric IDs remains available
    /// for manual insertion.
    ///
    /// ## Parameters
    /// - `id_keys`: A collection of `Key`s that must have **complete key paths** with **numeric IDs**
    ///   (the `id` component must be set) to be reserved.
    ///   This parameter is highly flexible:
    ///   * You can pass a **container of keys** (e.g., `Vec<Key>`) to consume the container and all
    ///     keys within it.
    ///   * You can pass an **address of a container** (e.g., `&[Key]`) to keep the container and
    ///     the keys.
    ///   * You can pass a **container of key references** (e.g., `Vec<&Key>`) where the container
    ///     itself is consumed, but the referenced key objects are retained by the caller.
    ///
    /// ## Returns
    /// A [`Result`] that is `Ok(())` on success, or an [`EntailError`] if the underlying
    /// API call fails (e.g., if one of the Keys is invalid or the reservation fails).
    pub async fn reserve_ids<I>(&self, id_keys: I) -> Result<(), EntailError>
    where
        I: IntoIterator,
        I::Item: Borrow<ds::Key>,
    {
        let keys: Vec<google_datastore1::api::Key> = id_keys
            .into_iter()
            .map(|key| key.borrow().to_api())
            .collect();
        if keys.is_empty() {
            return Ok(());
        }
        let request = ReserveIdsRequest {
            database_id: self.database_id.clone(),
            keys: Some(keys),
        };
        let response = self
            .hub
            .projects()
            .reserve_ids(request, &self.project_id)
            .doit()
            .await;
        match response {
            Ok(_) => Ok(()),
            Err(err) => simple_error(EntailErrorKind::RequestFailure, "Reserve IDs error", err),
        }
    }
}
