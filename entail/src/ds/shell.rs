use super::super::*;

use google_datastore1::api::{
    BeginTransactionRequest, CommitRequest, LookupRequest, ReadOptions, ReadWrite, RollbackRequest,
    RunQueryRequest, TransactionOptions,
};
use google_datastore1::yup_oauth2::{
    ApplicationDefaultCredentialsAuthenticator, ApplicationDefaultCredentialsFlowOpts,
    authenticator::ApplicationDefaultCredentialsTypes,
};
use google_datastore1::{Datastore, common::NoToken};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use std::error::Error;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Clone)]
pub struct DatastoreShell {
    pub project_id: String,
    pub hub: Arc<Datastore<HttpsConnector<HttpConnector>>>,
    pub database_id: Option<String>,
    pub transaction: Option<Vec<u8>>,
}

fn simple_error<T>(
    s: Cow<'static, str>,
    error: google_datastore1::Error,
) -> Result<T, EntailError> {
    Err(EntailError {
        message: s,
        ds_error: Some(error),
    })
}

impl DatastoreShell {
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
            Err(err) => simple_error("Lookup error".into(), err),
        }
    }

    pub async fn get_all(&self, keys: &[ds::Key]) -> Result<Vec<ds::Entity>, EntailError> {
        let mut native_keys = keys.iter().map(|key| key.to_api()).collect();
        loop {
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
                    let e: Vec<ds::Entity> = lr
                        .found
                        .unwrap_or_default()
                        .into_iter()
                        .map(|er| er.entity.unwrap().into())
                        .collect();
                    if deferred.is_empty() {
                        return Ok(e);
                    } else {
                        native_keys = deferred;
                    }
                }
                Err(err) => {
                    return simple_error("Lookup error".into(), err);
                }
            }
        }
    }

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
            Err(err) => simple_error("Query error".into(), err),
        }
    }

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
        let response = self
            .hub
            .projects()
            .commit(request, &self.project_id)
            .doit()
            .await;
        match response {
            Ok((_, result)) => Ok(result.into()),
            Err(err) => simple_error("Commit error".into(), err),
        }
    }

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
            Err(err) => simple_error("BeginTransaction error".into(), err),
        }
    }

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
            Err(err) => simple_error("Rollback error".into(), err),
        }
    }
}

pub struct TransactionShell {
    pub ds: DatastoreShell,
    pub active: bool,
}

impl<'a> Deref for TransactionShell {
    type Target = DatastoreShell;
    fn deref(&self) -> &Self::Target {
        &self.ds
    }
}

impl TransactionShell {
    pub async fn commit(
        &mut self,
        batch: ds::MutationBatch,
    ) -> Result<ds::MutationResponse, EntailError> {
        let result = self.ds.commit(batch).await;
        if result.is_ok() {
            self.active = false;
        }
        result
    }

    pub async fn rollback(&mut self) -> Result<(), EntailError> {
        let result = self.ds.rollback(&None).await;
        if result.is_ok() {
            self.active = false;
        }
        result
    }
}

impl From<DatastoreShell> for TransactionShell {
    fn from(ds: DatastoreShell) -> Self {
        Self {  ds, active: true }
    }
}
