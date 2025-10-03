use crate::ds::MutationBatch;

use super::super::*;

use google_datastore1::api::{CommitRequest, LookupRequest, ReadOptions, RunQueryRequest};
use google_datastore1::yup_oauth2::{
    ApplicationDefaultCredentialsAuthenticator, ApplicationDefaultCredentialsFlowOpts,
    authenticator::ApplicationDefaultCredentialsTypes,
};
use google_datastore1::{Datastore, common::NoToken};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use std::error::Error;
use std::sync::Arc;

#[derive(Clone)]
pub struct DatastoreShell {
    pub project_id: String,
    pub hub: Arc<Datastore<HttpsConnector<HttpConnector>>>,
    pub database_id: Option<String>,
    pub transaction: Option<Vec<u8>>,
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
            read_consistency: Some("STRONG".into()),
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
            Ok(result) => {
                let e: Option<ds::Entity> = result
                    .1
                    .found
                    .and_then(|e| e.into_iter().next())
                    .and_then(|er| er.entity.map(|e| e.into()));
                Ok(e)
            }
            Err(err) => Err(EntailError {
                message: "Lookup error".into(),
                ds_error: Some(err),
            }),
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
                Ok(result) => {
                    let lr = result.1;
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
                    return Err(EntailError {
                        message: "Lookup error".into(),
                        ds_error: Some(err),
                    });
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
            Ok(result) => Ok(result.1.batch.unwrap_or_default().into()),
            Err(err) => Err(EntailError {
                message: "Query error".into(),
                ds_error: Some(err),
            }),
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
            Ok(result) => Ok(result.1.into()),
            Err(err) => Err(EntailError {
                message: "Commit error".into(),
                ds_error: Some(err),
            }),
        }
    }
}
