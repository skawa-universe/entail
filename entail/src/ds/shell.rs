use super::super::*;

use google_datastore1::api::{LookupRequest, ReadOptions};
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

pub struct DatastoreShell {
    pub project_id: String,
    pub hub: Arc<Datastore<HttpsConnector<HttpConnector>>>,
    pub database_id: Option<String>,
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
        })
    }

    pub async fn get_single(&self, key: ds::Key) -> Result<Option<ds::Entity>, EntailError> {
        let native_key = key.into();
        let lookup = LookupRequest {
            database_id: self.database_id.clone(),
            keys: Some(vec![native_key]),
            read_options: Some(ReadOptions {
                read_consistency: Some("STRONG".into()),
                ..Default::default()
            }),
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
}
