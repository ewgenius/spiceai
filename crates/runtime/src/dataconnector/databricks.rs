use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use deltalake::aws::storage::s3_constants::AWS_S3_ALLOW_UNSAFE_RENAME;
use deltalake::protocol::SaveMode;
use deltalake::{open_table_with_storage_options, DeltaOps};
use secrecy::ExposeSecret;
use secrets::Secret;
use serde::Deserialize;
use snafu::prelude::*;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use spicepod::component::dataset::Dataset;

use crate::datapublisher::{AddDataResult, DataPublisher};
use crate::dataupdate::DataUpdate;

use super::DataConnector;

#[derive(Clone)]
pub struct Databricks {
    secret: Arc<Option<Secret>>,
}

#[async_trait]
impl DataConnector for Databricks {
    fn new(
        secret: Option<Secret>,
        _params: Arc<Option<HashMap<String, String>>>,
    ) -> Pin<Box<dyn Future<Output = super::Result<Self>> + Send>>
    where
        Self: Sized,
    {
        // Needed to be able to load the s3:// scheme
        deltalake::aws::register_handlers(None);
        deltalake::azure::register_handlers(None);
        Box::pin(async move {
            Ok(Self {
                secret: Arc::new(secret),
            })
        })
    }

    fn get_all_data(
        &self,
        dataset: &Dataset,
    ) -> Pin<Box<dyn Future<Output = Vec<arrow::record_batch::RecordBatch>> + Send>> {
        let dataset = dataset.clone();
        let secret = Arc::clone(&self.secret);
        Box::pin(async move {
            let ctx = SessionContext::new();

            let table_provider = match get_table_provider(&secret, &dataset).await {
                Ok(provider) => provider,
                Err(e) => {
                    tracing::error!("Failed to get table provider: {}", e);
                    return vec![];
                }
            };

            let _ = ctx.register_table("temp_table", table_provider);

            let sql = "SELECT * FROM temp_table;";

            let df = match ctx.sql(sql).await {
                Ok(df) => df,
                Err(e) => {
                    tracing::error!("Failed to execute query: {}", e);
                    return vec![];
                }
            };

            df.collect().await.unwrap_or_else(|e| {
                tracing::error!("Failed to collect results: {}", e);
                vec![]
            })
        })
    }

    fn has_table_provider(&self) -> bool {
        true
    }

    async fn get_table_provider(
        &self,
        dataset: &Dataset,
    ) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
        get_table_provider(&self.secret, dataset).await
    }

    fn get_data_publisher(&self) -> Option<Box<dyn DataPublisher>> {
        Some(Box::new(self.clone()))
    }
}

impl DataPublisher for Databricks {
    fn add_data(&self, dataset: Arc<Dataset>, data_update: DataUpdate) -> AddDataResult {
        Box::pin(async move {
            let delta_table = get_delta_table(&self.secret, &dataset).await?;

            let _ = DeltaOps(delta_table)
                .write(data_update.data)
                .with_save_mode(SaveMode::Append)
                .await?;

            Ok(())
        })
    }

    fn name(&self) -> &str {
        "Databricks"
    }
}

async fn get_table_provider(
    secret: &Arc<Option<Secret>>,
    dataset: &Dataset,
) -> std::result::Result<Arc<dyn datafusion::datasource::TableProvider>, super::Error> {
    let delta_table: deltalake::DeltaTable = get_delta_table(secret, dataset).await?;

    Ok(Arc::new(delta_table))
}

async fn get_delta_table(
    secret: &Arc<Option<Secret>>,
    dataset: &Dataset,
) -> std::result::Result<deltalake::DeltaTable, super::Error> {
    let table_uri = resolve_table_uri(dataset, secret)
        .await
        .context(super::UnableToGetTableProviderSnafu)?;

    let mut storage_options = HashMap::new();
    if let Some(secret) = secret.as_ref() {
        for (key, value) in secret.iter() {
            if key == "token" {
                continue;
            }
            storage_options.insert(key.to_string(), value.expose_secret().clone());
        }
    };
    storage_options.insert(AWS_S3_ALLOW_UNSAFE_RENAME.to_string(), "true".to_string());

    let delta_table = open_table_with_storage_options(table_uri, storage_options)
        .await
        .boxed()
        .context(super::UnableToGetTableProviderSnafu)?;

    Ok(delta_table)
}

#[derive(Deserialize)]
struct DatabricksTablesApiResponse {
    storage_location: String,
}

pub async fn resolve_table_uri(
    dataset: &Dataset,
    secret: &Arc<Option<Secret>>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = match &dataset.params {
        None => return Err("Dataset params not found".into()),
        Some(params) => match params.get("endpoint") {
            Some(val) => val,
            None => return Err("Endpoint not specified in dataset params".into()),
        },
    };

    let table_name = dataset.path();

    let mut token = "Token not found in auth provider";
    if let Some(secret) = secret.as_ref() {
        if let Some(token_secret_val) = secret.get("token") {
            token = token_secret_val;
        };
    };

    let url = format!(
        "{}/api/2.1/unity-catalog/tables/{}",
        endpoint.trim_end_matches('/'),
        table_name
    );

    let client = reqwest::Client::new();
    let response = client.get(&url).bearer_auth(token).send().await?;

    if response.status().is_success() {
        let api_response: DatabricksTablesApiResponse = response.json().await?;
        Ok(api_response.storage_location)
    } else {
        Err(format!(
            "Failed to retrieve databricks table URI. Status: {}",
            response.status()
        )
        .into())
    }
}
