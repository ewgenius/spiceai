/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// use std::{collections::HashMap, sync::Arc};

// use datafusion::catalog::TableProvider;
use duckdb::Connection;
use snafu::prelude::*;

use async_trait::async_trait;
use runtime::{
    // component::dataset::Dataset,
    // dataconnector::{create_new_connector, ConnectorParamsBuilder, DataConnectorError},
    extension::{Error as ExtensionError, Extension, ExtensionFactory, ExtensionManifest, Result},
    // federated_table::FederatedTable,
    secrets::Secrets,
    Runtime,
};
// use tokio::sync::RwLock;

// #[derive(Debug, Snafu)]
// pub enum Error {
//     #[snafu(display("Unable to get read table provider"))]
//     NoReadProvider {},

//     #[snafu(display("Unable to create data connector"))]
//     UnableToCreateDataConnector {
//         source: Box<dyn std::error::Error + Sync + Send>,
//     },
//     // #[snafu(display("Unable to create source table provider"))]
//     // UnableToCreateSourceTableProvider { source: DataConnectorError },
// }

pub struct TpchExtension {
    manifest: ExtensionManifest,
}

impl TpchExtension {
    #[must_use]
    pub fn new(manifest: ExtensionManifest) -> Self {
        TpchExtension { manifest }
    }

    // async fn register_tpch_table(&self, runtime: &Runtime, name: &str, path: &str) -> Result<()> {
    //     let dataset = get_duckdb_dataset(name, path)
    //         .boxed()
    //         .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;

    //     let table = create_tpch_table(name, &dataset, runtime.secrets())
    //         .await
    //         .boxed()
    //         .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;

    //     runtime
    //         .datafusion()
    //         .register_table(Arc::new(dataset), table)
    //         .await
    //         .boxed()
    //         .map_err(|e| runtime::extension::Error::UnableToStartExtension { source: e })?;

    //     Ok(())
    // }
}

impl Default for TpchExtension {
    fn default() -> Self {
        TpchExtension::new(ExtensionManifest::default())
    }
}

#[async_trait]
impl Extension for TpchExtension {
    fn name(&self) -> &'static str {
        "tpch"
    }

    async fn initialize(&mut self, runtime: &Runtime) -> Result<()> {
        if !self.manifest.enabled {
            return Ok(());
        }

        let path = self
            .manifest
            .params
            .get("path")
            .map_or(String::from(".spice/data/tpch.db"), |v| v.to_string());

        let scale_factor = self
            .manifest
            .params
            .get("scale_factor")
            .map_or(String::from("1"), |v| v.to_string());

        println!("TPCH extension params: {:?}", self.manifest.params);
        println!("TPCH extension is loading...");

        let connection = Connection::open(path.clone())
            .boxed()
            .map_err(|source| ExtensionError::UnableToInitializeExtension { source })?;

        let query = format!(
            r"
            INSTALL tpch;
            LOAD tpch;
            CALL dbgen(sf = {scale_factor});"
        );

        connection
            .execute_batch(&query.as_str())
            .boxed()
            .map_err(|source| ExtensionError::UnableToInitializeExtension { source })?;

        connection
            .close()
            .map_err(|(_, err)| ExtensionError::UnableToInitializeExtension {
                source: Box::new(err),
            })?;

        println!("TPCH extension is loaded");

        // let secrets = runtime.secrets();

        // let tpch_part_provider = get_tpch_table_provider("tpch.part", path.as_str(), secrets);

        // let dataset_name = "tpch.part";

        // let mut dataset = Dataset::try_new("duckdb:part".to_string(), dataset_name)
        //     .boxed()
        //     .map_err(|source| ExtensionError::UnableToInitializeExtension { source })?;

        // let mut params = HashMap::new();
        // params.insert("duckdb_open".to_string(), path.to_string());
        // dataset.params = params;

        // let connector_params = ConnectorParamsBuilder::new(dataset_name.into(), (&dataset).into())
        //     .build(runtime.secrets())
        //     .await
        //     .context(UnableToCreateDataConnectorSnafu)
        //     .boxed()
        //     .map_err(|source| ExtensionError::UnableToInitializeExtension { source })?;

        // let data_connector = create_new_connector("duckdb", connector_params)
        //     .await
        //     .ok_or_else(|| NoReadWriteProviderSnafu {}.build())?
        //     .context(UnableToCreateDataConnectorSnafu)?;

        // // .boxed()
        // // .map_err(|source| ExtensionError::UnableToInitializeExtension { source })?;

        // let table_provider = data_connector
        //     .read_provider(&dataset)
        //     .await
        //     .ok_or_else(|| NoReadWriteProviderSnafu {}.build())?
        //     .boxed()
        //     .map_err(|source| ExtensionError::UnableToInitializeExtension { source })?;

        Ok(())
    }

    async fn on_start(&self, _runtime: &Runtime) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct TpchExtensionFactory {
    manifest: ExtensionManifest,
}

impl TpchExtensionFactory {
    #[must_use]
    pub fn new(manifest: ExtensionManifest) -> Self {
        TpchExtensionFactory { manifest }
    }
}

impl ExtensionFactory for TpchExtensionFactory {
    fn create(&self) -> Box<dyn Extension> {
        Box::new(TpchExtension {
            manifest: self.manifest.clone(),
        })
    }
}

// async fn create_tpch_table(
//     name: &str,
//     dataset: &Dataset,
//     secrets: Arc<RwLock<Secrets>>,
// ) -> Result<FederatedTable, Error> {
//     let tpch_part_provider = get_tpch_table_provider(name, dataset, secrets).await?;

//     let federated_table = FederatedTable::new(tpch_part_provider);

//     Ok(federated_table)
// }

// async fn get_tpch_table_provider(
//     name: &str,
//     dataset: &Dataset,
//     secrets: Arc<RwLock<Secrets>>,
// ) -> Result<Arc<dyn TableProvider>, Error> {
//     let connector_params = ConnectorParamsBuilder::new(name.into(), (dataset).into())
//         .build(secrets)
//         .await
//         .context(UnableToCreateDataConnectorSnafu)?;

//     let data_connector = create_new_connector("duckdb", connector_params)
//         .await
//         .ok_or_else(|| NoReadProviderSnafu {}.build())?
//         .context(UnableToCreateDataConnectorSnafu)?;

//     let table_provider = data_connector
//         .read_provider(&dataset)
//         .await
//         .context(UnableToCreateSourceTableProviderSnafu)?;

//     Ok(table_provider)
// }

// fn get_duckdb_dataset(name: &str, path: &str) -> Result<Dataset, Error> {
//     let mut dataset = Dataset::try_new("duckdb:part".to_string(), name)
//         .boxed()
//         .context(UnableToCreateDataConnectorSnafu)?;

//     let mut params = HashMap::new();
//     params.insert("duckdb_open".to_string(), path.to_string());
//     dataset.params = params;

//     Ok(dataset)
// }
