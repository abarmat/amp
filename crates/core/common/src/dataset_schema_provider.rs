//! Schema provider for a dataset.
//!
//! Provides table and function resolution from a pre-resolved dataset without
//! requiring a data store. Tables are resolved as [`PlanTable`] instances
//! that expose schema information only.

use std::{any::Any, collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{
        AsyncSchemaProvider as TableAsyncSchemaProvider, SchemaProvider as TableSchemaProvider,
        TableProvider,
    },
    error::DataFusionError,
    logical_expr::ScalarUDF,
};
use datasets_common::{dataset::Dataset, table_name::TableName};
use datasets_derived::dataset::Dataset as DerivedDataset;
use parking_lot::RwLock;

use crate::{
    func_catalog::{
        function_provider::{FunctionProvider, ScalarFunctionProvider},
        schema_provider::{
            AsyncSchemaProvider as FuncAsyncSchemaProvider, SchemaProvider as FuncSchemaProvider,
        },
    },
    plan_table::PlanTable,
    udfs::plan::PlanJsUdf,
};

/// Schema provider for a dataset.
///
/// Resolves tables as [`PlanTable`] instances (schema-only, no data access)
/// and functions as planning-phase [`PlanJsUdf`] representations that carry
/// no runtime resources.
pub struct DatasetSchemaProvider {
    schema_name: String,
    dataset: Arc<dyn Dataset>,
    tables: RwLock<BTreeMap<String, Arc<dyn TableProvider>>>,
    functions: RwLock<BTreeMap<String, Arc<ScalarUDF>>>,
}

impl DatasetSchemaProvider {
    /// Creates a new provider for the given dataset and schema name.
    pub(crate) fn new(schema_name: String, dataset: Arc<dyn Dataset>) -> Self {
        Self {
            schema_name,
            dataset,
            tables: RwLock::new(Default::default()),
            functions: RwLock::new(Default::default()),
        }
    }
}

#[async_trait]
impl TableSchemaProvider for DatasetSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.dataset
            .table_names()
            .into_iter()
            .map(|n| n.to_string())
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        // Check cache first
        {
            let tables = self.tables.read();
            if let Some(table) = tables.get(name) {
                return Ok(Some(table.clone()));
            }
        }

        let table_name: TableName = name.parse().map_err(|err| {
            DataFusionError::Plan(format!("Invalid table name '{}': {}", name, err))
        })?;

        // Find table in dataset
        let Some(dataset_table) = self.dataset.get_table(&table_name) else {
            return Ok(None);
        };

        let table_schema = dataset_table.schema().clone();

        let table_provider: Arc<dyn TableProvider> = Arc::new(PlanTable::new(table_schema));

        // Cache table provider
        self.tables
            .write()
            .insert(name.to_string(), table_provider.clone());

        Ok(Some(table_provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        if self.tables.read().contains_key(name) {
            return true;
        }

        let Ok(table_name) = name.parse::<TableName>() else {
            return false;
        };

        self.dataset.has_table(&table_name)
    }
}

#[async_trait]
impl TableAsyncSchemaProvider for DatasetSchemaProvider {
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        <Self as TableSchemaProvider>::table(self, name).await
    }
}

#[async_trait]
impl FuncSchemaProvider for DatasetSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn function_names(&self) -> Vec<String> {
        let functions = self.functions.read();
        functions.keys().cloned().collect()
    }

    async fn function(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FunctionProvider>>, DataFusionError> {
        // Check cache first
        {
            let functions = self.functions.read();
            if let Some(func) = functions.get(name) {
                return Ok(Some(Arc::new(ScalarFunctionProvider::from(func.clone()))));
            }
        }

        // Try to get UDF from derived dataset and build a planning-only UDF.
        let udf: Option<ScalarUDF> = self
            .dataset
            .downcast_ref::<DerivedDataset>()
            .and_then(|d| d.function_by_name(name))
            .map(|function| {
                ScalarUDF::new_from_impl(PlanJsUdf::from_function(
                    name,
                    function,
                    Some(&self.schema_name),
                ))
            });

        if let Some(udf) = udf {
            let udf = Arc::new(udf);
            self.functions.write().insert(name.to_string(), udf.clone());
            return Ok(Some(Arc::new(ScalarFunctionProvider::from(udf))));
        }

        Ok(None)
    }

    /// Returns whether the function is known **from the cache only**.
    ///
    /// This deliberately does not probe the dataset or the store because:
    /// - Derived-dataset UDF lookup (`function_by_name`) is sync but allocates a
    ///   full `ScalarUDF` as a side effect, which is inappropriate for an existence check.
    ///
    /// Callers that need authoritative existence checks should use the async
    /// `function()` method instead.
    fn function_exist(&self, name: &str) -> bool {
        let functions = self.functions.read();
        functions.contains_key(name)
    }
}

#[async_trait]
impl FuncAsyncSchemaProvider for DatasetSchemaProvider {
    async fn function(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn FunctionProvider>>, DataFusionError> {
        <Self as FuncSchemaProvider>::function(self, name).await
    }
}

impl std::fmt::Debug for DatasetSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetSchemaProvider")
            .field("schema_name", &self.schema_name)
            .finish()
    }
}
