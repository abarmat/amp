use amp_data_store::DataStore;
use amp_datasets_registry::DatasetsRegistry;
use common::{datasets_cache::DatasetsCache, udfs::eth_call::EthCallUdfsCache};
use metadata_db::MetadataDb;

/// The controller-admin-datasets context
#[derive(Clone)]
pub struct Ctx {
    pub metadata_db: MetadataDb,
    /// Datasets registry for manifest and version tag operations.
    pub datasets_registry: DatasetsRegistry,
    /// Datasets cache for loading datasets.
    pub datasets_cache: DatasetsCache,
    /// EthCall UDFs cache for eth_call UDF creation.
    pub ethcall_udfs_cache: EthCallUdfsCache,
    /// Object store for output data.
    pub data_store: DataStore,
}
