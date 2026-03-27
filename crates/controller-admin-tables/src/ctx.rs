use std::sync::Arc;

use amp_data_store::DataStore;
use amp_datasets_registry::DatasetsRegistry;
use common::datasets_cache::DatasetsCache;
use metadata_db::MetadataDb;

use crate::revisions::revision_guard::RevisionGuard;

/// The controller-admin-tables context
#[derive(Clone)]
pub struct Ctx {
    pub metadata_db: MetadataDb,
    /// Datasets registry for manifest and version tag operations.
    pub datasets_registry: DatasetsRegistry,
    /// Datasets cache for loading datasets.
    pub datasets_cache: DatasetsCache,
    /// Guards destructive revision operations by checking writer job state.
    pub revision_guard: Arc<dyn RevisionGuard>,
    /// Object store for output data.
    pub data_store: DataStore,
}
