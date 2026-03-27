//! Service context

use std::sync::Arc;

use crate::workers::worker_service::WorkerService;

/// The controller-admin-system context
#[derive(Clone)]
pub struct Ctx {
    /// Worker service for querying registered worker nodes.
    pub worker_service: Arc<dyn WorkerService>,
}
