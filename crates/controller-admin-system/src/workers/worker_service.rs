//! Trait abstraction for worker data access

use amp_worker_core::node_id::NodeId;
use async_trait::async_trait;
use metadata_db::workers::Worker;

/// Trait for querying worker information
///
/// Abstracts over the scheduler's worker query methods so the handlers
/// do not depend directly on the admin-api `Scheduler` trait.
#[async_trait]
pub trait WorkerService: Send + Sync {
    /// List all registered workers
    async fn list_workers(&self) -> Result<Vec<Worker>, WorkerServiceError>;

    /// Get a worker by its node ID
    ///
    /// Returns `None` if no worker with the given ID exists.
    async fn get_worker_by_id(&self, id: &NodeId) -> Result<Option<Worker>, WorkerServiceError>;
}

/// Error returned by [`WorkerService`] operations
///
/// This occurs when:
/// - Database connection fails or is lost during the query
/// - Query execution encounters an internal database error
/// - Connection pool is exhausted or unavailable
#[derive(Debug, thiserror::Error)]
#[error("worker service error")]
pub struct WorkerServiceError(#[source] pub metadata_db::Error);
