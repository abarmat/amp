use async_trait::async_trait;
use metadata_db::jobs::JobId;

/// Guards destructive operations on revisions by checking writer job state.
///
/// Implementations check whether a revision's writer job is in a terminal state
/// (completed, stopped, failed), which determines whether the revision can be
/// safely modified (deleted, truncated, pruned).
#[async_trait]
pub trait RevisionGuard: Send + Sync {
    /// Check whether a writer job permits destructive operations on its revision.
    ///
    /// Returns `false` if the job is in a terminal state or not found (safe to proceed).
    /// Returns `true` if the job is still active and blocks the operation.
    async fn check_writer_job(&self, job_id: JobId) -> Result<bool, RevisionGuardError>;
}

/// Error when checking writer job status
///
/// This occurs when the underlying job lookup fails, typically due to
/// database connection issues or query failures.
#[derive(Debug, thiserror::Error)]
#[error("failed to check writer job status")]
pub struct RevisionGuardError(#[source] pub metadata_db::Error);
