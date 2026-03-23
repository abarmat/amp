//! Idempotency key computation for garbage collection jobs.
//!
//! Computes idempotency keys by combining the GC job kind discriminator
//! with a location ID, producing a deterministic hash that prevents duplicate
//! job scheduling for the same physical table revision.

use datasets_common::hash::Hash;
use metadata_db::{jobs::IdempotencyKey, physical_table_revision::LocationId};

use crate::job_kind::JOB_KIND;

/// Compute an idempotency key for a GC job.
///
/// The key is derived by hashing `{job_kind}:{location_id}`,
/// producing a deterministic 64-character hex string.
pub fn idempotency_key(location_id: LocationId) -> IdempotencyKey<'static> {
    let input = format!("{JOB_KIND}:{location_id}");
    let hash: Hash = datasets_common::hash::hash(input);
    // SAFETY: The hash is a validated 64-char hex string produced by our hash function.
    IdempotencyKey::from_owned_unchecked(hash.into_inner())
}
