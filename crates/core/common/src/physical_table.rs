//! Physical table — segment resolution and table identity.
//!
//! This module owns the concept of segments and physical table resolution.
//!
//! ## Module layout
//!
//! - `file` — `FileMetadata`: file identity and metadata from DB rows
//! - `segments` — `Segment`, `Chain`, `canonical_chain()`, `missing_ranges()`
//! - `table` — `PhysicalTable`: identity + segment resolution
//! - `snapshot` — `TableSnapshot`: resolved segments view

pub mod file;
pub mod segments;
pub mod snapshot;
pub mod table;

pub use file::FileMetadata;
pub use snapshot::MultiNetworkSegmentsError;
pub use table::{
    CanonicalChainError, GetFilesError, GetSegmentsError, MissingRangesError, PhysicalTable,
    SnapshotError,
};
