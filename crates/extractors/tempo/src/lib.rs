//! Tempo dataset extractor.

use datasets_common::hash_reference::HashReference;
use datasets_raw::dataset::Dataset as RawDataset;

pub mod tables;

pub use datasets_raw::{dataset_kind::TempoDatasetKind, manifest::TempoManifest as Manifest};

/// Convert a Tempo manifest into a logical dataset representation.
///
/// Dataset identity (namespace, name, version, hash reference) must be provided externally as they
/// are not part of the manifest.
pub fn dataset(reference: HashReference, manifest: Manifest) -> RawDataset {
    let network = manifest.network;
    RawDataset::new(
        reference,
        manifest.kind.into(),
        network.clone(),
        tables::all(&network),
        Some(manifest.start_block),
        manifest.finalized_blocks_only,
    )
}
