use amp_data_store::{DataStore, PhyTableRevision};
use amp_parquet::footer::{AmpMetadataFromParquetError, amp_metadata_from_parquet_file};
use futures::{StreamExt as _, stream};

/// Registers all files in a revision with their Amp-specific metadata.
///
/// Lists all files in the revision directory in object storage, extracts
/// Parquet metadata including Amp-specific block range information, and
/// registers each file in the metadata database.
///
/// Files are processed concurrently (up to 16 at a time).
#[tracing::instrument(skip_all, err)]
pub async fn register_revision_files(
    store: &DataStore,
    revision: &PhyTableRevision,
) -> Result<i32, RegisterRevisionFilesError> {
    let files = store
        .list_revision_files_in_object_store(revision)
        .await
        .map_err(RegisterRevisionFilesError::ListFiles)?;
    let total_files = files.len();

    // Process files in parallel using buffered stream
    const CONCURRENT_METADATA_FETCHES: usize = 16;

    let object_store = store.clone();
    let mut file_stream = stream::iter(files.into_iter())
        .map(|object_meta| {
            let store = object_store.clone();
            async move {
                let (file_name, amp_meta, footer) =
                    amp_metadata_from_parquet_file(&store, &object_meta)
                        .await
                        .map_err(RegisterRevisionFilesError::ReadParquetMetadata)?;

                let parquet_meta_json = serde_json::to_value(amp_meta)
                    .map_err(RegisterRevisionFilesError::SerializeMetadata)?;

                let object_size = object_meta.size;
                let object_e_tag = object_meta.e_tag;
                let object_version = object_meta.version;

                Ok((
                    file_name,
                    object_size,
                    object_e_tag,
                    object_version,
                    parquet_meta_json,
                    footer,
                ))
            }
        })
        .buffered(CONCURRENT_METADATA_FETCHES);

    // Register all files in the metadata database as they complete
    while let Some(result) = file_stream.next().await {
        let (file_name, object_size, object_e_tag, object_version, parquet_meta_json, footer) =
            result?;
        store
            .register_revision_file(
                revision,
                &file_name,
                object_size,
                object_e_tag,
                object_version,
                parquet_meta_json,
                &footer,
            )
            .await
            .map_err(RegisterRevisionFilesError::RegisterFile)?;
    }

    Ok(total_files as i32)
}

/// Errors that occur when registering revision files
///
/// This error type is used by [`register_revision_files`].
#[derive(Debug, thiserror::Error)]
pub enum RegisterRevisionFilesError {
    /// Failed to list files in the revision directory
    ///
    /// This occurs when:
    /// - The object storage path for the revision is inaccessible
    /// - Network or permission errors when listing objects
    /// - The revision directory does not exist in object storage
    #[error("Failed to list files in revision")]
    ListFiles(#[source] amp_data_store::ListRevisionFilesInObjectStoreError),

    /// Failed to read Amp metadata from parquet file
    ///
    /// This occurs when extracting Amp-specific metadata from a Parquet file fails.
    /// Common causes include:
    /// - Corrupted or invalid Parquet file structure
    /// - Missing required metadata keys in the file
    /// - Incompatible metadata schema version
    /// - I/O errors reading from object store
    /// - JSON parsing failures in metadata values
    ///
    /// See `AmpMetadataFromParquetError` for specific error details.
    #[error("Failed to read Amp metadata from parquet file")]
    ReadParquetMetadata(#[source] AmpMetadataFromParquetError),

    /// Failed to serialize parquet metadata to JSON
    ///
    /// This occurs when:
    /// - The extracted Amp metadata cannot be represented as valid JSON
    /// - Serialization encounters unsupported types or values
    #[error("Failed to serialize parquet metadata to JSON")]
    SerializeMetadata(#[source] serde_json::Error),

    /// Failed to register file in metadata database
    ///
    /// This occurs when:
    /// - Database connection or transaction errors during file registration
    /// - Constraint violations when inserting file metadata
    /// - Concurrent modification conflicts
    #[error("Failed to register file in metadata database")]
    RegisterFile(#[source] amp_data_store::RegisterFileError),
}
