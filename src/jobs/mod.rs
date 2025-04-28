use crate::metadata::{JobParams, MetadataError, MetadataStore};
use crate::runs::RunError;
use crate::storage::{ObjectStore, StorageError};

mod wal_compaction;

pub use wal_compaction::execute_wal_compaction;

#[derive(Debug, thiserror::Error)]
pub enum JobError {
    #[error("Metadata error: {0}")]
    Metadata(#[from] MetadataError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Run error: {0}")]
    Run(#[from] RunError),
}

pub async fn execute(
    metadata_store: MetadataStore,
    object_store: ObjectStore,
    job_id: i64,
) -> Result<(), JobError> {
    let job_params = metadata_store.get_job(job_id).await?;

    match job_params {
        JobParams::WALCompaction => execute_wal_compaction(metadata_store, object_store).await,
    }
}
