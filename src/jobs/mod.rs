use crate::forest::ForestError;
use crate::metadata::{self, JobParams, MetadataError, MetadataStore};
use crate::runs::RunError;
use crate::storage::{ObjectStore, StorageError};

mod table_buffer_compaction;
mod wal_compaction;

mod k_way;

#[derive(Debug, thiserror::Error)]
pub enum JobError {
    #[error("Metadata error: {0}")]
    Metadata(#[from] MetadataError),

    #[error("Forest error: {0}")]
    Forest(#[from] ForestError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Run error: {0}")]
    Run(#[from] RunError),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub async fn execute(
    metadata_store: MetadataStore,
    object_store: ObjectStore,
    job_id: metadata::JobId,
) -> Result<(), JobError> {
    let job_params = metadata_store.get_job(job_id).await?;

    match job_params {
        JobParams::WALCompaction => {
            wal_compaction::execute(metadata_store, object_store, job_id).await
        },
        JobParams::TableBufferCompaction(table_name) => {
            table_buffer_compaction::execute(metadata_store, object_store, job_id, table_name).await
        },
    }
}
