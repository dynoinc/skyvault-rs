use crate::{
    forest::ForestError,
    metadata::{
        self, JobParams, JobStatus, MetadataError, MetadataStore
    },
    runs::RunError,
    storage::{
        ObjectStore,
        StorageError,
    },
};

mod table_buffer_compaction;
mod table_tree_compaction;
mod wal_compaction;

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
    job_id: metadata::JobID,
) -> Result<(), JobError> {
    let job = metadata_store.get_job(job_id).await?;

    if job.status != JobStatus::Pending {
        return Err(JobError::InvalidInput(format!("Job {job_id} is not pending: {:?}", job.status)));
    }

    match job.params {
        JobParams::WALCompaction => {
            let (compacted, table_runs) = wal_compaction::execute(metadata_store.clone(), object_store.clone()).await?;
            metadata_store
                .append_wal_compaction(job_id, compacted, table_runs)
                .await?;
            Ok(())
        },
        JobParams::TableBufferCompaction(table_id) => {
            let (compacted, new_runs) =
                table_buffer_compaction::execute(metadata_store.clone(), object_store.clone(), table_id).await?;
            metadata_store
                .append_table_compaction(job_id, compacted, new_runs)
                .await?;
            Ok(())
        },
        JobParams::TableTreeCompaction(table_id, level) => {
            let (compacted, new_runs) =
                table_tree_compaction::execute(metadata_store.clone(), object_store.clone(), table_id, level).await?;
            metadata_store
                .append_table_compaction(job_id, compacted, new_runs)
                .await?;
            Ok(())
        },
    }
}
