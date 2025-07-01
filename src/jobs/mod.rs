use crate::{
    forest::{
        ForestError,
        ForestImpl,
    },
    metadata::{
        self,
        JobParams,
        JobStatus,
        MetadataError,
        MetadataStore,
    },
    runs::{
        RunError,
        Stats,
    },
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
        return Err(JobError::InvalidInput(format!(
            "Job {job_id} is not pending: {:?}",
            job.status
        )));
    }

    let tx_ctx = sentry::TransactionContext::new(&format!("job-execution-{job_id}"), "job.execute");
    let transaction = sentry::start_transaction(tx_ctx);

    // Set transaction as current span and add metadata
    sentry::configure_scope(|scope| scope.set_span(Some(transaction.clone().into())));
    transaction.set_data("job_id", sentry::protocol::Value::String(job_id.to_string()));
    transaction.set_data(
        "job_type",
        sentry::protocol::Value::String(job.params.job_type().to_string()),
    );

    if let Ok(params_json) = serde_json::to_value(&job.params) {
        transaction.set_data("job_params", params_json);
    }

    let result: Result<(), JobError> = async {
        match job.params {
            JobParams::WALCompaction => {
                let state = ForestImpl::latest(metadata_store.clone(), object_store.clone()).await?;
                let (compacted, table_runs) = wal_compaction::execute(object_store, &state).await?;
                metadata_store
                    .append_wal_compaction(Some(job_id), compacted, table_runs)
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
                    table_tree_compaction::execute(metadata_store.clone(), object_store.clone(), table_id, level)
                        .await?;
                metadata_store
                    .append_table_compaction(job_id, compacted, new_runs)
                    .await?;
                Ok(())
            },
        }
    }
    .await;

    match &result {
        Ok(_) => {
            transaction.set_status(sentry::protocol::SpanStatus::Ok);
            tracing::info!("Job completed successfully");
        },
        Err(e) => {
            transaction.set_status(sentry::protocol::SpanStatus::InternalError);
            transaction.set_data("error", sentry::protocol::Value::String(e.to_string()));
            tracing::error!(error = %e, "Job failed");
        },
    }

    transaction.finish();
    result
}

pub async fn run_wal_compactor(metadata_store: MetadataStore, storage: ObjectStore) -> anyhow::Result<()> {
    let forest = ForestImpl::watch(metadata_store.clone(), storage.clone(), |stream| stream).await?;

    let mut state_rx = forest.watch_state();

    let mut last_seq_no = None;
    tracing::info!("Starting WAL compactor daemon");
    loop {
        if state_rx.changed().await.is_err() {
            break;
        }

        let state = state_rx.borrow().clone();
        if last_seq_no
            .map(|last_seq_no| last_seq_no >= state.seq_no)
            .unwrap_or(false)
        {
            continue;
        }

        let total_wal_size = state
            .wal
            .values()
            .map(|r| match &r.stats {
                Stats::StatsV1(stats) => stats.size_bytes,
            })
            .sum::<u64>();

        if total_wal_size < 100_000_000 && state.wal.len() < 25 {
            continue;
        }

        let tx_ctx = sentry::TransactionContext::new("wal-compactor", "job.execute");
        let transaction = sentry::start_transaction(tx_ctx);

        sentry::configure_scope(|scope| scope.set_span(Some(transaction.clone().into())));
        transaction.set_data(
            "job_type",
            sentry::protocol::Value::String("wal-compaction".to_string()),
        );

        let result = async {
            let (compacted, table_runs) = wal_compaction::execute(storage.clone(), &state).await?;
            let seq_no = metadata_store
                .append_wal_compaction(None, compacted, table_runs)
                .await?;
            Ok::<_, anyhow::Error>(seq_no)
        }
        .await;

        match result {
            Ok(seq_no) => {
                last_seq_no = Some(seq_no);
                transaction.set_status(sentry::protocol::SpanStatus::Ok);
                tracing::info!("WAL compaction completed successfully");
            },
            Err(e) => {
                transaction.set_status(sentry::protocol::SpanStatus::InternalError);
                transaction.set_data("error", sentry::protocol::Value::String(e.to_string()));
                tracing::error!(error = %e, "Failed to compact WALs");
            },
        }

        transaction.finish();
    }

    Ok(())
}
