use futures::stream::{self, StreamExt, TryStreamExt};
use tokio::sync::mpsc;

use super::JobError;
use crate::forest::ForestImpl;
use crate::metadata::{MetadataStore, TableID};
use crate::runs::{RunError, RunId, Stats, WriteOperation};
use crate::storage::ObjectStore;
use crate::{k_way, runs};

pub async fn execute(
    metadata_store: MetadataStore,
    object_store: ObjectStore,
) -> Result<(Vec<RunId>, Vec<(RunId, TableID, Stats)>), JobError> {
    let forest = ForestImpl::latest(metadata_store.clone(), object_store.clone()).await?;
    let state = forest.get_state();
    if state.wal.is_empty() {
        return Ok((vec![], vec![]));
    }

    let count = state.wal.len();
    let run_data = stream::iter(state.wal.clone())
        .map(|(seq_no, metadata)| {
            let store = object_store.clone();
            async move {
                store
                    .get_run(metadata.id)
                    .await
                    .map(|run| (seq_no, run))
                    .map_err(JobError::Storage)
            }
        })
        .buffered(count)
        .try_collect::<Vec<_>>()
        .await?;

    let run_streams = run_data
        .into_iter()
        .map(|(seq_no, byte_stream)| {
            let bytes_stream = stream::unfold(byte_stream, |mut bs| {
                Box::pin(async move {
                    match bs.try_next().await {
                        Ok(Some(bytes)) => Some((Ok(bytes), bs)),
                        Ok(None) => None,
                        Err(e) => Some((Err(std::io::Error::other(e.to_string())), bs)),
                    }
                })
            });

            (seq_no, runs::read_run_stream(bytes_stream))
        })
        .collect::<Vec<_>>();

    let merged_stream = k_way::merge(run_streams);

    // Vector to collect (run_id, table_name, stats) for each table
    let mut table_runs = Vec::new();

    // Current table state: (table_name, sender, task)
    type TableState = (
        TableID,
        mpsc::Sender<Result<WriteOperation, RunError>>,
        tokio::task::JoinHandle<Result<(RunId, Stats), JobError>>,
    );
    let mut current_state: Option<TableState> = None;

    // Process stream one operation at a time
    let mut stream = Box::pin(merged_stream);
    while let Some(result) = stream.next().await {
        let op = result?;

        let (table_prefix_len, new_table) = {
            // Parse key in format "table_id.key"
            let (table_id_str, _) = op.key().split_once('.').ok_or_else(|| {
                JobError::InvalidInput(format!(
                    "Key does not follow 'table_id.key' format: {}",
                    op.key()
                ))
            })?;

            let table_id = TableID::from(table_id_str.parse::<i64>().map_err(|e| {
                JobError::InvalidInput(format!("Invalid table ID '{table_id_str}': {e}"))
            })?);

            let table_prefix_len = format!("{table_id}.").len();
            let new_table = match current_state
                .as_ref()
                .map(|(current_table_id, _, _)| current_table_id)
                != Some(&table_id)
            {
                true => Some(table_id),
                false => None,
            };

            (table_prefix_len, new_table)
        };

        let op = match op {
            WriteOperation::Put(mut key, value) => {
                WriteOperation::Put(key.split_off(table_prefix_len), value)
            },
            WriteOperation::Delete(mut key) => {
                WriteOperation::Delete(key.split_off(table_prefix_len))
            },
        };

        if let Some(new_table) = new_table {
            // Close previous state if any
            if let Some((old_table, sender, task)) = current_state.take() {
                // Close channel
                drop(sender);

                // Wait for task to complete
                if let Ok((run_id, stats)) = task
                    .await
                    .map_err(|e| JobError::Internal(format!("Table task failed: {e}")))?
                {
                    // Add the completed run to our results
                    table_runs.push((run_id, old_table, stats));
                }
            }

            // Create channel for the new table
            let (tx, rx) = mpsc::channel(100);

            // Create stream from receiver
            let rx_stream = stream::unfold(rx, |mut rx| {
                Box::pin(async move {
                    rx.recv()
                        .await
                        .map(|item: Result<WriteOperation, RunError>| (item, rx))
                })
            })
            .map(|item_result| {
                item_result.map_err(|e| RunError::Format(format!("Channel receive error: {e}")))
            }); // Handle channel error -> RunError::Format

            // Start task to build run for this table
            let object_store_clone = object_store.clone();
            let task = tokio::spawn(async move {
                // Build run from stream
                // build_runs returns a stream. For now, collect it and expect exactly one run.
                let results: Vec<_> = runs::build_runs(rx_stream)
                    .try_collect()
                    .await
                    .map_err(JobError::Run)?;

                if results.len() != 1 {
                    // This might indicate an issue with compaction logic or an unexpected large run
                    return Err(JobError::Internal(format!(
                        "WAL compaction expected exactly one output run for table, got {}",
                        results.len()
                    )));
                }
                // Panic if the stream was empty, which shouldn't happen after the check above.
                let (run_data, stats) = results.into_iter().next().unwrap();

                let run_id = RunId(ulid::Ulid::new().to_string());

                // Persist run
                object_store_clone
                    .put_run(run_id.clone(), run_data)
                    .await
                    .map_err(JobError::Storage)?;

                Ok((run_id, stats))
            });

            // Save new state
            current_state = Some((new_table, tx, task));
        }

        // Send operation to current table's channel
        if let Some((_, sender, _)) = &current_state {
            if sender.send(Ok(op)).await.is_err() {
                return Err(JobError::Internal(
                    "Failed to send operation to table channel".into(),
                ));
            }
        }
    }

    // Close final state if any
    if let Some((old_table, sender, task)) = current_state.take() {
        // Close channel
        drop(sender);

        // Wait for task to complete
        if let Ok((run_id, stats)) = task
            .await
            .map_err(|e| JobError::Internal(format!("Table task failed: {e}")))?
        {
            // Add the completed run to our results
            table_runs.push((run_id, old_table, stats));
        }
    }

    let compacted = state
        .wal
        .values()
        .map(|metadata| metadata.id.clone())
        .collect::<Vec<_>>();

    Ok((compacted, table_runs))
}
