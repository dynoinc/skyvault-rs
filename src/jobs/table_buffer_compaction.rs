use futures::future;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};

use super::JobError;
use crate::forest::ForestImpl;
use crate::metadata::{self, MetadataStore, RunMetadata};
use crate::runs::{RunError, RunId, WriteOperation};
use crate::storage::ObjectStore;
use crate::{k_way, runs};

// Define a type alias for the boxed stream
type RunStream = BoxStream<'static, Result<WriteOperation, RunError>>;

pub async fn execute(
    metadata_store: MetadataStore,
    object_store: ObjectStore,
    table_id: metadata::TableID,
) -> Result<(Vec<RunId>, Vec<RunMetadata>), JobError> {
    let forest = ForestImpl::latest(metadata_store.clone(), object_store.clone()).await?;
    let state = forest.get_state();

    let table = match state.tables.get(&table_id) {
        Some(table) if table.buffer.is_empty() => return Ok((vec![], vec![])),
        Some(table) => table,
        None => return Ok((vec![], vec![])),
    };

    let count = table.buffer.len();
    let run_data = stream::iter(table.buffer.clone())
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

    let mut run_streams = run_data
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

            // Box the stream to unify the type
            let boxed_stream: RunStream = Box::pin(runs::read_run_stream(bytes_stream));
            (seq_no, boxed_stream)
        })
        .collect::<Vec<_>>();

    if let Some(level0) = table.tree.get(&metadata::Level::zero()) {
        let object_store_clone = object_store.clone();
        let level0_run_ids: Vec<_> = level0.values().map(|m| m.id.clone()).collect();

        let level0_streams = stream::iter(level0_run_ids)
            .then(move |run_id| {
                let store = object_store_clone.clone(); // Use the clone here
                async move {
                    let bytes_stream = match store.get_run(run_id).await {
                        Ok(bs) => bs,
                        Err(e) => {
                            let err: RunError = RunError::Io(std::io::Error::other(e.to_string()));
                            return Box::pin(stream::once(future::ready(Err(err)))) as RunStream;
                        },
                    };

                    let adapted_stream = stream::unfold(bytes_stream, |mut bs| {
                        Box::pin(async move {
                            match bs.next().await {
                                Some(Ok(bytes)) => Some((Ok(bytes), bs)),
                                Some(Err(e)) => {
                                    Some((Err(std::io::Error::other(e.to_string())), bs))
                                },
                                None => None,
                            }
                        })
                    });

                    Box::pin(runs::read_run_stream(adapted_stream))
                }
            })
            .flatten();

        // Add this at seq_no 0, so it has the lowest priority
        run_streams.push((metadata::SeqNo::from(0), Box::pin(level0_streams)));
    }

    let merged_stream = k_way::merge(run_streams);
    let run_stream = runs::build_runs(merged_stream);

    let mut new_runs = Vec::new();

    tokio::pin!(run_stream);
    while let Some(result) = run_stream.try_next().await? {
        let (run_data, stats) = result;
        let run_id = RunId(ulid::Ulid::new().to_string());

        // Store the run immediately
        object_store.put_run(run_id.clone(), run_data).await?;

        // Collect the run ID and stats
        new_runs.push(metadata::RunMetadata {
            id: run_id,
            belongs_to: metadata::BelongsTo::TableTree(table_id, metadata::Level::zero()),
            stats,
        });
    }

    if new_runs.is_empty() {
        return Err(JobError::Internal(
            "No runs were generated during compaction".into(),
        ));
    }

    let compacted = table
        .buffer
        .values()
        .map(|metadata| metadata.id.clone())
        .chain(
            table
                .tree
                .get(&metadata::Level::zero())
                .into_iter()
                .flat_map(|level0| level0.values().map(|metadata| metadata.id.clone())),
        )
        .collect();

    Ok((compacted, new_runs))
}
