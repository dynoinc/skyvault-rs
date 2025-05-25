use futures::{
    future,
    stream::{
        self,
        BoxStream,
        StreamExt,
        TryStreamExt,
    },
};
use rand::seq::IteratorRandom;

use super::JobError;
use crate::{
    forest::ForestImpl,
    k_way,
    metadata::{
        self,
        MetadataStore,
        RunMetadata,
    },
    runs,
    runs::{
        RunError,
        RunId,
        Stats,
        StatsV1,
        WriteOperation,
    },
    storage::ObjectStore,
};

// Define a type alias for the boxed stream
type RunStream = BoxStream<'static, Result<WriteOperation, RunError>>;

pub async fn execute(
    metadata_store: MetadataStore,
    object_store: ObjectStore,
    table_id: metadata::TableID,
    level: metadata::Level,
) -> Result<(Vec<RunId>, Vec<RunMetadata>), JobError> {
    if level == metadata::Level::max() {
        // Cannot compact the max level
        return Ok((vec![], vec![]));
    }

    let state = ForestImpl::latest(metadata_store.clone(), object_store.clone()).await?;

    // Pick a random run at level and all the runs that overlap with it in the next level.
    let table = match state.trees.get(&table_id) {
        Some(table) => table,
        None => {
            // Table might have been dropped
            return Ok((vec![], vec![]));
        },
    };

    let level_run_metadata = match table.tree.get(&level).and_then(|runs| {
        let mut rng = rand::rng();
        runs.iter()
            .choose(&mut rng)
            .map(|(_, v)| v.clone())
    }) {
        Some(run_metadata) => run_metadata,
        None => {
            // No runs at this level to compact
            return Ok((vec![], vec![]));
        },
    };

    let key_range = match level_run_metadata.stats {
        Stats::StatsV1(StatsV1 { min_key, max_key, .. }) => min_key..=max_key,
    };

    let next_level_overlapping_runs: Vec<metadata::RunMetadata> = table
        .tree
        .get(&level.next())
        .map(|m| {
            m.values()
                .filter(|run| {
                    let (min_key, max_key) = match &run.stats {
                        Stats::StatsV1(StatsV1 {
                            min_key, max_key, ..
                        }) => (min_key, max_key),
                    };
                    // Check for overlap
                    key_range.contains(min_key) || key_range.contains(max_key)
                    // Also check if run's range contains the key_range entirely
                        || (min_key <= key_range.start() && max_key >= key_range.end())
                })
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let next_level_run_ids: Vec<_> = next_level_overlapping_runs.iter().map(|m| m.id.clone()).collect();

    let level_run_id = level_run_metadata.id.clone();
    let object_store_clone = object_store.clone();
    let level_run_stream_fut = async move {
        let store = object_store_clone;
        let bytes_stream = match store.get_run(level_run_id.clone()).await {
            Ok(bs) => bs,
            Err(e) => {
                let err: RunError = RunError::Io(std::io::Error::other(e.to_string()));
                // If we can't get the run, return a stream yielding the error
                return Box::pin(stream::once(future::ready(Err(err)))) as RunStream;
            },
        };
        let adapted_stream = stream::unfold(bytes_stream, |mut bs| {
            Box::pin(async move {
                match bs.next().await {
                    Some(Ok(bytes)) => Some((Ok(bytes), bs)),
                    Some(Err(e)) => Some((Err(std::io::Error::other(e.to_string())), bs)),
                    None => None,
                }
            })
        });
        Box::pin(runs::read_run_stream(adapted_stream))
    };
    let level_run_stream = level_run_stream_fut.await;

    let object_store_clone = object_store.clone();
    let next_level_runs_stream = stream::iter(next_level_run_ids.clone())
        .then(move |run_id| {
            let store = object_store_clone.clone();
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
                            Some(Err(e)) => Some((Err(std::io::Error::other(e.to_string())), bs)),
                            None => None,
                        }
                    })
                });
                Box::pin(runs::read_run_stream(adapted_stream))
            }
        })
        .flatten();

    // Give level_run higher priority (1) than next_level_runs (0)
    // k_way::merge sorts by seq_no first, then by key, then by operation type.
    let streams_to_merge = vec![
        (metadata::SeqNo::from(1), level_run_stream),
        (metadata::SeqNo::from(0), Box::pin(next_level_runs_stream) as RunStream),
    ];

    let next_level = level.next();
    let mut merged_stream: RunStream = Box::pin(k_way::merge(streams_to_merge));
    if next_level == metadata::Level::max() {
        // At the max level, we filter out all Delete operations
        // since they're no longer needed (nothing can be below this level)
        merged_stream = Box::pin(
            merged_stream.filter(|op_result| future::ready(!matches!(op_result, Ok(WriteOperation::Delete(_))))),
        );
    }

    let run_builder_stream = runs::build_runs(merged_stream);

    let mut new_runs = Vec::new();

    tokio::pin!(run_builder_stream);
    while let Some(result) = run_builder_stream.try_next().await? {
        let (run_data, stats) = result;
        let new_run_id = RunId(ulid::Ulid::new().to_string());

        // Store the run immediately
        object_store.put_run(new_run_id.clone(), run_data).await?;

        // Collect the run metadata
        new_runs.push(metadata::RunMetadata {
            id: new_run_id,
            belongs_to: metadata::BelongsTo::TableTreeLevel(
                table_id, next_level, // New runs belong to the next level
            ),
            stats,
        });
    }

    let mut compacted_run_ids = vec![level_run_metadata.id];
    compacted_run_ids.extend(next_level_overlapping_runs.into_iter().map(|m| m.id));

    Ok((compacted_run_ids, new_runs))
}
