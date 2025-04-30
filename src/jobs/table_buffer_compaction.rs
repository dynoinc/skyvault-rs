use futures::future;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};

use super::JobError;
use crate::forest::State;
use crate::jobs::k_way;
use crate::metadata::MetadataStore;
use crate::runs::{RunError, WriteOperation};
use crate::storage::ObjectStore;

// Define a type alias for the boxed stream
type RunStream = BoxStream<'static, Result<WriteOperation, RunError>>;

pub async fn execute(
    metadata_store: MetadataStore,
    object_store: ObjectStore,
    job_id: i64,
    table_name: String,
) -> Result<(), JobError> {
    let (snapshot, _) = metadata_store.get_changelog_snapshot().await?;
    let state = State::from_snapshot(metadata_store.clone(), snapshot).await?;

    let table = match state.tables.get(&table_name) {
        Some(table) if table.buffer.is_empty() => return Ok(()),
        Some(table) => table,
        None => return Ok(()),
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
            let boxed_stream: RunStream = Box::pin(crate::runs::read_run_stream(bytes_stream));
            (seq_no, boxed_stream)
        })
        .collect::<Vec<_>>();

    if let Some(level0) = table.tree.get(&0) {
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

                    Box::pin(crate::runs::read_run_stream(adapted_stream))
                }
            })
            .flatten();

        run_streams.push((0, Box::pin(level0_streams)));
    }

    let merged_stream = k_way::merge(run_streams);
    let (run_data, stats) = crate::runs::build_run(merged_stream).await?;

    let run_id = crate::runs::RunId(ulid::Ulid::new().to_string());
    object_store.put_run(run_id.clone(), run_data).await?;

    let compacted = table
        .buffer
        .values()
        .map(|metadata| metadata.id.clone())
        .chain(
            table
                .tree
                .get(&0)
                .into_iter()
                .flat_map(|level0| level0.iter().map(|(_, metadata)| metadata.id.clone())),
        )
        .collect();

    metadata_store
        .append_table_compaction(job_id, compacted, crate::metadata::RunMetadata {
            id: run_id,
            belongs_to: crate::metadata::BelongsTo::TableTree(table_name, 0),
            stats,
        })
        .await?;

    Ok(())
}
