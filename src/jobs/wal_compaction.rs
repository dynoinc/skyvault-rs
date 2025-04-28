use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::pin::Pin;

use futures::Stream;
use futures::stream::{self, StreamExt, TryStreamExt};
use tokio::sync::mpsc;

use crate::metadata::{BelongsTo, ChangelogEntry, MetadataStore};
use crate::runs::{RunError, WriteOperation};
use crate::storage::ObjectStore;

use super::JobError;

pub async fn execute_wal_compaction(
    metadata_store: MetadataStore,
    object_store: ObjectStore,
) -> Result<(), JobError> {
    let (changelog_snapshot, _) = metadata_store.get_changelog_snapshot().await?;

    let mut wal_runs = HashSet::new();
    for entry in changelog_snapshot {
        match entry {
            ChangelogEntry::V1(v1) => {
                for run_id in v1.runs_added {
                    wal_runs.insert(run_id);
                }
                for run_id in v1.runs_removed {
                    wal_runs.remove(&run_id);
                }
            },
        }
    }

    let run_metadatas = metadata_store.get_run_metadata_batch(wal_runs).await?;
    let run_ids_and_seqno = run_metadatas
        .values()
        .map(|metadata| {
            (metadata.id.clone(), match metadata.belongs_to {
                BelongsTo::WalSeqNo(seqno) => seqno,
                BelongsTo::TableName(_) => {
                    panic!("Run ID {} belongs to table, not WAL", metadata.id)
                },
            })
        })
        .collect::<Vec<_>>();

    let count = run_ids_and_seqno.len();
    let run_data = stream::iter(run_ids_and_seqno.clone())
        .map(|(run_id, _)| {
            let store = object_store.clone();
            async move { store.get_run(&run_id).await.map_err(JobError::Storage) }
        })
        .buffered(count)
        .try_collect::<Vec<_>>()
        .await?;

    let run_streams = run_data
        .into_iter()
        .enumerate()
        .map(|(i, byte_stream)| {
            let (_, seqno) = run_ids_and_seqno[i];
            let bytes_stream = stream::unfold(byte_stream, |mut bs| {
                Box::pin(async move {
                    match bs.try_next().await {
                        Ok(Some(bytes)) => Some((Ok(bytes), bs)),
                        Ok(None) => None,
                        Err(e) => Some((Err(std::io::Error::other(e.to_string())), bs)),
                    }
                })
            });

            (seqno, crate::runs::read_run_stream(bytes_stream))
        })
        .collect::<Vec<_>>();

    #[derive(Eq, PartialEq)]
    struct HeapItem {
        op: WriteOperation,
        seq_no: i64,
    }

    impl Ord for HeapItem {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            match self.op.key().cmp(other.op.key()) {
                std::cmp::Ordering::Equal => other.seq_no.cmp(&self.seq_no), // Higher seq_no first
                ord => ord.reverse(),                                        // Min-heap by key
            }
        }
    }

    impl PartialOrd for HeapItem {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    let merged_stream = {
        let (tx, rx) = mpsc::channel(100);

        tokio::spawn(async move {
            let mut heap = BinaryHeap::new();
            let mut streams_map = HashMap::new();

            // Use Pin<Box<dyn Stream>> for streams
            type PinnedStream =
                Pin<Box<dyn Stream<Item = Result<WriteOperation, RunError>> + Send>>;

            for (seq_no, stream) in run_streams {
                // Pin the stream before use
                let mut pinned_stream: PinnedStream = Box::pin(stream);
                match pinned_stream.as_mut().next().await {
                    Some(Ok(op)) => {
                        streams_map.insert(seq_no, pinned_stream);
                        heap.push(HeapItem { op, seq_no });
                    },
                    Some(Err(e)) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    },
                    None => {},
                }
            }

            // Process until all streams exhausted
            let mut last_key: Option<String> = None;
            while !heap.is_empty() || !streams_map.is_empty() {
                let item = heap.pop().unwrap();
                if !last_key
                    .as_ref()
                    .map(|k| item.op.key() == k)
                    .unwrap_or_default()
                {
                    last_key = Some(item.op.key().to_string());
                    if tx.send(Ok(item.op)).await.is_err() {
                        return;
                    }
                }

                // Get next item from the stream with this seq_no
                if let Entry::Occupied(mut entry) = streams_map.entry(item.seq_no) {
                    let stream = entry.get_mut();
                    match stream.next().await {
                        Some(Ok(next_op)) => {
                            heap.push(HeapItem {
                                op: next_op,
                                seq_no: item.seq_no,
                            });
                        },
                        Some(Err(e)) => {
                            let _ = tx.send(Err(e)).await;
                            return;
                        },
                        None => {
                            let _ = entry.remove_entry();
                        },
                    }
                }
            }
        });

        // Return a stream that consumes from the receiver
        stream::unfold(rx, |mut rx| {
            Box::pin(async move { rx.recv().await.map(|item| (item, rx)) })
        })
    };

    let (run_data, stats) = crate::runs::build_run(merged_stream).await?;
    let run_id = ulid::Ulid::new().to_string();
    object_store.put_run(&run_id, run_data).await?;

    let compacted = run_metadatas.keys().cloned().collect::<Vec<_>>();
    let smallest_seq_no = run_metadatas
        .values()
        .map(|metadata| match metadata.belongs_to {
            BelongsTo::WalSeqNo(seqno) => seqno,
            BelongsTo::TableName(_) => {
                panic!("Run ID {} belongs to table, not WAL", metadata.id)
            },
        })
        .min()
        .unwrap();

    metadata_store
        .compact_wal(compacted, smallest_seq_no, run_id, stats)
        .await?;
    Ok(())
} 