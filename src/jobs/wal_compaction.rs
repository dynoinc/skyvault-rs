use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::pin::Pin;

use futures::Stream;
use futures::stream::{self, StreamExt, TryStreamExt};
use tokio::sync::mpsc;

use super::JobError;
use crate::metadata::{BelongsTo, ChangelogEntry, MetadataStore};
use crate::runs::{RunError, WriteOperation};
use crate::storage::ObjectStore;

#[derive(Eq, PartialEq)]
struct HeapItem {
    op: WriteOperation,
    seq_no: i64,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.op.key().cmp(other.op.key()) {
            std::cmp::Ordering::Equal => self.seq_no.cmp(&other.seq_no),
            ord => ord.reverse(),
        }
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod heap_item_tests {
    use super::*;

    #[test]
    fn test_heap_item_ordering_different_keys() {
        // Create items with different keys
        let item1 = HeapItem {
            op: WriteOperation::Put("apple".to_string(), vec![1]),
            seq_no: 1,
        };
        let item2 = HeapItem {
            op: WriteOperation::Put("banana".to_string(), vec![2]),
            seq_no: 2,
        };

        assert!(item2 < item1);
    }

    #[test]
    fn test_heap_item_ordering_same_key() {
        // Create items with the same key but different sequence numbers
        let item1 = HeapItem {
            op: WriteOperation::Put("apple".to_string(), vec![1]),
            seq_no: 1,
        };
        let item2 = HeapItem {
            op: WriteOperation::Put("apple".to_string(), vec![2]),
            seq_no: 2,
        };

        assert!(item1 < item2);
    }

    #[test]
    fn test_heap_item_ordering_binary_heap() {
        // Test that BinaryHeap pops items in the expected order
        let mut heap = BinaryHeap::new();

        heap.push(HeapItem {
            op: WriteOperation::Put("c".to_string(), vec![1]),
            seq_no: 1,
        });
        heap.push(HeapItem {
            op: WriteOperation::Put("a".to_string(), vec![1]),
            seq_no: 1,
        });
        heap.push(HeapItem {
            op: WriteOperation::Put("b".to_string(), vec![1]),
            seq_no: 1,
        });
        heap.push(HeapItem {
            op: WriteOperation::Put("a".to_string(), vec![2]),
            seq_no: 2,
        });

        // Should pop in order: a(seq_no=2), a(seq_no=1), b, c
        let item = heap.pop().unwrap();
        assert_eq!(item.op.key(), "a");
        assert_eq!(item.seq_no, 2);

        let item = heap.pop().unwrap();
        assert_eq!(item.op.key(), "a");
        assert_eq!(item.seq_no, 1);

        let item = heap.pop().unwrap();
        assert_eq!(item.op.key(), "b");

        let item = heap.pop().unwrap();
        assert_eq!(item.op.key(), "c");
    }
}

/// Merges multiple run streams into a single stream, preserving correct ordering
/// by key and taking the latest value (highest sequence number) for each key.
pub fn merge_run_streams<S>(
    run_streams: Vec<(i64, S)>,
) -> impl Stream<Item = Result<WriteOperation, RunError>>
where
    S: Stream<Item = Result<WriteOperation, RunError>> + Send + 'static,
{
    let (tx, rx) = mpsc::channel(100);

    tokio::spawn(async move {
        let mut heap = BinaryHeap::new();
        let mut streams_map = HashMap::new();

        // Use Pin<Box<dyn Stream>> for streams
        type PinnedStream = Pin<Box<dyn Stream<Item = Result<WriteOperation, RunError>> + Send>>;

        for (seq_no, stream) in run_streams {
            // Pin the stream before use
            let mut pinned_stream: PinnedStream = Box::pin(stream);
            match pinned_stream.next().await {
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
}

pub async fn execute_wal_compaction(
    metadata_store: MetadataStore,
    object_store: ObjectStore,
    job_id: i64,
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

    let run_ids = wal_runs.into_iter().collect::<Vec<_>>();
    let run_metadatas = metadata_store.get_run_metadata_batch(run_ids).await?;
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

    let merged_stream = merge_run_streams(run_streams);

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
        .append_compaction(
            job_id,
            compacted,
            run_id,
            BelongsTo::WalSeqNo(smallest_seq_no),
            stats,
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use futures::stream;

    use super::*;
    use crate::runs::WriteOperation;

    #[tokio::test]
    async fn test_merge_run_streams() {
        // Create mock streams with known operations
        let stream1 = stream::iter(vec![
            Ok(WriteOperation::Put("a".to_string(), vec![1])),
            Ok(WriteOperation::Put("c".to_string(), vec![3])),
        ]);

        let stream2 = stream::iter(vec![
            Ok(WriteOperation::Put("a".to_string(), vec![10])), /* Higher seq_no should win for
                                                                 * duplicate keys */
            Ok(WriteOperation::Put("b".to_string(), vec![20])),
        ]);

        // Create input for merge_run_streams with different sequence numbers
        let run_streams = vec![(1, stream1), (2, stream2)];

        // Merge the streams
        let merged = merge_run_streams(run_streams);

        // Collect the results and verify
        let results: Vec<WriteOperation> = merged.map(|r| r.unwrap()).collect::<Vec<_>>().await;

        // Expected outcome:
        // 1. "a" from stream2 (seq_no 2) as it has higher seq_no
        // 2. "b" from stream2
        // 3. "c" from stream1
        assert_eq!(results.len(), 3);

        // Convert results to a map for easier assertion
        let result_map: HashMap<String, Vec<u8>> = results
            .into_iter()
            .map(|op| match op {
                WriteOperation::Put(key, value) => (key, value),
                _ => panic!("Unexpected DeleteOperation in test"),
            })
            .collect();

        assert_eq!(result_map.get("a").unwrap(), &vec![10]);
        assert_eq!(result_map.get("b").unwrap(), &vec![20]);
        assert_eq!(result_map.get("c").unwrap(), &vec![3]);
    }
}
