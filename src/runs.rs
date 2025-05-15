use std::fmt::{self, Display};
use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::proto;

// Type aliases for clarity
pub type Key = String;
pub type Value = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct RunId(pub String);

impl<T: Into<String>> From<T> for RunId {
    fn from(value: T) -> Self {
        RunId(value.into())
    }
}

impl Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Represents write operations to be included in a run
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOperation {
    Put(Key, Value),
    Delete(Key),
}

impl WriteOperation {
    pub fn key(&self) -> &str {
        match self {
            WriteOperation::Put(k, _) | WriteOperation::Delete(k) => k,
        }
    }
}

impl From<proto::GetFromRunItem> for WriteOperation {
    fn from(op: proto::GetFromRunItem) -> Self {
        match op.result {
            Some(proto::get_from_run_item::Result::Value(v)) => WriteOperation::Put(op.key, v),
            Some(proto::get_from_run_item::Result::Deleted(_)) => WriteOperation::Delete(op.key),
            None => panic!("Invalid GetFromRunItem: no result"),
        }
    }
}

impl From<WriteOperation> for proto::GetFromRunItem {
    fn from(val: WriteOperation) -> Self {
        proto::GetFromRunItem {
            key: val.key().to_string(),
            result: match val {
                WriteOperation::Put(_, value) => {
                    Some(proto::get_from_run_item::Result::Value(value))
                },
                WriteOperation::Delete(_) => Some(proto::get_from_run_item::Result::Deleted(())),
            },
        }
    }
}
// Result of searching within a run
#[derive(Debug, PartialEq, Eq)]
pub enum SearchResult {
    Found(Value),
    Tombstone,
    NotFound,
}

// Errors that can occur during run operations
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Data format error: {0}")]
    Format(String),
    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::Status),
    #[error("Unsupported run version: {0}")]
    UnsupportedVersion(u8),
    #[error("Input list of operations cannot be empty")]
    EmptyInput,
}

pub const MAX_RUN_SIZE_BYTES: u64 = 10 * 1024 * 1024; // 10 MB
pub const CURRENT_VERSION: u8 = 1;

const MARKER_PUT: u8 = 1;
const MARKER_DELETE: u8 = 2;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StatsV1 {
    pub min_key: String,
    pub max_key: String,
    pub size_bytes: u64,
    pub put_count: u64,
    pub delete_count: u64,
}

impl From<StatsV1> for proto::StatsV1 {
    fn from(stats: StatsV1) -> Self {
        proto::StatsV1 {
            min_key: stats.min_key,
            max_key: stats.max_key,
            size_bytes: stats.size_bytes,
            put_count: stats.put_count,
            delete_count: stats.delete_count,
        }
    }
}

impl From<proto::StatsV1> for StatsV1 {
    fn from(stats: proto::StatsV1) -> Self {
        StatsV1 {
            min_key: stats.min_key,
            max_key: stats.max_key,
            size_bytes: stats.size_bytes,
            put_count: stats.put_count,
            delete_count: stats.delete_count,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum Stats {
    StatsV1(StatsV1),
}

impl From<Stats> for proto::run_metadata::Stats {
    fn from(stats: Stats) -> Self {
        match stats {
            Stats::StatsV1(stats) => proto::run_metadata::Stats::StatsV1(stats.into()),
        }
    }
}

impl From<proto::run_metadata::Stats> for Stats {
    fn from(stats: proto::run_metadata::Stats) -> Self {
        match stats {
            proto::run_metadata::Stats::StatsV1(stats) => Stats::StatsV1(stats.into()),
        }
    }
}

/// Builds multiple runs from a stream of sorted write operations, splitting
/// runs when they reach approximately MAX_RUN_SIZE_BYTES.
///
/// Returns a stream yielding tuples of (serialized run data, stats)
///
/// # Errors
///
/// Yields an error if:
/// - The operations are not sorted by key
/// - There's an I/O error during serialization or reading the input stream
pub fn build_runs<S>(mut operations: S) -> impl Stream<Item = Result<(Bytes, Stats), RunError>>
where
    S: Stream<Item = Result<WriteOperation, RunError>> + Unpin + Send + 'static,
{
    async_stream::stream! {
        let mut min_key: Option<String> = None;
        let mut max_key: Option<String> = None;
        let mut put_count: u64 = 0;
        let mut delete_count: u64 = 0;
        let mut current_run_data: Vec<u8> = Vec::new();
        let mut current_run_size_bytes: u64 = 0;
        let mut last_key: Option<String> = None;
        let mut first_op_in_run = true;

        while let Some(op_result) = operations.next().await {
            let op = match op_result {
                Ok(op) => op,
                Err(e) => {
                    yield Err(e); // Propagate error from the input stream
                    return;
                }
            };
            let current_key = op.key().to_string();

            // Check if keys are sorted across the entire input stream
            if let Some(ref last) = last_key {
                if current_key <= *last {
                    yield Err(RunError::Format(
                        "Operations must be sorted by key".to_string(),
                    ));
                    return;
                }
            }
            last_key = Some(current_key.clone());

            // Calculate the size this operation would add
            let op_size = match &op {
                WriteOperation::Put(key, value) => {
                    1 + 4 + key.len() as u64 + 4 + value.len() as u64
                },
                WriteOperation::Delete(key) => {
                    1 + 4 + key.len() as u64
                },
            };

            // If this is the first operation in a *new* run, account for the version byte.
            let size_with_op = if first_op_in_run {
                current_run_size_bytes + 1 + op_size
            } else {
                current_run_size_bytes + op_size
            };

            // Check if adding this operation exceeds the size limit AND we have items already
            if !first_op_in_run && size_with_op > MAX_RUN_SIZE_BYTES {
                // Finalize and yield the current run
                let stats = Stats::StatsV1(StatsV1 {
                    min_key: min_key.take().unwrap(), // Should always have a value if item_count > 0
                    max_key: max_key.take().unwrap(), // Should always have a value if item_count > 0
                    size_bytes: current_run_size_bytes,
                    put_count,
                    delete_count,
                });
                yield Ok((Bytes::from(current_run_data), stats));

                // Reset state for the next run
                current_run_data = Vec::new();
                current_run_size_bytes = 0;
                put_count = 0;
                delete_count = 0;
                // min_key and max_key already taken.
                first_op_in_run = true;
                // The current 'op' will be the first in the new run. Recalculate its size contribution below.
            }

            // Add operation to the current run
            if first_op_in_run {
                current_run_data.push(CURRENT_VERSION);
                current_run_size_bytes += 1; // Version byte
                min_key = Some(current_key.clone()); // Set min_key for the new run
                first_op_in_run = false;
            }

            max_key = Some(current_key.clone()); // Update max_key for the current run
            current_run_size_bytes += op_size; // Add actual operation size

            // Serialize the operation into the current run's buffer
            match &op {
                WriteOperation::Put(key, value) => {
                    current_run_data.push(MARKER_PUT);
                    current_run_data.extend_from_slice(&(key.len() as u32).to_be_bytes());
                    current_run_data.extend_from_slice(key.as_bytes());
                    current_run_data.extend_from_slice(&(value.len() as u32).to_be_bytes());
                    current_run_data.extend_from_slice(value);
                    put_count += 1;
                },
                WriteOperation::Delete(key) => {
                    current_run_data.push(MARKER_DELETE);
                    current_run_data.extend_from_slice(&(key.len() as u32).to_be_bytes());
                    current_run_data.extend_from_slice(key.as_bytes());
                    delete_count += 1;
                },
            }
        }

        // Yield the last run if it contains any data
        if put_count > 0 || delete_count > 0 {
            let stats = Stats::StatsV1(StatsV1 {
                min_key: min_key.unwrap(),
                max_key: max_key.unwrap(),
                size_bytes: current_run_size_bytes,
                put_count,
                delete_count,
            });
            yield Ok((Bytes::from(current_run_data), stats));
        }
    }
}

/// Searches for a key within a serialized run (v1).
pub fn search_run(run_data: &[u8], search_key: &str) -> SearchResult {
    let mut cursor = Cursor::new(run_data);

    // Check if data is empty
    if run_data.is_empty() {
        panic!("Empty run data");
    }

    // Read and check version
    let version = cursor.read_u8().expect("Failed to read version byte");
    if version != CURRENT_VERSION {
        panic!("Unsupported version: {}", version);
    }

    // Iterate through entries
    while cursor.position() < run_data.len() as u64 {
        // Check if we have enough data to read the marker
        if cursor.position() >= run_data.len() as u64 {
            panic!("Unexpected end of data");
        }

        let marker = cursor.read_u8().expect("Failed to read marker byte");
        if marker != MARKER_PUT && marker != MARKER_DELETE {
            panic!("Invalid marker byte: {}", marker);
        }

        // Check if we have enough data to read the key length
        if cursor.position() + 4 > run_data.len() as u64 {
            panic!("Incomplete key length data");
        }

        // Read key
        let key_len = cursor
            .read_u32::<BigEndian>()
            .expect("Failed to read key length") as usize;
        let current_pos = cursor.position() as usize;

        // Check for potential overflow or incomplete data before allocation
        if current_pos.checked_add(key_len).is_none() {
            panic!("Key length overflow");
        }

        if current_pos + key_len > run_data.len() {
            panic!("Incomplete key data");
        }

        let key_slice = &run_data[current_pos..current_pos + key_len];

        match key_slice.cmp(search_key.as_bytes()) {
            std::cmp::Ordering::Less => {
                // Key is smaller, skip this entry and continue
                cursor.set_position((current_pos + key_len) as u64); // Move cursor past the key
                if marker == MARKER_PUT {
                    // Check if we have enough data to read the value length
                    if cursor.position() + 4 > run_data.len() as u64 {
                        panic!("Incomplete value length data");
                    }

                    // Skip value if it was a Put operation
                    let value_len = cursor
                        .read_u32::<BigEndian>()
                        .expect("Failed to read value length")
                        as usize;
                    let val_pos = cursor.position() as usize;

                    // Check for potential overflow or incomplete data
                    if val_pos.checked_add(value_len).is_none() {
                        panic!("Value length overflow");
                    }

                    if val_pos + value_len > run_data.len() {
                        panic!("Incomplete value data");
                    }

                    cursor.set_position((val_pos + value_len) as u64); // Move cursor past the value
                }
                // If marker was Delete, we've already skipped the key, nothing more to skip.
            },
            std::cmp::Ordering::Equal => {
                // Found the key
                cursor.set_position((current_pos + key_len) as u64); // Move cursor past the key
                return match marker {
                    MARKER_PUT => {
                        // Check if we have enough data to read the value length
                        if cursor.position() + 4 > run_data.len() as u64 {
                            panic!("Incomplete value length data for found key");
                        }

                        let value_len = cursor
                            .read_u32::<BigEndian>()
                            .expect("Failed to read value length")
                            as usize;
                        let val_pos = cursor.position() as usize;

                        // Check for potential overflow or incomplete data
                        if val_pos.checked_add(value_len).is_none() {
                            panic!("Value length overflow for found key");
                        }

                        if val_pos + value_len > run_data.len() {
                            panic!("Incomplete value data for found key");
                        }

                        let value = run_data[val_pos..val_pos + value_len].to_vec();
                        SearchResult::Found(value)
                    },
                    MARKER_DELETE => SearchResult::Tombstone,
                    _ => panic!("Invalid marker byte: {}", marker),
                };
            },
            std::cmp::Ordering::Greater => {
                // Current key is larger than search key. Since runs are sorted,
                // the key cannot exist further in the run.
                return SearchResult::NotFound;
            },
        }
    }

    // Reached end of run without finding the key
    SearchResult::NotFound
}

/// Reads a serialized run file and returns a stream of write operations.
///
/// This function takes a stream of bytes representing a serialized run and returns
/// a stream that yields each write operation (Put or Delete) contained in the run.
pub fn read_run_stream<S>(stream: S) -> impl Stream<Item = Result<WriteOperation, RunError>>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
{
    async_stream::stream! {
        let mut buffer = Vec::new();
        let mut stream = stream.fuse();

        // Read the entire stream into a buffer first
        // In a future optimization, we could process the stream incrementally
        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => buffer.extend_from_slice(&chunk),
                Err(e) => {
                    yield Err(RunError::Io(e));
                    return;
                }
            }
        }

        if buffer.is_empty() {
            yield Err(RunError::EmptyInput);
            return;
        }

        let mut cursor = Cursor::new(&buffer);

        // Read version
        let version = match cursor.read_u8() {
            Ok(v) => v,
            Err(e) => {
                yield Err(RunError::Io(e));
                return;
            }
        };

        if version != CURRENT_VERSION {
            yield Err(RunError::UnsupportedVersion(version));
            return;
        }

        // Process entries until we reach the end of the buffer
        while cursor.position() < buffer.len() as u64 {
            // Read marker byte
            let marker = match cursor.read_u8() {
                Ok(m) => m,
                Err(e) => {
                    yield Err(RunError::Io(e));
                    return;
                }
            };

            // Read key length
            let key_len = match cursor.read_u32::<BigEndian>() {
                Ok(len) => len as usize,
                Err(e) => {
                    yield Err(RunError::Io(e));
                    return;
                }
            };

            // Read key
            let key_pos = cursor.position() as usize;
            if key_pos + key_len > buffer.len() {
                yield Err(RunError::Format("Incomplete key data".to_string()));
                return;
            }

            let key = match std::str::from_utf8(&buffer[key_pos..key_pos + key_len]) {
                Ok(k) => k.to_string(),
                Err(_) => {
                    yield Err(RunError::Format("Invalid UTF-8 in key".to_string()));
                    return;
                }
            };

            cursor.set_position((key_pos + key_len) as u64);

            match marker {
                MARKER_PUT => {
                    // Read value length
                    let value_len = match cursor.read_u32::<BigEndian>() {
                        Ok(len) => len as usize,
                        Err(e) => {
                            yield Err(RunError::Io(e));
                            return;
                        }
                    };

                    // Read value
                    let val_pos = cursor.position() as usize;
                    if val_pos + value_len > buffer.len() {
                        yield Err(RunError::Format("Incomplete value data".to_string()));
                        return;
                    }

                    let value = buffer[val_pos..val_pos + value_len].to_vec();
                    cursor.set_position((val_pos + value_len) as u64);

                    yield Ok(WriteOperation::Put(key, value));
                },
                MARKER_DELETE => {
                    yield Ok(WriteOperation::Delete(key));
                },
                _ => {
                    yield Err(RunError::Format(format!("Invalid marker byte: {}", marker)));
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;

    use super::*;

    fn value(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[tokio::test]
    async fn test_create_run_simple() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Put("banana".to_string(), value("yellow")),
        ];

        let results: Vec<Result<(Bytes, Stats), RunError>> =
            build_runs(stream::iter(ops.into_iter().map(Ok)))
                .collect()
                .await;

        // Assert exactly one run was produced for this small input
        assert_eq!(results.len(), 1);
        let (data, stats) = results.into_iter().next().unwrap().unwrap();

        match stats {
            Stats::StatsV1(stats) => {
                assert_eq!(stats.min_key, "apple");
                assert_eq!(stats.max_key, "banana");
                // Expected size: version(1) +
                // apple: marker(1) + keylen(4) + key(5) + vallen(4) + val(3) = 17
                // banana: marker(1) + keylen(4) + key(6) + vallen(4) + val(6) = 21
                // total = 1 + 17 + 21 = 39
                assert_eq!(stats.size_bytes, 39);
            },
        }

        // Basic check of the first byte (version)
        assert_eq!(data[0], 1);
    }

    #[tokio::test]
    async fn test_create_run_with_duplicates() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("green")),
            WriteOperation::Put("apple".to_string(), value("red")),
        ];
        let results: Vec<_> = build_runs(stream::iter(ops.into_iter().map(Ok)))
            .collect()
            .await;
        assert_eq!(results.len(), 1); // Expect one item, which is an error
        assert!(matches!(results[0], Err(RunError::Format(_))));
    }

    #[tokio::test]
    async fn test_create_run_empty_input() {
        let ops: Vec<WriteOperation> = vec![];
        let results: Vec<_> = build_runs(stream::iter(ops.into_iter().map(Ok)))
            .collect()
            .await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_search_run_found() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Put("banana".to_string(), value("yellow")),
            WriteOperation::Put("cherry".to_string(), value("red")),
        ];
        let results: Vec<_> = build_runs(stream::iter(ops.into_iter().map(Ok)))
            .collect()
            .await;
        assert_eq!(results.len(), 1);
        let (data, _) = results.into_iter().next().unwrap().unwrap();

        assert_eq!(
            search_run(&data, "banana"),
            SearchResult::Found(value("yellow"))
        );
        assert_eq!(
            search_run(&data, "apple"),
            SearchResult::Found(value("red"))
        );
        assert_eq!(
            search_run(&data, "cherry"),
            SearchResult::Found(value("red"))
        );
    }

    #[tokio::test]
    async fn test_search_run_tombstone() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Delete("banana".to_string()),
            WriteOperation::Put("cherry".to_string(), value("red")),
        ];
        let results: Vec<_> = build_runs(stream::iter(ops.into_iter().map(Ok)))
            .collect()
            .await;
        assert_eq!(results.len(), 1);
        let (data, _) = results.into_iter().next().unwrap().unwrap();

        assert_eq!(search_run(&data, "banana"), SearchResult::Tombstone);
        assert_eq!(
            search_run(&data, "apple"),
            SearchResult::Found(value("red"))
        );
    }

    #[tokio::test]
    async fn test_search_run_not_found() {
        let ops = vec![
            WriteOperation::Put("banana".to_string(), value("yellow")),
            WriteOperation::Put("date".to_string(), value("brown")),
        ];
        let results: Vec<_> = build_runs(stream::iter(ops.into_iter().map(Ok)))
            .collect()
            .await;
        assert_eq!(results.len(), 1);
        let (data, _) = results.into_iter().next().unwrap().unwrap();

        // Key too small
        assert_eq!(search_run(&data, "apple"), SearchResult::NotFound);
        // Key in between
        assert_eq!(search_run(&data, "cherry"), SearchResult::NotFound);
        // Key too large
        assert_eq!(search_run(&data, "elderberry"), SearchResult::NotFound);
    }

    #[test]
    #[should_panic(expected = "Empty run data")]
    fn test_search_run_empty_data() {
        let data = vec![];
        search_run(&data, "any");
    }

    #[test]
    #[should_panic(expected = "Unsupported version: 2")]
    fn test_search_run_invalid_version() {
        let data = vec![2, 0]; // Version 2, dummy data
        search_run(&data, "any");
    }

    #[tokio::test]
    async fn test_create_run_with_iterator() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Put("banana".to_string(), value("yellow")),
        ];
        let ops_copy = ops.clone();
        let results1: Vec<_> = build_runs(stream::iter(ops_copy.into_iter().map(Ok)))
            .collect()
            .await;
        assert_eq!(results1.len(), 1);
        let (data1, _) = results1.into_iter().next().unwrap().unwrap();

        // Using an array
        let array_ops = [
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Put("banana".to_string(), value("yellow")),
        ];
        let results2: Vec<_> = build_runs(stream::iter(array_ops.into_iter().map(Ok)))
            .collect()
            .await;
        assert_eq!(results2.len(), 1);
        let (data2, _) = results2.into_iter().next().unwrap().unwrap();

        // All should produce the same serialized data
        assert_eq!(data1, data2);
    }

    // Add a new test case for splitting runs
    #[tokio::test]
    async fn test_create_multiple_runs_due_to_size() {
        // Temporarily override MAX_RUN_SIZE_BYTES for this test scope - this requires more setup.
        // For now, let's simulate by assuming each op exceeds a hypothetical small limit.
        // A more robust test would involve actually setting a low limit, maybe via config or
        // feature flag.

        // Simulating the logic with a very small conceptual limit (e.g., 30 bytes).
        // Version byte (1)
        // Run 1: apple (1+4+5+4+3=17) -> Total = 1 + 17 = 18. Below limit.
        // Run 2: banana (1+4+6+4+6=21). Would exceed limit (18+21=39). Start new run.
        //        New Run 2: version(1) + banana(21) = 22. Below limit.
        // Run 3: cherry (1+4+6+4+4=19). Would exceed limit (22+19=41). Start new run.
        //        New Run 3: version(1) + cherry(19) = 20.

        // Create a larger stream to test the actual 128MB limit splitting (this will be slow/large)
        // We need a helper to generate large-ish data.
        fn generate_op(key_prefix: &str, index: usize, size: usize) -> WriteOperation {
            let key = format!("{}_{:010}", key_prefix, index);
            let value = vec![0u8; size]; // Generate a value of specified size
            WriteOperation::Put(key, value)
        }

        const OP_SIZE: usize = 1024 * 1024; // ~1MB per operation value
        const OPS_PER_RUN_APPROX: usize = (MAX_RUN_SIZE_BYTES as usize) / OP_SIZE;

        let mut large_ops = Vec::new();
        // Generate enough ops to create at least two runs
        for i in 0..(OPS_PER_RUN_APPROX + 50) {
            // Key size adds a bit, value size is dominant
            let overhead = 1 + 4 + format!("{}_{:010}", "key", i).len() + 4;
            large_ops.push(generate_op("key", i, OP_SIZE - overhead)); // Adjust value size to make op roughly OP_SIZE
        }

        let stream = stream::iter(large_ops.into_iter().map(Ok));
        let results: Vec<Result<(Bytes, Stats), RunError>> = build_runs(stream).collect().await;

        // Assert that more than one run was produced
        assert!(
            results.len() > 1,
            "Expected multiple runs, got {}",
            results.len()
        );

        // Basic sanity checks on the yielded runs
        let mut previous_max_key: Option<String> = None;
        for (i, result) in results.iter().enumerate() {
            match result {
                Ok((_data, stats)) => {
                    match stats {
                        Stats::StatsV1(s) => {
                            println!(
                                "Run {}: min={}, max={}, put={}, delete={}, size={}",
                                i, s.min_key, s.max_key, s.put_count, s.delete_count, s.size_bytes
                            );
                            // Check size is close to the limit (except maybe the last one)
                            if i < results.len() - 1 {
                                assert!(
                                    s.size_bytes <= MAX_RUN_SIZE_BYTES,
                                    "Run {} size {} exceeded limit {}",
                                    i,
                                    s.size_bytes,
                                    MAX_RUN_SIZE_BYTES
                                );
                                // Check it's reasonably full (e.g., > 90%? - this might be too
                                // strict depending on op sizes)
                                // assert!(s.size_bytes > (MAX_RUN_SIZE_BYTES * 9 / 10));
                            } else {
                                assert!(
                                    s.size_bytes <= MAX_RUN_SIZE_BYTES,
                                    "Last run size {} exceeded limit {}",
                                    s.size_bytes,
                                    MAX_RUN_SIZE_BYTES
                                );
                            }

                            // Check keys are sorted across runs
                            if let Some(ref prev_max) = previous_max_key {
                                assert!(
                                    s.min_key > *prev_max,
                                    "Run {} min_key {} is not > previous max_key {}",
                                    i,
                                    s.min_key,
                                    prev_max
                                );
                            }
                            previous_max_key = Some(s.max_key.clone());
                        },
                    }
                },
                Err(e) => panic!("Unexpected error in run stream: {:?}", e),
            }
        }
    }
}
