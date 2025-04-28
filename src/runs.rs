use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::proto;

// Type aliases for clarity
pub type Key = String;
pub type Value = Vec<u8>;

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
    #[error("Unsupported run version: {0}")]
    UnsupportedVersion(u8),
    #[error("Input list of operations cannot be empty")]
    EmptyInput,
}

const CURRENT_VERSION: u8 = 1;
const MARKER_PUT: u8 = 0x01;
const MARKER_DELETE: u8 = 0x00;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct StatsV1 {
    pub min_key: String,
    pub max_key: String,
    pub size_bytes: u64,
    pub item_count: u64,
}

impl From<StatsV1> for proto::StatsV1 {
    fn from(stats: StatsV1) -> Self {
        proto::StatsV1 {
            min_key: stats.min_key,
            max_key: stats.max_key,
            size_bytes: stats.size_bytes,
            item_count: stats.item_count,
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

/// Builds a run from a stream of sorted write operations
///
/// Returns a tuple of (serialized run data, stats)
///
/// # Errors
///
/// Returns an error if:
/// - The input stream is empty
/// - The operations are not sorted by key
/// - There's an I/O error during serialization
pub async fn build_run<S>(operations: S) -> Result<(Bytes, Stats), RunError>
where
    S: Stream<Item = Result<WriteOperation, RunError>> + Unpin + Send + Sync + 'static,
{
    let mut min_key: Option<String> = None;
    let mut max_key: Option<String> = None;
    let mut item_count: u64 = 0;
    let mut size_bytes: u64 = 1; // Start with 1 byte for version
    let mut last_key: Option<String> = None;

    // Start with version header
    let mut result = Vec::new();
    result.push(CURRENT_VERSION);

    let mut operations = Box::pin(operations);

    while let Some(op) = operations.next().await {
        let op = op?;
        let current_key = op.key().to_string();

        // Check if keys are sorted
        if let Some(ref last) = last_key {
            if current_key < *last {
                return Err(RunError::Format(
                    "Operations must be sorted by key".to_string(),
                ));
            }
        }

        // Update stats
        if min_key.is_none() {
            min_key = Some(current_key.clone());
        }
        max_key = Some(current_key.clone());
        last_key = Some(current_key);
        item_count += 1;

        // Serialize the operation
        match &op {
            WriteOperation::Put(key, value) => {
                result.push(MARKER_PUT);
                result.extend_from_slice(&(key.len() as u32).to_be_bytes());
                result.extend_from_slice(key.as_bytes());
                result.extend_from_slice(&(value.len() as u32).to_be_bytes());
                result.extend_from_slice(value);

                size_bytes += 1 + 4 + key.len() as u64 + 4 + value.len() as u64;
            },
            WriteOperation::Delete(key) => {
                result.push(MARKER_DELETE);
                result.extend_from_slice(&(key.len() as u32).to_be_bytes());
                result.extend_from_slice(key.as_bytes());

                size_bytes += 1 + 4 + key.len() as u64;
            },
        }
    }

    // Check if any operations were processed
    if item_count == 0 {
        return Err(RunError::EmptyInput);
    }

    let stats = Stats::StatsV1(StatsV1 {
        min_key: min_key.unwrap(),
        max_key: max_key.unwrap(),
        size_bytes,
        item_count,
    });

    Ok((Bytes::from(result), stats))
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

        let (data, stats) = build_run(stream::iter(ops.into_iter().map(|op| Ok(op))))
            .await
            .unwrap();
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
    async fn test_create_run_with_delete_and_duplicates() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("green")), // Overwritten
            WriteOperation::Put("cherry".to_string(), value("red")),
            WriteOperation::Put("apple".to_string(), value("red")), // Kept
            WriteOperation::Delete("banana".to_string()),           // Kept
            WriteOperation::Put("banana".to_string(), value("yellow")), // Overwritten by delete
        ];
        let (data, stats) = build_run(stream::iter(ops.into_iter().map(|op| Ok(op))))
            .await
            .unwrap();
        match stats {
            Stats::StatsV1(stats) => {
                assert_eq!(stats.min_key, "apple");
                assert_eq!(stats.max_key, "cherry");
                assert_eq!(stats.size_bytes, 57);
            },
        }
        assert_eq!(data[0], 1); // Version
    }

    #[tokio::test]
    async fn test_create_run_empty_input() {
        let ops: Vec<WriteOperation> = vec![];
        let result = build_run(stream::iter(ops.into_iter().map(|op| Ok(op)))).await;
        assert!(matches!(result, Err(RunError::EmptyInput)));
    }

    #[tokio::test]
    async fn test_search_run_found() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Put("banana".to_string(), value("yellow")),
            WriteOperation::Put("cherry".to_string(), value("red")),
        ];
        let (data, _) = build_run(stream::iter(ops.into_iter().map(|op| Ok(op))))
            .await
            .unwrap();

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
        let (data, _) = build_run(stream::iter(ops.into_iter().map(|op| Ok(op))))
            .await
            .unwrap();

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
        let (data, _) = build_run(stream::iter(ops.into_iter().map(|op| Ok(op))))
            .await
            .unwrap();

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

        // Test with various iterator types

        // Using into_iter()
        let ops_copy = ops.clone();
        let (data1, _) = build_run(stream::iter(ops_copy.into_iter().map(|op| Ok(op))))
            .await
            .unwrap();

        // Using an array
        let array_ops = [
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Put("banana".to_string(), value("yellow")),
        ];
        let (data2, _) = build_run(stream::iter(array_ops.into_iter().map(|op| Ok(op))))
            .await
            .unwrap();

        // All should produce the same serialized data
        assert_eq!(data1, data2);
    }
}
