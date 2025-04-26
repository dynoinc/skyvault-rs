use std::collections::BTreeMap;
use std::io::{Cursor, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

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
    fn key(&self) -> &str {
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

/// Creates a serialized run file (v1) from an iterator of write operations.
///
/// Operations are deduplicated (last write wins) and sorted by key.
/// Returns the serialized byte vector and metadata.
pub fn create_run<I>(operations: I) -> Result<(Vec<u8>, Stats), RunError>
where
    I: IntoIterator<Item = WriteOperation>,
{
    // Deduplicate operations, keeping the last one for each key
    let mut unique_ops = BTreeMap::new();
    for op in operations {
        unique_ops.insert(op.key().to_string(), op);
    }

    // Check if we received any operations
    if unique_ops.is_empty() {
        return Err(RunError::EmptyInput);
    }

    // Extract sorted, unique operations
    let sorted_ops: Vec<WriteOperation> = unique_ops.into_values().collect();

    // Determine min and max keys - safe now that we've checked for emptiness
    let min_key = sorted_ops
        .first()
        .map(|op| op.key().to_string())
        .ok_or_else(|| RunError::Format("Failed to get minimum key".to_string()))?;
    let max_key = sorted_ops
        .last()
        .map(|op| op.key().to_string())
        .ok_or_else(|| RunError::Format("Failed to get maximum key".to_string()))?;

    let mut buffer = Vec::new();
    let mut cursor = Cursor::new(&mut buffer);

    // Write version
    cursor.write_u8(CURRENT_VERSION)?;

    // Write entries
    for op in &sorted_ops {
        match op {
            WriteOperation::Put(key, value) => {
                cursor.write_u8(MARKER_PUT)?;
                // Key
                cursor.write_u32::<BigEndian>(key.len() as u32)?;
                cursor.write_all(key.as_bytes())?;
                // Value
                cursor.write_u32::<BigEndian>(value.len() as u32)?;
                cursor.write_all(value)?;
            },
            WriteOperation::Delete(key) => {
                cursor.write_u8(MARKER_DELETE)?;
                // Key
                cursor.write_u32::<BigEndian>(key.len() as u32)?;
                cursor.write_all(key.as_bytes())?;
            },
        }
    }

    let size_bytes = cursor.position();
    let item_count = sorted_ops.len() as u64;

    let stats = StatsV1 {
        min_key,
        max_key,
        size_bytes,
        item_count,
    };

    Ok((buffer, Stats::StatsV1(stats)))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn value(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_create_run_simple() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Put("banana".to_string(), value("yellow")),
        ];
        let (data, stats) = create_run(ops).unwrap();
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

    #[test]
    fn test_create_run_with_delete_and_duplicates() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("green")), // Overwritten
            WriteOperation::Put("cherry".to_string(), value("red")),
            WriteOperation::Put("apple".to_string(), value("red")), // Kept
            WriteOperation::Delete("banana".to_string()),           // Kept
            WriteOperation::Put("banana".to_string(), value("yellow")), // Overwritten by delete
        ];
        let (data, stats) = create_run(ops).unwrap();
        match stats {
            Stats::StatsV1(stats) => {
                assert_eq!(stats.min_key, "apple");
                assert_eq!(stats.max_key, "cherry");
                assert_eq!(stats.size_bytes, 57);
            },
        }
        assert_eq!(data[0], 1); // Version
    }

    #[test]
    fn test_create_run_empty_input() {
        let ops: Vec<WriteOperation> = vec![];
        let result = create_run(ops);
        assert!(matches!(result, Err(RunError::EmptyInput)));
    }

    #[test]
    fn test_search_run_found() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Put("banana".to_string(), value("yellow")),
            WriteOperation::Put("cherry".to_string(), value("red")),
        ];
        let (data, _) = create_run(ops).unwrap();

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

    #[test]
    fn test_search_run_tombstone() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Delete("banana".to_string()),
            WriteOperation::Put("cherry".to_string(), value("red")),
        ];
        let (data, _) = create_run(ops).unwrap();

        assert_eq!(
            search_run(&data, "banana"),
            SearchResult::Tombstone
        );
        assert_eq!(
            search_run(&data, "apple"),
            SearchResult::Found(value("red"))
        );
    }

    #[test]
    fn test_search_run_not_found() {
        let ops = vec![
            WriteOperation::Put("banana".to_string(), value("yellow")),
            WriteOperation::Put("date".to_string(), value("brown")),
        ];
        let (data, _) = create_run(ops).unwrap();

        // Key too small
        assert_eq!(search_run(&data, "apple"), SearchResult::NotFound);
        // Key in between
        assert_eq!(search_run(&data, "cherry"), SearchResult::NotFound);
        // Key too large
        assert_eq!(
            search_run(&data, "elderberry"),
            SearchResult::NotFound
        );
    }

    #[test]
    fn test_search_run_empty_data() {
        let data = vec![];
        let result = search_run(&data, "any");
        assert!(matches!(result, SearchResult::NotFound));
    }

    #[test]
    #[should_panic(expected = "Unsupported version: 2")]
    fn test_search_run_invalid_version() {
        let data = vec![2, 0]; // Version 2, dummy data
        search_run(&data, "any");
    }

    #[test]
    fn test_create_run_with_iterator() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Put("banana".to_string(), value("yellow")),
        ];

        // Test with various iterator types

        // Using iter().cloned()
        let ops_iter = ops.iter().cloned();
        let (data1, _) = create_run(ops_iter).unwrap();

        // Using into_iter()
        let ops_copy = ops.clone();
        let (data2, _) = create_run(ops_copy).unwrap();

        // Using an array
        let array_ops = [
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Put("banana".to_string(), value("yellow")),
        ];
        let (data3, _) = create_run(array_ops).unwrap();

        // All should produce the same serialized data
        assert_eq!(data1, data2);
        assert_eq!(data1, data3);
    }
}
