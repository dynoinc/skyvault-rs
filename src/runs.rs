use std::collections::BTreeMap;
use std::io::{Cursor, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

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
            WriteOperation::Put(k, _) => k,
            WriteOperation::Delete(k) => k,
        }
    }
}

// Metadata about a created run
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunMetadata {
    pub version: u8,
    pub min_key: Key,
    pub max_key: Key,
    pub size_bytes: usize,
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

/// Creates a serialized run file (v1) from a list of write operations.
///
/// Operations are deduplicated (last write wins) and sorted by key.
/// Returns the serialized byte vector and metadata.
pub fn create_run(operations: Vec<WriteOperation>) -> Result<(Vec<u8>, RunMetadata), RunError> {
    if operations.is_empty() {
        return Err(RunError::EmptyInput);
    }

    // Deduplicate operations, keeping the last one for each key
    let mut unique_ops = BTreeMap::new();
    for op in operations {
        unique_ops.insert(op.key().to_string(), op);
    }

    // Extract sorted, unique operations
    let sorted_ops: Vec<WriteOperation> = unique_ops.into_values().collect();

    // Determine min and max keys
    // We already checked that sorted_ops is not empty due to the initial check
    let min_key = sorted_ops.first().unwrap().key().to_string();
    let max_key = sorted_ops.last().unwrap().key().to_string();

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
            }
            WriteOperation::Delete(key) => {
                cursor.write_u8(MARKER_DELETE)?;
                // Key
                cursor.write_u32::<BigEndian>(key.len() as u32)?;
                cursor.write_all(key.as_bytes())?;
            }
        }
    }

    let size_bytes = cursor.position() as usize;

    let metadata = RunMetadata {
        version: CURRENT_VERSION,
        min_key,
        max_key,
        size_bytes,
    };

    Ok((buffer, metadata))
}

/// Searches for a key within a serialized run (v1).
pub fn search_run(run_data: &[u8], search_key: &str) -> Result<SearchResult, RunError> {
    let mut cursor = Cursor::new(run_data);

    // Read and check version
    let version = cursor.read_u8()?;
    if version != CURRENT_VERSION {
        return Err(RunError::UnsupportedVersion(version));
    }

    // Iterate through entries
    while cursor.position() < run_data.len() as u64 {
        let marker = cursor.read_u8()?;

        // Read key
        let key_len = cursor.read_u32::<BigEndian>()? as usize;
        let current_pos = cursor.position() as usize;
         // Check for potential overflow or incomplete data before allocation
        if current_pos.checked_add(key_len).map_or(true, |end| end > run_data.len()) {
            return Err(RunError::Format("Incomplete key data".to_string()));
        }
        let key_slice = &run_data[current_pos..current_pos + key_len];


        match key_slice.cmp(search_key.as_bytes()) {
            std::cmp::Ordering::Less => {
                // Key is smaller, skip this entry and continue
                cursor.set_position((current_pos + key_len) as u64); // Move cursor past the key
                if marker == MARKER_PUT {
                    // Skip value if it was a Put operation
                    let value_len = cursor.read_u32::<BigEndian>()? as usize;
                     let val_pos = cursor.position() as usize;
                    // Check for potential overflow or incomplete data
                    if val_pos.checked_add(value_len).map_or(true, |end| end > run_data.len()) {
                         return Err(RunError::Format("Incomplete value data".to_string()));
                    }
                    cursor.set_position((val_pos + value_len) as u64); // Move cursor past the value
                }
                // If marker was Delete, we've already skipped the key, nothing more to skip.
            }
            std::cmp::Ordering::Equal => {
                // Found the key
                 cursor.set_position((current_pos + key_len) as u64); // Move cursor past the key
                return match marker {
                    MARKER_PUT => {
                        let value_len = cursor.read_u32::<BigEndian>()? as usize;
                         let val_pos = cursor.position() as usize;
                        // Check for potential overflow or incomplete data
                        if val_pos.checked_add(value_len).map_or(true, |end| end > run_data.len()) {
                             return Err(RunError::Format("Incomplete value data for found key".to_string()));
                        }
                        let value = run_data[val_pos..val_pos + value_len].to_vec();
                        Ok(SearchResult::Found(value))
                    }
                    MARKER_DELETE => Ok(SearchResult::Tombstone),
                    _ => Err(RunError::Format(format!("Invalid marker byte: {}", marker))),
                };
            }
            std::cmp::Ordering::Greater => {
                // Current key is larger than search key. Since runs are sorted,
                // the key cannot exist further in the run.
                return Ok(SearchResult::NotFound);
            }
        }
    }

    // Reached end of run without finding the key
    Ok(SearchResult::NotFound)
}


#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    fn value(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_create_run_simple() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Put("banana".to_string(), value("yellow")),
        ];
        let (data, metadata) = create_run(ops).unwrap();

        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.min_key, "apple");
        assert_eq!(metadata.max_key, "banana");
        // Expected size: version(1) +
        // apple: marker(1) + keylen(4) + key(5) + vallen(4) + val(3) = 17
        // banana: marker(1) + keylen(4) + key(6) + vallen(4) + val(6) = 21
        // total = 1 + 17 + 21 = 39
        assert_eq!(metadata.size_bytes, 39);

        // Basic check of the first byte (version)
        assert_eq!(data[0], 1);
    }

     #[test]
    fn test_create_run_with_delete_and_duplicates() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("green")), // Overwritten
            WriteOperation::Put("cherry".to_string(), value("red")),
            WriteOperation::Put("apple".to_string(), value("red")),   // Kept
            WriteOperation::Delete("banana".to_string()),             // Kept
            WriteOperation::Put("banana".to_string(), value("yellow")), // Overwritten by delete
        ];
        let (data, metadata) = create_run(ops).unwrap();

        assert_eq!(metadata.version, 1);
        assert_eq!(metadata.min_key, "apple");
        assert_eq!(metadata.max_key, "cherry");
        // Expected entries: apple(Put), banana(Delete), cherry(Put)
        // apple: marker(1) + keylen(4) + key(5) + vallen(4) + val(3) = 17
        // banana: marker(1) + keylen(4) + key(6) = 11
        // cherry: marker(1) + keylen(4) + key(6) + vallen(4) + val(3) = 18
        // total = 1 + 17 + 11 + 18 = 47
        assert_eq!(metadata.size_bytes, 47);
        assert_eq!(data[0], 1); // Version
    }

    #[test]
    fn test_create_run_empty_input() {
        let ops = vec![];
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

        assert_eq!(search_run(&data, "banana").unwrap(), SearchResult::Found(value("yellow")));
        assert_eq!(search_run(&data, "apple").unwrap(), SearchResult::Found(value("red")));
         assert_eq!(search_run(&data, "cherry").unwrap(), SearchResult::Found(value("red")));
    }

     #[test]
    fn test_search_run_tombstone() {
        let ops = vec![
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Delete("banana".to_string()),
            WriteOperation::Put("cherry".to_string(), value("red")),
        ];
        let (data, _) = create_run(ops).unwrap();

        assert_eq!(search_run(&data, "banana").unwrap(), SearchResult::Tombstone);
         assert_eq!(search_run(&data, "apple").unwrap(), SearchResult::Found(value("red")));
    }

    #[test]
    fn test_search_run_not_found() {
         let ops = vec![
            WriteOperation::Put("banana".to_string(), value("yellow")),
            WriteOperation::Put("date".to_string(), value("brown")),
        ];
        let (data, _) = create_run(ops).unwrap();

        // Key too small
        assert_eq!(search_run(&data, "apple").unwrap(), SearchResult::NotFound);
        // Key in between
        assert_eq!(search_run(&data, "cherry").unwrap(), SearchResult::NotFound);
        // Key too large
        assert_eq!(search_run(&data, "elderberry").unwrap(), SearchResult::NotFound);
    }

     #[test]
    fn test_search_run_empty_data() {
        let data = vec![];
        let result = search_run(&data, "any");
        // Expecting an I/O error because it can't even read the version byte
        assert!(matches!(result, Err(RunError::Io(_))));
    }

     #[test]
    fn test_search_run_invalid_version() {
        let data = vec![2, 0]; // Version 2, dummy data
        let result = search_run(&data, "any");
        assert!(matches!(result, Err(RunError::UnsupportedVersion(2))));
    }

     #[test]
    fn test_search_run_malformed_data() {
         let ops = vec![WriteOperation::Put("apple".to_string(), value("red"))];
         let (mut data, _) = create_run(ops).unwrap();

        // Truncate data mid-entry
        let bad_data = &data[..20]; // Truncate somewhere in the middle
        let result = search_run(bad_data, "apple");
        assert!(matches!(result, Err(RunError::Format(_)) | Err(RunError::Io(_))));


        // Corrupt marker
        data[1] = 0x99; // Invalid marker after version byte
        let result_marker = search_run(&data, "apple");
         // This specific corruption leads to reading key length where marker should be,
         // then attempting to read a key based on that corrupt length, likely causing Format/IO error.
        assert!(matches!(result_marker, Err(RunError::Format(_)) | Err(RunError::Io(_))));


    }
     #[test]
    fn test_search_run_malformed_key_len() {
        // version(1) + marker(1) + key_len(4) = 6 bytes minimum for header + one entry start
        let mut data = vec![1, MARKER_PUT, 0, 0, 0, 100]; // Version 1, Put, key_len=100
        // Add only 5 bytes for the key, not 100
        data.extend_from_slice(b"short");

        let result = search_run(&data, "any");
        // Should fail when trying to read 100 bytes for the key
        assert!(matches!(result, Err(RunError::Format(msg)) if msg == "Incomplete key data"));
    }

    #[test]
    fn test_search_run_malformed_value_len() {
        let key = key("apple");
        let value = value("red");
        let mut buffer = Vec::new();
        let mut cursor = Cursor::new(&mut buffer);

        cursor.write_u8(CURRENT_VERSION).unwrap(); // Version
        cursor.write_u8(MARKER_PUT).unwrap(); // Marker
        cursor.write_u32::<BigEndian>(key.len() as u32).unwrap(); // Key len
        cursor.write_all(&key).unwrap(); // Key
        cursor.write_u32::<BigEndian>(100).unwrap(); // Value len = 100
        cursor.write_all(&value).unwrap(); // Actual value is only 3 bytes

        let result = search_run(&buffer, "apple");
        // Should fail when trying to read 100 bytes for the value after finding the key
        assert!(matches!(result, Err(RunError::Format(msg)) if msg == "Incomplete value data for found key"));
    }

    #[test]
    fn test_search_run_malformed_value_len_skip() {
        // Create data for two entries: ("apple", "red") and ("cherry", "red")
         let ops = vec![
            WriteOperation::Put("apple".to_string(), value("red")),
            WriteOperation::Put("cherry".to_string(), value("red")),
        ];
        let (mut data, _) = create_run(ops).unwrap();

        // Find the start of the "apple" value length (1 byte version + 1 byte marker + 4 bytes keylen + 5 bytes key = 11)
        let value_len_pos = 1 + 1 + 4 + 5;
        // Corrupt the value length of "apple" to be huge
        let mut cursor = Cursor::new(&mut data[value_len_pos..]);
        cursor.write_u32::<BigEndian>(1000).unwrap(); // Write huge length

        // Search for "cherry". This requires *skipping* the corrupted "apple" entry.
        let result = search_run(&data, "cherry");
        // It should fail when trying to skip the value part of "apple"
        assert!(matches!(result, Err(RunError::Format(msg)) if msg == "Incomplete value data"));

    }
}
