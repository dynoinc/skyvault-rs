use async_trait::async_trait;
use rusqlite::{Connection, params};
use std::collections::HashMap;
use thiserror::Error;
use std::pin::Pin;
use futures::stream::{Stream, StreamExt};
use futures::channel::mpsc::{channel, Receiver};
use futures::sink::SinkExt;
use std::task::{Context, Poll};

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] rusqlite::Error),

    #[error("Invalid key: {0}")]
    InvalidKey(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

/// Key composed of a partition key and sort key
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompositeKey {
    pub partition_key: String,
    pub sort_key: String,
}

impl CompositeKey {
    pub fn new(partition_key: impl Into<String>, sort_key: impl Into<String>) -> Self {
        Self {
            partition_key: partition_key.into(),
            sort_key: sort_key.into(),
        }
    }
}

/// Batch operation for writing
#[derive(Debug, Clone)]
pub enum BatchOp<T> {
    Put(CompositeKey, T),
    Delete(CompositeKey),
}

pub struct KeyValueStream<T> {
    receiver: Receiver<Result<(CompositeKey, T), MetadataError>>,
}

impl<T: 'static + Send> Stream for KeyValueStream<T> {
    type Item = Result<(CompositeKey, T), MetadataError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_next_unpin(cx)
    }
}

#[async_trait]
pub trait MetadataStore: Send + Sync {
    type Value: Send + Sync;
    
    /// Get a single value by key
    async fn get(&self, key: &CompositeKey) -> Result<Option<Self::Value>, MetadataError>;
    
    /// Get multiple values by keys
    async fn multi_get(&self, keys: &[CompositeKey]) -> Result<HashMap<CompositeKey, Self::Value>, MetadataError>;
    
    /// Put a single value
    async fn put(&self, key: &CompositeKey, value: Self::Value) -> Result<(), MetadataError>;
    
    /// Delete a single key
    async fn delete(&self, key: &CompositeKey) -> Result<(), MetadataError>;
    
    /// Batch write operations (puts and deletes)
    async fn batch_write(&self, operations: Vec<BatchOp<Self::Value>>) -> Result<(), MetadataError>;
    
    /// List keys as a stream
    fn list(&self, partition_key: String, batch_size: usize, start_after: Option<String>) -> KeyValueStream<Self::Value>;
}

pub struct SqliteMetadataStore {
    db_path: String,
}

impl SqliteMetadataStore {
    pub fn new(db_path: impl Into<String>) -> Result<Self, MetadataError> {
        let db_path = db_path.into();
        let conn = Connection::open(&db_path)?;
        
        // Create table if it doesn't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS metadata (
                partition_key TEXT NOT NULL,
                sort_key TEXT NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY (partition_key, sort_key)
            )",
            [],
        )?;
        
        Ok(Self { db_path })
    }
    
    fn get_connection(&self) -> Result<Connection, MetadataError> {
        Ok(Connection::open(&self.db_path)?)
    }
}

#[async_trait]
impl MetadataStore for SqliteMetadataStore {
    type Value = Vec<u8>;
    
    async fn get(&self, key: &CompositeKey) -> Result<Option<Self::Value>, MetadataError> {
        let conn = self.get_connection()?;
        let mut stmt = conn.prepare("SELECT value FROM metadata WHERE partition_key = ?1 AND sort_key = ?2")?;
        let mut rows = stmt.query(params![key.partition_key, key.sort_key])?;
        
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }
    
    async fn multi_get(&self, keys: &[CompositeKey]) -> Result<HashMap<CompositeKey, Self::Value>, MetadataError> {
        let conn = self.get_connection()?;
        let mut result = HashMap::new();
        
        for key in keys {
            let mut stmt = conn.prepare("SELECT value FROM metadata WHERE partition_key = ?1 AND sort_key = ?2")?;
            let mut rows = stmt.query(params![key.partition_key, key.sort_key])?;
            
            if let Some(row) = rows.next()? {
                result.insert(key.clone(), row.get(0)?);
            }
        }
        
        Ok(result)
    }
    
    async fn put(&self, key: &CompositeKey, value: Self::Value) -> Result<(), MetadataError> {
        let conn = self.get_connection()?;
        conn.execute(
            "INSERT OR REPLACE INTO metadata (partition_key, sort_key, value) VALUES (?1, ?2, ?3)",
            params![key.partition_key, key.sort_key, value],
        )?;
        
        Ok(())
    }
    
    async fn delete(&self, key: &CompositeKey) -> Result<(), MetadataError> {
        let conn = self.get_connection()?;
        conn.execute(
            "DELETE FROM metadata WHERE partition_key = ?1 AND sort_key = ?2",
            params![key.partition_key, key.sort_key],
        )?;
        
        Ok(())
    }
    
    async fn batch_write(&self, operations: Vec<BatchOp<Self::Value>>) -> Result<(), MetadataError> {
        let mut conn = self.get_connection()?;
        let tx = conn.transaction()?;
        
        for op in operations {
            match op {
                BatchOp::Put(key, value) => {
                    tx.execute(
                        "INSERT OR REPLACE INTO metadata (partition_key, sort_key, value) VALUES (?1, ?2, ?3)",
                        params![key.partition_key, key.sort_key, value],
                    )?;
                },
                BatchOp::Delete(key) => {
                    tx.execute(
                        "DELETE FROM metadata WHERE partition_key = ?1 AND sort_key = ?2",
                        params![key.partition_key, key.sort_key],
                    )?;
                }
            }
        }
        
        tx.commit()?;
        Ok(())
    }
    
    fn list(&self, partition_key: String, batch_size: usize, start_after: Option<String>) -> KeyValueStream<Self::Value> {
        let (mut sender, receiver) = channel(batch_size);
        let db_path = self.db_path.clone();
        
        tokio::spawn(async move {
            let mut current_start = start_after;
            
            loop {
                match fetch_batch(&db_path, &partition_key, batch_size, current_start.as_deref()).await {
                    Ok(batch) => {
                        if batch.is_empty() {
                            break;
                        }
                        
                        let last_key = batch.last().map(|(key, _)| key.sort_key.clone());
                        
                        for item in batch {
                            if let Err(_) = sender.send(Ok(item)).await {
                                return; // Receiver was dropped
                            }
                        }
                        
                        if let Some(last) = last_key {
                            current_start = Some(last);
                        } else {
                            break;
                        }
                    },
                    Err(e) => {
                        let _ = sender.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
        
        KeyValueStream { receiver }
    }
}

async fn fetch_batch(db_path: &str, partition_key: &str, limit: usize, start_after: Option<&str>) 
    -> Result<Vec<(CompositeKey, Vec<u8>)>, MetadataError> {
    let conn = Connection::open(db_path)?;
    
    let (query, params_values) = if let Some(start_key) = start_after {
        (
            "SELECT partition_key, sort_key, value FROM metadata 
            WHERE partition_key = ?1 AND sort_key > ?2 
            ORDER BY sort_key ASC LIMIT ?3",
            params![partition_key, start_key.to_string(), limit as u32]
        )
    } else {
        (
            "SELECT partition_key, sort_key, value FROM metadata 
            WHERE partition_key = ?1 
            ORDER BY sort_key ASC LIMIT ?2",
            params![partition_key, limit as u32]
        )
    };
    
    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map(params_values, |row| {
        Ok((
            CompositeKey {
                partition_key: row.get(0)?,
                sort_key: row.get(1)?,
            },
            row.get(2)?,
        ))
    })?;
    
    let mut items = Vec::with_capacity(limit);
    for row in rows {
        items.push(row?);
    }
    
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[tokio::test]
    async fn test_sqlite_metadata_store_basic_operations() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db").to_str().unwrap().to_string();
        
        let store = SqliteMetadataStore::new(db_path).unwrap();
        
        // Test put and get
        let key = CompositeKey::new("test-partition", "test-key");
        let value = b"test-value".to_vec();
        
        store.put(&key, value.clone()).await.unwrap();
        let result = store.get(&key).await.unwrap();
        
        assert_eq!(result, Some(value.clone()));
        
        // Test delete
        store.delete(&key).await.unwrap();
        let result = store.get(&key).await.unwrap();
        assert_eq!(result, None);
        
        // Test batch operations
        let key1 = CompositeKey::new("test-batch", "key1");
        let key2 = CompositeKey::new("test-batch", "key2");
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();
        
        let ops = vec![
            BatchOp::Put(key1.clone(), value1.clone()),
            BatchOp::Put(key2.clone(), value2.clone()),
        ];
        
        store.batch_write(ops).await.unwrap();
        
        let keys = vec![key1.clone(), key2.clone()];
        let results = store.multi_get(&keys).await.unwrap();
        
        assert_eq!(results.get(&key1), Some(&value1));
        assert_eq!(results.get(&key2), Some(&value2));
        
        // Test list
        let mut items = Vec::new();
        let mut stream = store.list("test-batch".to_string(), 10, None);
        
        while let Some(item) = stream.next().await {
            items.push(item.unwrap());
        }
        
        assert_eq!(items.len(), 2);
    }
}
