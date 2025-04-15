use async_trait::async_trait;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Invalid configuration: {0}")]
    ConfigError(String),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn get_object(&self, key: &str) -> Result<Vec<u8>, StorageError>;
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError>;
    async fn delete_object(&self, key: &str) -> Result<(), StorageError>;
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
}

pub struct LocalObjectStore {
    root_dir: std::path::PathBuf,
}

impl LocalObjectStore {
    pub fn new(root_dir: impl Into<std::path::PathBuf>) -> Result<Self, StorageError> {
        let root_dir = root_dir.into();
        
        // Create the directory if it doesn't exist
        if !root_dir.exists() {
            std::fs::create_dir_all(&root_dir)?;
        } else if !root_dir.is_dir() {
            return Err(StorageError::ConfigError(format!(
                "Path exists but is not a directory: {}",
                root_dir.display()
            )));
        }
        
        Ok(Self { root_dir })
    }
    
    fn object_path(&self, key: &str) -> std::path::PathBuf {
        self.root_dir.join(key)
    }
}

#[async_trait]
impl ObjectStore for LocalObjectStore {
    async fn get_object(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let path = self.object_path(key);
        tokio::fs::read(path).await.map_err(Into::into)
    }
    
    async fn put_object(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError> {
        let path = self.object_path(key);
        
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        
        tokio::fs::write(path, data).await.map_err(Into::into)
    }
    
    async fn delete_object(&self, key: &str) -> Result<(), StorageError> {
        let path = self.object_path(key);
        
        if path.exists() {
            tokio::fs::remove_file(path).await?;
        }
        
        Ok(())
    }
    
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let prefix_path = self.root_dir.join(prefix);
        let prefix_str = prefix_path.to_string_lossy().to_string();
        let root_str = self.root_dir.to_string_lossy().to_string();
        
        let mut results = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.root_dir).await?;
        
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                let path_str = path.to_string_lossy().to_string();
                if path_str.starts_with(&prefix_str) {
                    // Convert absolute path to relative key
                    if let Some(key) = path_str.strip_prefix(&root_str) {
                        let key = key.trim_start_matches('/');
                        results.push(key.to_string());
                    }
                }
            }
        }
        
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[tokio::test]
    async fn test_local_object_store() {
        let temp_dir = tempdir().unwrap();
        let store = LocalObjectStore::new(temp_dir.path()).unwrap();
        
        // Test put_object and get_object
        let key = "test/file.txt";
        let data = b"Hello, world!".to_vec();
        
        store.put_object(key, data.clone()).await.unwrap();
        let retrieved = store.get_object(key).await.unwrap();
        assert_eq!(retrieved, data);
        
        // Test list_objects
        let keys = store.list_objects("test").await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], key);
        
        // Test delete_object
        store.delete_object(key).await.unwrap();
        let result = store.get_object(key).await;
        assert!(result.is_err());
        
        // Test list after delete
        let keys = store.list_objects("test").await.unwrap();
        assert_eq!(keys.len(), 0);
    }
}
