use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::runs;
use crate::storage::{ObjectStore, StorageError};

/// Error types for the runs cache operations
#[derive(thiserror::Error, Debug)]
pub enum RunsCacheError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Run search error: {0}")]
    RunError(#[from] runs::RunError),
}

/// A cache manager for run data that caches runs from storage in memory
pub struct RunsCache {
    /// The underlying storage system
    storage: ObjectStore,

    /// In-memory cache of run data
    cache: Arc<RwLock<HashMap<String, Arc<Vec<u8>>>>>,
}

impl RunsCache {
    /// Create a new RunsCache manager
    pub fn new(storage: ObjectStore) -> Self {
        Self {
            storage,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get run data from cache or storage if not cached
    pub async fn get_run(&self, run_id: &str) -> Result<Arc<Vec<u8>>, RunsCacheError> {
        // First check if the run is in the cache
        {
            let cache = self.cache.read().unwrap();
            if let Some(run_data) = cache.get(run_id) {
                return Ok(run_data.clone());
            }
        }

        // Not in cache, fetch from storage
        let run_data = Arc::new(self.fetch_from_storage(run_id).await?);

        // Update cache
        {
            let mut cache = self.cache.write().unwrap();
            cache.insert(run_id.to_string(), run_data.clone());
        }

        Ok(run_data)
    }

    /// Internal method to fetch run data from storage
    async fn fetch_from_storage(&self, run_id: &str) -> Result<Vec<u8>, RunsCacheError> {
        Ok(self.storage.get_run(run_id).await?)
    }
}
