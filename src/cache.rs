use std::{
    fs::{
        File,
        OpenOptions,
    },
    io::Write,
    num::NonZeroUsize,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{
        Context as TaskContext,
        Poll,
    },
};

use anyhow::{
    Context,
    Result,
};
use bytes::Bytes;
use futures_util::Stream;
use lru::LruCache;
use memmap2::{
    Mmap,
    MmapOptions,
};
use tokio::{
    fs,
    io::AsyncRead,
    sync::RwLock,
};
use tracing::{
    debug,
    info,
    warn,
};

/// Configuration for the disk-backed cache
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Directory where cache files are stored
    pub cache_dir: PathBuf,
    /// Maximum number of entries in the cache
    pub max_entries: usize,
}

/// A memory-mapped view of a cache entry
pub struct MmapView {
    _file: File,
    mmap: Mmap,
}

impl MmapView {
    /// Get a byte slice view of the cached data
    pub fn as_bytes(&self) -> &[u8] {
        &self.mmap
    }

    /// Get the length of the cached data
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// Check if the cached data is empty
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }

    /// Convert to Bytes without copying
    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.mmap)
    }

    /// Create a stream of bytes from the mmap view
    pub fn into_stream(self, chunk_size: usize) -> MmapStream {
        MmapStream::new(self, chunk_size)
    }
}

/// A stream that yields chunks of bytes from a memory-mapped file
pub struct MmapStream {
    mmap_view: MmapView,
    position: usize,
    chunk_size: usize,
}

impl MmapStream {
    fn new(mmap_view: MmapView, chunk_size: usize) -> Self {
        Self {
            mmap_view,
            position: 0,
            chunk_size,
        }
    }
}

impl Stream for MmapStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let data_len = this.mmap_view.len();

        if this.position >= data_len {
            return Poll::Ready(None);
        }

        let end = std::cmp::min(this.position + this.chunk_size, data_len);
        let chunk = &this.mmap_view.as_bytes()[this.position..end];
        this.position = end;

        Poll::Ready(Some(Ok(Bytes::copy_from_slice(chunk))))
    }
}

/// Entry metadata for tracking cache usage
#[derive(Debug, Clone)]
struct CacheEntry {
    file_path: PathBuf,
    size: u64,
}

/// Cache state protected by a single lock
#[derive(Debug)]
struct CacheState {
    /// LRU cache for tracking entry usage order
    lru: LruCache<String, CacheEntry>,
    /// Current total size of all cached files
    total_size: u64,
}

/// A disk-backed cache with LRU eviction and mmap support
pub struct DiskCache {
    config: CacheConfig,
    /// Cache state protected by a single lock
    state: Arc<RwLock<CacheState>>,
}

impl DiskCache {
    /// Create a new disk cache with the given configuration
    pub async fn new(config: CacheConfig) -> Result<Self> {
        // Ensure cache directory exists
        fs::create_dir_all(&config.cache_dir).await.with_context(|| {
            format!(
                "Failed to create cache directory: {cache_dir:?}",
                cache_dir = config.cache_dir
            )
        })?;

        let max_entries =
            NonZeroUsize::new(config.max_entries).with_context(|| "max_entries must be greater than 0")?;

        let state = Arc::new(RwLock::new(CacheState {
            lru: LruCache::new(max_entries),
            total_size: 0,
        }));

        let cache = Self { config, state };

        // Load existing cache entries on startup
        cache.load_existing_entries().await?;

        Ok(cache)
    }

    /// Load existing cache entries from disk on startup
    async fn load_existing_entries(&self) -> Result<()> {
        info!("Loading existing cache entries from {:?}", self.config.cache_dir);

        let mut entries = fs::read_dir(&self.config.cache_dir).await.with_context(|| {
            format!(
                "Failed to read cache directory: {cache_dir:?}",
                cache_dir = self.config.cache_dir
            )
        })?;

        let mut loaded_entries = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.is_file() {
                if let Some(key) = path.file_name().and_then(|n| n.to_str()) {
                    let metadata = entry.metadata().await?;
                    let size = metadata.len();

                    loaded_entries.push((
                        key.to_string(),
                        CacheEntry {
                            file_path: path.clone(),
                            size,
                        },
                        metadata.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                    ));

                    debug!("Found cache entry: {} (size: {} bytes)", key, size);
                }
            }
        }

        // Sort by modification time to maintain LRU order (oldest first)
        loaded_entries.sort_by_key(|(_, _, modified)| *modified);

        let mut state = self.state.write().await;

        for (key, entry, _) in loaded_entries {
            state.total_size += entry.size;
            state.lru.put(key, entry);
        }

        info!(
            "Loaded {} cache entries, total size: {} bytes",
            state.lru.len(),
            state.total_size
        );
        Ok(())
    }

    /// Get the file path for a cache key
    fn get_file_path(&self, key: &str) -> PathBuf {
        self.config.cache_dir.join(key)
    }

    /// Put data into the cache from bytes
    pub async fn put(&self, key: String, data: &[u8]) -> Result<()> {
        self.put_impl(key, data).await
    }

    /// Put data into the cache from Bytes
    pub async fn put_bytes(&self, key: String, data: Bytes) -> Result<()> {
        self.put_impl(key, &data).await
    }

    /// Put data into the cache from an async reader (like ByteStream)
    pub async fn put_stream<R>(&self, key: String, mut reader: R) -> Result<()>
    where
        R: AsyncRead + Unpin,
    {
        let file_path = self.get_file_path(&key);

        // Write data to file
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)
            .await
            .with_context(|| format!("Failed to create cache file: {file_path:?}"))?;

        let size = tokio::io::copy(&mut reader, &mut file)
            .await
            .with_context(|| format!("Failed to write stream to cache file: {file_path:?}"))?;

        file.sync_all()
            .await
            .with_context(|| format!("Failed to sync cache file: {file_path:?}"))?;

        let entry = CacheEntry {
            file_path: file_path.clone(),
            size,
        };

        // Update cache state
        let mut state = self.state.write().await;

        // If key already exists, remove old size (though data never changes, this
        // handles overwrites)
        if let Some(old_entry) = state.lru.peek(&key) {
            state.total_size -= old_entry.size;
        }

        // Add new entry and handle eviction
        if let Some(evicted) = state.lru.put(key.clone(), entry) {
            // Remove evicted file
            state.total_size -= evicted.size;
            drop(state); // Release lock before file operation

            if let Err(e) = fs::remove_file(&evicted.file_path).await {
                warn!("Failed to remove evicted cache file {:?}: {}", evicted.file_path, e);
            } else {
                debug!("Evicted cache entry: {:?}", evicted.file_path);
            }
        } else {
            drop(state);
        }

        // Update total size
        let mut state = self.state.write().await;
        state.total_size += size;

        debug!("Cached key '{}' with size {} bytes", key, size);
        Ok(())
    }

    /// Internal implementation for putting data
    async fn put_impl(&self, key: String, data: &[u8]) -> Result<()> {
        let file_path = self.get_file_path(&key);

        // Write data to file
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)
            .with_context(|| format!("Failed to create cache file: {file_path:?}"))?;

        file.write_all(data)
            .with_context(|| format!("Failed to write data to cache file: {file_path:?}"))?;

        file.sync_all()
            .with_context(|| format!("Failed to sync cache file: {file_path:?}"))?;

        let size = data.len() as u64;
        let entry = CacheEntry {
            file_path: file_path.clone(),
            size,
        };

        // Update cache state
        let mut state = self.state.write().await;

        // If key already exists, remove old size (though data never changes, this
        // handles overwrites)
        if let Some(old_entry) = state.lru.peek(&key) {
            state.total_size -= old_entry.size;
        }

        // Add new entry and handle eviction
        if let Some(evicted) = state.lru.put(key.clone(), entry) {
            // Remove evicted file
            state.total_size -= evicted.size;
            let evicted_path = evicted.file_path.clone();
            drop(state); // Release lock before file operation

            if let Err(e) = fs::remove_file(&evicted_path).await {
                warn!("Failed to remove evicted cache file {:?}: {}", evicted_path, e);
            } else {
                debug!("Evicted cache entry: {:?}", evicted_path);
            }
        } else {
            drop(state);
        }

        // Update total size
        let mut state = self.state.write().await;
        state.total_size += size;

        debug!("Cached key '{}' with size {} bytes", key, size);
        Ok(())
    }

    /// Get a memory-mapped view of cached data
    pub async fn get_mmap(&self, key: &str) -> Result<Option<MmapView>> {
        let file_path = {
            let mut state = self.state.write().await;
            if let Some(entry) = state.lru.get(key) {
                entry.file_path.clone()
            } else {
                return Ok(None);
            }
        };

        let file = File::open(&file_path).with_context(|| format!("Failed to open cache file: {file_path:?}"))?;

        let mmap = unsafe {
            MmapOptions::new()
                .map(&file)
                .with_context(|| format!("Failed to mmap cache file: {file_path:?}"))?
        };

        Ok(Some(MmapView { _file: file, mmap }))
    }

    /// Get cached data as a stream of bytes
    pub async fn get_stream(&self, key: &str, chunk_size: usize) -> Result<Option<MmapStream>> {
        if let Some(mmap_view) = self.get_mmap(key).await? {
            Ok(Some(mmap_view.into_stream(chunk_size)))
        } else {
            Ok(None)
        }
    }

    /// Check if a key exists in the cache
    pub async fn contains_key(&self, key: &str) -> bool {
        let state = self.state.read().await;
        state.lru.contains(key)
    }

    /// Remove a key from the cache
    pub async fn remove(&self, key: &str) -> Result<bool> {
        let entry = {
            let mut state = self.state.write().await;
            if let Some(entry) = state.lru.pop(key) {
                state.total_size -= entry.size;
                entry
            } else {
                return Ok(false);
            }
        };

        if let Err(e) = fs::remove_file(&entry.file_path).await {
            warn!("Failed to remove cache file {:?}: {}", entry.file_path, e);
        }

        debug!("Removed cache entry: {}", key);
        Ok(true)
    }

    /// Clear all entries from the cache
    pub async fn clear(&self) -> Result<()> {
        let entries = {
            let mut state = self.state.write().await;
            let entries: Vec<_> = state.lru.iter().map(|(_, entry)| entry.file_path.clone()).collect();
            state.lru.clear();
            state.total_size = 0;
            entries
        };

        // Remove all cache files
        for file_path in entries {
            if let Err(e) = fs::remove_file(&file_path).await {
                warn!("Failed to remove cache file {:?}: {}", file_path, e);
            }
        }

        info!("Cleared all cache entries");
        Ok(())
    }

    /// Get the number of entries in the cache
    pub async fn len(&self) -> usize {
        let state = self.state.read().await;
        state.lru.len()
    }

    /// Check if the cache is empty
    pub async fn is_empty(&self) -> bool {
        let state = self.state.read().await;
        state.lru.is_empty()
    }

    /// Get the current total size of cached data
    pub async fn current_size(&self) -> u64 {
        let state = self.state.read().await;
        state.total_size
    }

    /// Get all keys currently in the cache
    pub async fn keys(&self) -> Vec<String> {
        let state = self.state.read().await;
        state.lru.iter().map(|(key, _)| key.clone()).collect()
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        let state = self.state.read().await;
        CacheStats {
            entries: state.lru.len(),
            max_entries: self.config.max_entries,
            total_size: state.total_size,
            cache_dir: self.config.cache_dir.clone(),
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub max_entries: usize,
    pub total_size: u64,
    pub cache_dir: PathBuf,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use futures_util::StreamExt;
    use tempfile::TempDir;

    use super::*;

    async fn create_test_cache() -> (DiskCache, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            max_entries: 3,
        };
        let cache = DiskCache::new(config).await.unwrap();
        (cache, temp_dir)
    }

    #[tokio::test]
    async fn test_put_and_get_mmap() {
        let (cache, _temp_dir) = create_test_cache().await;

        let key = "test_key".to_string();
        let data = b"test data";

        cache.put(key.clone(), data).await.unwrap();

        let mmap_view = cache.get_mmap(&key).await.unwrap().unwrap();
        assert_eq!(mmap_view.as_bytes(), data);
    }

    #[tokio::test]
    async fn test_put_stream() {
        let (cache, _temp_dir) = create_test_cache().await;

        let key = "test_key".to_string();
        let data = b"test data from stream";
        let cursor = Cursor::new(data);

        cache.put_stream(key.clone(), cursor).await.unwrap();

        let mmap_view = cache.get_mmap(&key).await.unwrap().unwrap();
        assert_eq!(mmap_view.as_bytes(), data);
    }

    #[tokio::test]
    async fn test_get_stream() {
        let (cache, _temp_dir) = create_test_cache().await;

        let key = "test_key".to_string();
        let data = b"test data for streaming";

        cache.put(key.clone(), data).await.unwrap();

        let mut stream = cache.get_stream(&key, 5).await.unwrap().unwrap();
        let mut result = Vec::new();

        while let Some(chunk) = stream.next().await {
            result.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_lru_eviction() {
        let (cache, _temp_dir) = create_test_cache().await;

        // Fill cache to capacity
        cache.put("key1".to_string(), b"data1").await.unwrap();
        cache.put("key2".to_string(), b"data2").await.unwrap();
        cache.put("key3".to_string(), b"data3").await.unwrap();

        assert_eq!(cache.len().await, 3);

        // Add one more to trigger eviction
        cache.put("key4".to_string(), b"data4").await.unwrap();

        assert_eq!(cache.len().await, 3);
        assert!(!cache.contains_key("key1").await); // Should be evicted
        assert!(cache.contains_key("key4").await); // Should be present
    }

    #[tokio::test]
    async fn test_cache_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig {
            cache_dir: temp_dir.path().to_path_buf(),
            max_entries: 5,
        };

        // Create cache and add some data
        {
            let cache = DiskCache::new(config.clone()).await.unwrap();
            cache
                .put("persistent_key".to_string(), b"persistent_data")
                .await
                .unwrap();
        }

        // Create new cache instance and verify data persists
        {
            let cache = DiskCache::new(config).await.unwrap();
            let mmap_view = cache.get_mmap("persistent_key").await.unwrap().unwrap();
            assert_eq!(mmap_view.as_bytes(), b"persistent_data");
        }
    }
}
