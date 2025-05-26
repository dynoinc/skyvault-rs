use std::{
    fs::{
        File,
        OpenOptions,
    },
    io::Write,
    path::PathBuf,
    sync::Arc,
};

use anyhow::{
    Context,
    Result,
};
use bytes::Bytes;
use memmap2::{
    Mmap,
    MmapOptions,
};
use schnellru::{
    Limiter,
    LruMap,
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
    /// Optional maximum size in bytes (if None, calculated from available disk
    /// space)
    pub max_size_bytes: Option<usize>,
    /// Percentage of available disk space to use for cache (default: 80%)
    pub disk_usage_percentage: f64,
}

impl CacheConfig {
    /// Create a new cache configuration with automatic disk space calculation
    pub fn new(cache_dir: PathBuf) -> Self {
        Self {
            cache_dir,
            max_size_bytes: None,
            disk_usage_percentage: 80.0,
        }
    }

    /// Create a new cache configuration with a fixed maximum size
    pub fn with_max_size(cache_dir: PathBuf, max_size_bytes: usize) -> Self {
        Self {
            cache_dir,
            max_size_bytes: Some(max_size_bytes),
            disk_usage_percentage: 80.0,
        }
    }
}

/// A memory-mapped view of cached data
#[derive(Debug)]
pub struct MmapView {
    _file: File,
    mmap: Mmap,
}

impl MmapView {
    /// Get the data as a byte slice
    pub fn as_bytes(&self) -> &[u8] {
        &self.mmap
    }

    /// Get the length of the data
    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    /// Check if the data is empty
    pub fn is_empty(&self) -> bool {
        self.mmap.is_empty()
    }

    /// Convert to owned bytes
    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.mmap)
    }
}

/// Cache entry metadata
#[derive(Debug, Clone)]
struct CacheEntry {
    file_path: PathBuf,
    size: u64,
}

/// Custom limiter that limits cache by total size in bytes
#[derive(Debug)]
struct BySizeBytes {
    max_size: usize,
    current_size: usize,
}

impl BySizeBytes {
    fn new(max_size: usize) -> Self {
        Self {
            max_size,
            current_size: 0,
        }
    }
}

impl Limiter<String, CacheEntry> for BySizeBytes {
    type KeyToInsert<'a> = String;
    type LinkType = u32;

    fn is_over_the_limit(&self, _length: usize) -> bool {
        self.current_size > self.max_size
    }

    fn on_insert(
        &mut self,
        _length: usize,
        key: Self::KeyToInsert<'_>,
        value: CacheEntry,
    ) -> Option<(String, CacheEntry)> {
        self.current_size += value.size as usize;
        Some((key.to_string(), value))
    }

    fn on_replace(
        &mut self,
        _length: usize,
        _key: &mut String,
        _new_key: Self::KeyToInsert<'_>,
        new_value: &mut CacheEntry,
        old_value: &mut CacheEntry,
    ) -> bool {
        self.current_size = self.current_size.saturating_sub(old_value.size as usize);
        self.current_size += new_value.size as usize;
        true
    }

    fn on_removed(&mut self, _key: &mut String, value: &mut CacheEntry) {
        self.current_size = self.current_size.saturating_sub(value.size as usize);
    }

    fn on_cleared(&mut self) {
        self.current_size = 0;
    }

    fn on_grow(&mut self, _new_memory_usage: usize) -> bool {
        true
    }
}

/// A disk-backed cache with LRU eviction and mmap support
pub struct DiskCache {
    config: CacheConfig,
    /// Cache state protected by a single lock
    state: Arc<RwLock<LruMap<String, CacheEntry, BySizeBytes>>>,
    /// Actual maximum size in bytes (calculated or configured)
    max_size_bytes: usize,
}

impl DiskCache {
    /// Calculate maximum size based on available disk space
    fn calculate_max_size(config: &CacheConfig) -> Result<usize> {
        let available_bytes = fs2::available_space(&config.cache_dir)
            .with_context(|| format!("Failed to get available space for {:?}", config.cache_dir))?;

        let usable_bytes = (available_bytes as f64 * config.disk_usage_percentage / 100.0) as u64;
        let max_size = usable_bytes.max(1) as usize;

        info!(
            "Calculated max_size: {} bytes (available: {} bytes, usable: {} bytes)",
            max_size, available_bytes, usable_bytes
        );

        Ok(max_size)
    }

    /// Create a new disk cache with the given configuration
    pub async fn new(config: CacheConfig) -> Result<Self> {
        // Ensure cache directory exists
        fs::create_dir_all(&config.cache_dir).await.with_context(|| {
            format!(
                "Failed to create cache directory: {cache_dir:?}",
                cache_dir = config.cache_dir
            )
        })?;

        let max_size_bytes = match config.max_size_bytes {
            Some(size) => size,
            None => Self::calculate_max_size(&config)?,
        };

        let limiter = BySizeBytes::new(max_size_bytes);
        let state = Arc::new(RwLock::new(LruMap::new(limiter)));

        let cache = Self {
            config,
            state,
            max_size_bytes,
        };

        // Load existing cache entries on startup
        cache.load_existing_entries().await?;

        Ok(cache)
    }

    /// Load existing cache entries from disk
    async fn load_existing_entries(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.config.cache_dir)
            .await
            .with_context(|| format!("Failed to read cache directory: {:?}", self.config.cache_dir))?;

        let mut loaded_entries = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                if let Some(key) = path.file_name().and_then(|n| n.to_str()) {
                    let metadata = entry.metadata().await?;
                    let size = metadata.len();
                    let modified = metadata.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);

                    let cache_entry = CacheEntry {
                        file_path: path.clone(),
                        size,
                    };

                    loaded_entries.push((key.to_string(), cache_entry, modified));
                }
            }
        }

        // Sort by modification time to maintain LRU order (oldest first)
        loaded_entries.sort_by_key(|(_, _, modified)| *modified);

        let mut state = self.state.write().await;

        for (key, entry, _) in loaded_entries {
            state.insert(key, entry);
        }

        let current_size = state.limiter().current_size;
        let entries_count = state.len();

        info!(
            "Loaded {} cache entries, total size: {} bytes",
            entries_count, current_size
        );
        Ok(())
    }

    /// Get the file path for a cache key
    fn get_file_path(&self, key: &str) -> PathBuf {
        self.config.cache_dir.join(key)
    }

    async fn insert_and_trim(&self, key: String, entry: CacheEntry) -> Result<()> {
        let files_to_remove = {
            let mut state = self.state.write().await;
            state.insert(key.clone(), entry);

            let mut files_to_remove = Vec::new();
            while state.limiter().is_over_the_limit(state.len()) {
                if let Some((_evicted_key, evicted_entry)) = state.pop_oldest() {
                    files_to_remove.push(evicted_entry.file_path.clone());
                } else {
                    break;
                }
            }

            files_to_remove
        };

        for file_path in files_to_remove {
            if let Err(e) = fs::remove_file(&file_path).await {
                warn!("Failed to remove evicted cache file {:?}: {}", file_path, e);
            } else {
                debug!("Evicted cache entry: {:?}", file_path);
            }
        }
        
        Ok(())
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

        self.insert_and_trim(key.clone(), entry).await?;
        debug!("Cached key '{}' with size {} bytes", key, size);
        Ok(())
    }

    pub async fn put(&self, key: String, data: &[u8]) -> Result<()> {
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

        self.insert_and_trim(key.clone(), entry).await?;
        debug!("Cached key '{}' with size {} bytes", key, size);
        Ok(())
    }

    /// Get a memory-mapped view of cached data
    pub async fn get_mmap(&self, key: &str) -> Result<Option<MmapView>> {
        let file_path = {
            let mut state = self.state.write().await;
            if let Some(entry) = state.get(key) {
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

    /// Check if a key exists in the cache
    pub async fn contains_key(&self, key: &str) -> bool {
        let state = self.state.read().await;
        state.peek(key).is_some()
    }

    /// Remove a key from the cache
    pub async fn remove(&self, key: &str) -> Result<bool> {
        let entry = {
            let mut state = self.state.write().await;
            state.remove(key)
        };

        if let Some(entry) = entry {
            if let Err(e) = fs::remove_file(&entry.file_path).await {
                warn!("Failed to remove cache file {:?}: {}", entry.file_path, e);
            }

            debug!("Removed cache entry: {}", key);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Clear all entries from the cache
    pub async fn clear(&self) -> Result<()> {
        let entries = {
            let mut state = self.state.write().await;
            let entries: Vec<_> = state.iter().map(|(_, entry)| entry.file_path.clone()).collect();
            state.clear();
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
        state.len()
    }

    /// Check if the cache is empty
    pub async fn is_empty(&self) -> bool {
        let state = self.state.read().await;
        state.is_empty()
    }

    /// Get the current total size of cached data
    pub async fn current_size(&self) -> usize {
        let state = self.state.read().await;
        state.limiter().current_size
    }

    /// Get all keys currently in the cache
    pub async fn keys(&self) -> Vec<String> {
        let state = self.state.read().await;
        state.iter().map(|(key, _)| key.clone()).collect()
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        let state = self.state.read().await;
        CacheStats {
            entries: state.len(),
            max_size_bytes: self.max_size_bytes,
            current_size: state.limiter().current_size,
            cache_dir: self.config.cache_dir.clone(),
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub max_size_bytes: usize,
    pub current_size: usize,
    pub cache_dir: PathBuf,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use tempfile::TempDir;

    use super::*;

    async fn create_test_cache() -> (DiskCache, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig::with_max_size(temp_dir.path().to_path_buf(), 1024); // 1KB limit
        let cache = DiskCache::new(config).await.unwrap();
        (cache, temp_dir)
    }

    #[tokio::test]
    async fn test_put_and_get_mmap() {
        let (cache, _temp_dir) = create_test_cache().await;

        let key = "test_key".to_string();
        let data = b"Hello, World!";

        cache.put(key.clone(), data).await.unwrap();

        let mmap_view = cache.get_mmap(&key).await.unwrap().unwrap();
        assert_eq!(mmap_view.as_bytes(), data);
        assert_eq!(mmap_view.len(), data.len());
        assert!(!mmap_view.is_empty());
    }

    #[tokio::test]
    async fn test_put_stream() {
        let (cache, _temp_dir) = create_test_cache().await;

        let key = "stream_key".to_string();
        let data = b"Stream data test";
        let cursor = Cursor::new(data);

        cache.put_stream(key.clone(), cursor).await.unwrap();

        let mmap_view = cache.get_mmap(&key).await.unwrap().unwrap();
        assert_eq!(mmap_view.as_bytes(), data);
    }

    #[tokio::test]
    async fn test_size_based_eviction() {
        let (cache, _temp_dir) = create_test_cache().await;

        // Add data that exceeds the 1KB limit
        cache.put("key1".to_string(), &vec![0u8; 400]).await.unwrap(); // 400 bytes
        cache.put("key2".to_string(), &vec![1u8; 400]).await.unwrap(); // 400 bytes
        cache.put("key3".to_string(), &vec![2u8; 400]).await.unwrap(); // 400 bytes - should evict key1

        // key1 should be evicted due to size limit
        assert!(!cache.contains_key("key1").await);
        assert!(cache.contains_key("key2").await);
        assert!(cache.contains_key("key3").await);

        let stats = cache.stats().await;
        assert!(stats.current_size <= stats.max_size_bytes);
    }

    #[tokio::test]
    async fn test_cache_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig::with_max_size(temp_dir.path().to_path_buf(), 2048);

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

    #[tokio::test]
    async fn test_automatic_size_calculation() {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig::new(temp_dir.path().to_path_buf());

        let cache = DiskCache::new(config).await.unwrap();
        let stats = cache.stats().await;

        // Should have calculated a reasonable max size based on available disk space
        assert!(stats.max_size_bytes > 0);
        println!("Calculated max size: {} bytes", stats.max_size_bytes);
    }
}
