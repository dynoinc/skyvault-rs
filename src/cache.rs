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
    sync::RwLock,
    task::JoinHandle,
};
use tracing::{
    debug,
    info,
    warn,
};

/// On Unix-like systems, deleting a file only removes its directory entry; the
/// file's data remains accessible via open file handles or memory maps (mmap)
/// until all references are closed. This ensures that even after cache eviction
/// and file deletion, any reader holding an mmap can safely access the data
/// until it is dropped.
#[derive(Debug)]
pub struct OnDiskData {
    path: PathBuf,
    _file: File,
    mmap: Mmap,
}

#[derive(Debug, Clone)]
pub enum CacheData {
    InMemory(Bytes),
    OnDisk(Arc<OnDiskData>),
}

impl From<Bytes> for CacheData {
    fn from(data: Bytes) -> Self {
        CacheData::InMemory(data)
    }
}

impl From<&[u8]> for CacheData {
    fn from(data: &[u8]) -> Self {
        CacheData::from(Bytes::copy_from_slice(data))
    }
}

impl From<Vec<u8>> for CacheData {
    fn from(data: Vec<u8>) -> Self {
        CacheData::from(Bytes::from(data))
    }
}

impl<const N: usize> From<&[u8; N]> for CacheData {
    fn from(data: &[u8; N]) -> Self {
        CacheData::from(Bytes::copy_from_slice(data))
    }
}

impl CacheData {
    /// Get the data as a byte slice
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            CacheData::OnDisk(data) => data.mmap.as_ref(),
            CacheData::InMemory(data) => data,
        }
    }

    /// Get the length of the data
    pub fn len(&self) -> usize {
        match self {
            CacheData::OnDisk(data) => data.mmap.len(),
            CacheData::InMemory(data) => data.len(),
        }
    }

    /// Check if the data is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl AsRef<[u8]> for CacheData {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Cache entry metadata
#[derive(Debug, Clone)]
struct CacheEntry {
    data: CacheData,
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
    dir: PathBuf,
    state: Arc<RwLock<LruMap<String, CacheEntry, BySizeBytes>>>,
}

impl DiskCache {
    /// Calculate maximum size based on available disk space
    fn calculate_max_size(dir: &PathBuf, disk_usage_percentage: f64) -> Result<usize> {
        let available_bytes =
            fs2::available_space(dir).with_context(|| format!("Failed to get available space for {dir:?}"))?;

        let usable_bytes = (available_bytes as f64 * disk_usage_percentage / 100.0) as u64;
        let max_size = usable_bytes.max(1) as usize;

        info!(
            "Calculated max_size: {} bytes (available: {} bytes, usable: {} bytes)",
            max_size, available_bytes, usable_bytes
        );

        Ok(max_size)
    }

    /// Create a new disk cache with the given configuration
    pub async fn new(dir: PathBuf, disk_usage_percentage: f64) -> Result<Self> {
        // Ensure cache directory exists
        fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("Failed to create cache directory: {dir:?}"))?;

        let max_size_bytes = Self::calculate_max_size(&dir, disk_usage_percentage)?;

        let limiter = BySizeBytes::new(max_size_bytes);
        let state = Arc::new(RwLock::new(LruMap::new(limiter)));

        let cache = Self { dir, state };

        // Load existing cache entries on startup
        cache.load_existing_entries().await?;

        Ok(cache)
    }

    #[cfg(test)]
    pub async fn new_with_max_size(dir: PathBuf, max_size_bytes: usize) -> Result<Self> {
        fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("Failed to create cache directory: {dir:?}"))?;

        let limiter = BySizeBytes::new(max_size_bytes);
        let state = Arc::new(RwLock::new(LruMap::new(limiter)));

        let cache = Self { dir, state };

        // Load existing cache entries on startup
        cache.load_existing_entries().await?;

        Ok(cache)
    }

    /// Load existing cache entries from disk
    async fn load_existing_entries(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.dir)
            .await
            .with_context(|| format!("Failed to read cache directory: {:?}", self.dir))?;

        let mut loaded_entries = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            if let Some(key) = path.file_name().and_then(|n| n.to_str()).map(|s| s.to_string()) {
                let metadata = entry.metadata().await?;
                let size = metadata.len();
                let modified = metadata.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);

                // Open file and create mmap for loading existing cache entries
                let file =
                    std::fs::File::open(&path).with_context(|| format!("Failed to open cache file: {path:?}"))?;
                let mmap = unsafe {
                    MmapOptions::new()
                        .map(&file)
                        .with_context(|| format!("Failed to mmap cache file: {path:?}"))?
                };

                let cache_entry = CacheEntry {
                    data: CacheData::OnDisk(Arc::new(OnDiskData {
                        path,
                        _file: file,
                        mmap,
                    })),
                    size,
                };

                loaded_entries.push((key.to_string(), cache_entry, modified));
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
        self.dir.join(key)
    }

    async fn insert_and_trim(&self, key: String, entry: CacheEntry) -> Result<()> {
        let mut state = self.state.write().await;
        state.insert(key.clone(), entry);

        while state.limiter().is_over_the_limit(state.len()) {
            if let Some((_key, value)) = state.pop_oldest() {
                if let CacheData::OnDisk(data) = value.data {
                    let path = data.path.clone();
                    tokio::spawn(async move {
                        if let Err(e) = fs::remove_file(&path).await {
                            warn!("Failed to delete evicted cache file {:?}: {}", path, e);
                        } else {
                            debug!("Deleted evicted cache file: {:?}", path);
                        }
                    });
                }
            }
        }

        Ok(())
    }

    pub async fn put(&self, key: String, data: CacheData) -> Result<JoinHandle<()>> {
        let size = data.len() as u64;
        let bytes_data = Bytes::copy_from_slice(data.as_bytes());

        let entry = CacheEntry {
            data: CacheData::InMemory(bytes_data.clone()),
            size,
        };

        self.insert_and_trim(key.clone(), entry).await?;
        debug!("Cached key '{}' with size {} bytes (in-memory)", key, size);

        // Spawn background task to write to disk
        let file_path = self.get_file_path(&key);
        let state = self.state.clone();
        let key_clone = key.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = Self::write_to_disk(&file_path, &bytes_data).await {
                warn!("Failed to write cache entry to disk: {}", e);
                return;
            }

            // Update cache entry to point to disk
            let mut state_guard = state.write().await;
            if let Some(entry) = state_guard.get(&key_clone) {
                // Open file and create mmap for the disk-based cache entry
                let file_result = std::fs::File::open(&file_path);
                match file_result {
                    Ok(file) => {
                        let mmap_result = unsafe {
                            MmapOptions::new()
                                .map(&file)
                                .with_context(|| format!("Failed to mmap cache file: {file_path:?}"))
                        };
                        match mmap_result {
                            Ok(mmap) => {
                                let updated_entry = CacheEntry {
                                    data: CacheData::OnDisk(Arc::new(OnDiskData {
                                        path: file_path.clone(),
                                        _file: file,
                                        mmap,
                                    })),
                                    size: entry.size,
                                };
                                state_guard.insert(key_clone.clone(), updated_entry);
                                debug!("Moved cache entry '{}' to disk", key_clone);
                            },
                            Err(e) => {
                                warn!("Failed to mmap cache file {:?}: {}", file_path, e);
                            },
                        }
                    },
                    Err(e) => {
                        warn!("Failed to open cache file {:?}: {}", file_path, e);
                    },
                }
            }
        });

        Ok(handle)
    }

    async fn write_to_disk(file_path: &PathBuf, data: &Bytes) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_path)
            .with_context(|| format!("Failed to create cache file: {file_path:?}"))?;

        file.write_all(data)
            .with_context(|| format!("Failed to write data to cache file: {file_path:?}"))?;

        file.sync_all()
            .with_context(|| format!("Failed to sync cache file: {file_path:?}"))?;

        Ok(())
    }

    /// Get a memory-mapped view of cached data
    pub async fn get_mmap(&self, key: &str) -> Option<CacheData> {
        let mut state = self.state.write().await;
        match state.get(key) {
            Some(entry) => Some(entry.data.clone()),
            None => None,
        }
    }

    /// Check if a key exists in the cache
    #[cfg(test)]
    pub async fn contains_key(&self, key: &str) -> bool {
        let state = self.state.read().await;
        state.peek(key).is_some()
    }

    #[cfg(test)]
    pub async fn stats(&self) -> CacheStats {
        let state = self.state.read().await;
        CacheStats {
            max_size_bytes: state.limiter().max_size,
            current_size: state.limiter().current_size,
        }
    }
}

/// Cache statistics
#[cfg(test)]
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub max_size_bytes: usize,
    pub current_size: usize,
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    async fn create_test_cache() -> (DiskCache, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let cache = DiskCache::new_with_max_size(temp_dir.path().to_path_buf(), 1024)
            .await
            .unwrap();
        (cache, temp_dir)
    }

    #[tokio::test]
    async fn test_put_and_get_mmap() {
        let (cache, _temp_dir) = create_test_cache().await;

        let key = "test_key".to_string();
        let data = b"Hello, World!";

        cache.put(key.clone(), data.into()).await.unwrap().await.unwrap();

        let mmap_view = cache.get_mmap(&key).await.unwrap();
        assert_eq!(mmap_view.as_bytes(), data);
        assert_eq!(mmap_view.len(), data.len());
        assert!(!mmap_view.is_empty());
    }

    #[tokio::test]
    async fn test_size_based_eviction() {
        let (cache, _temp_dir) = create_test_cache().await;

        // Add data that exceeds the 1KB limit
        cache
            .put("key1".to_string(), vec![0u8; 400].into())
            .await
            .unwrap()
            .await
            .unwrap(); // 400 bytes
        cache
            .put("key2".to_string(), vec![1u8; 400].into())
            .await
            .unwrap()
            .await
            .unwrap(); // 400 bytes
        cache
            .put("key3".to_string(), vec![2u8; 400].into())
            .await
            .unwrap()
            .await
            .unwrap(); // 400 bytes - should evict key1

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
        let cache_dir = temp_dir.path().to_path_buf();
        let cache = DiskCache::new_with_max_size(cache_dir.clone(), 2048).await.unwrap();

        // Create cache and add some data
        cache
            .put("persistent_key".to_string(), b"persistent_data".into())
            .await
            .unwrap()
            .await
            .unwrap();

        // Drop the first cache instance to ensure it's closed.
        drop(cache);

        // Create new cache instance and verify data persists
        let cache = DiskCache::new_with_max_size(cache_dir, 2048).await.unwrap();
        let mmap_view = cache.get_mmap("persistent_key").await.unwrap();
        assert_eq!(mmap_view.as_bytes(), b"persistent_data");
    }

    #[tokio::test]
    async fn test_automatic_size_calculation() {
        let temp_dir = TempDir::new().unwrap();
        let cache = DiskCache::new_with_max_size(temp_dir.path().to_path_buf(), 2048)
            .await
            .unwrap();
        let stats = cache.stats().await;

        // Should have calculated a reasonable max size based on available disk space
        assert!(stats.max_size_bytes > 0);
        println!("Calculated max size: {} bytes", stats.max_size_bytes);
    }
}
