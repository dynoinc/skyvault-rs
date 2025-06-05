use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        RwLock,
    },
    time::Instant,
};

use async_trait::async_trait;
use aws_sdk_s3::{
    Client as S3Client,
    operation::{
        create_bucket::CreateBucketError,
        get_object::GetObjectError,
        put_object::PutObjectError,
    },
    primitives::{
        ByteStream,
        ByteStreamError,
    },
};
use aws_smithy_runtime_api::client::{
    orchestrator::HttpResponse,
    result::SdkError,
};
use bytes::Bytes;
use metrics::{
    counter,
    histogram,
};
use thiserror::Error;
use tokio::sync::broadcast;

use crate::{
    cache::DiskCache,
    metadata::SnapshotID,
    runs::{
        RunId,
        RunView,
    },
};

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Create bucket error: {0}")]
    CreateBucketError(#[from] Box<SdkError<CreateBucketError, HttpResponse>>),

    #[error("Put object error: {0}")]
    PutObjectError(#[from] Box<SdkError<PutObjectError, HttpResponse>>),

    #[error("Get object error: {0}")]
    GetObjectError(#[from] Box<SdkError<GetObjectError, HttpResponse>>),

    #[error("Get object failed to read body: {0}")]
    GetObjectReadBodyError(#[from] ByteStreamError),

    #[error("Object not found: {0}")]
    NotFound(String),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

impl From<SdkError<CreateBucketError, HttpResponse>> for StorageError {
    fn from(err: SdkError<CreateBucketError, HttpResponse>) -> Self {
        StorageError::CreateBucketError(Box::new(err))
    }
}

impl From<SdkError<PutObjectError, HttpResponse>> for StorageError {
    fn from(err: SdkError<PutObjectError, HttpResponse>) -> Self {
        StorageError::PutObjectError(Box::new(err))
    }
}

impl From<SdkError<GetObjectError, HttpResponse>> for StorageError {
    fn from(err: SdkError<GetObjectError, HttpResponse>) -> Self {
        StorageError::GetObjectError(Box::new(err))
    }
}

#[async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait ObjectStoreTrait: Send + Sync + 'static {
    async fn put_run(&self, run_id: RunId, data: Bytes) -> Result<(), StorageError>;
    async fn get_run(&self, run_id: RunId) -> Result<ByteStream, StorageError>;

    async fn put_snapshot(&self, snapshot_id: SnapshotID, data: Bytes) -> Result<(), StorageError>;
    async fn get_snapshot(&self, snapshot_id: SnapshotID) -> Result<Bytes, StorageError>;
}

pub type ObjectStore = Arc<dyn ObjectStoreTrait>;

#[derive(Clone)]
pub struct S3ObjectStore {
    client: S3Client,
    bucket_name: String,
}

impl S3ObjectStore {
    pub async fn new(s3_config: aws_sdk_s3::config::Config, bucket_name: &str) -> Result<Self, StorageError> {
        let client = S3Client::from_conf(s3_config);

        match Self::create_bucket(&client, bucket_name).await {
            Ok(_) => {},
            Err(SdkError::ServiceError(err))
                if err.err().is_bucket_already_exists() || err.err().is_bucket_already_owned_by_you() =>
            {
                tracing::info!("Bucket already exists: {}", bucket_name);
            },
            Err(err) => return Err(StorageError::CreateBucketError(Box::new(err))),
        }

        Ok(Self {
            client,
            bucket_name: bucket_name.to_string(),
        })
    }

    async fn create_bucket(
        client: &S3Client,
        bucket_name: &str,
    ) -> Result<(), SdkError<CreateBucketError, HttpResponse>> {
        S3ObjectStore::record_s3_metrics("create_bucket", || async {
            client.create_bucket().bucket(bucket_name).send().await.map(|_| ())
        })
        .await
    }

    async fn record_s3_metrics<T, E, F, Fut>(method: &'static str, operation: F) -> Result<T, SdkError<E, HttpResponse>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, SdkError<E, HttpResponse>>>,
        E: std::fmt::Debug + aws_smithy_types::error::metadata::ProvideErrorMetadata,
    {
        let start = Instant::now();
        let result = operation().await;
        let duration = start.elapsed();

        let status = match result.as_ref() {
            Ok(_) => "success".to_string(),
            Err(err) => match err {
                SdkError::ConstructionFailure(_) => "construction_failure".to_string(),
                SdkError::TimeoutError(_) => "timeout".to_string(),
                SdkError::DispatchFailure(_) => "dispatch_failure".to_string(),
                SdkError::ResponseError(_) => "response_error".to_string(),
                SdkError::ServiceError(service_err) => {
                    service_err.err().message().unwrap_or("unknown_error").to_string()
                },
                _ => "unknown_error".to_string(),
            },
        };

        let req_counter = counter!(
            "skyvault/s3/requests_total",
            "operation" => method,
            "status" => status.clone()
        );
        let duration_hist = histogram!(
            "skyvault/s3/request_duration_seconds",
            "operation" => method,
            "status" => status
        );

        req_counter.increment(1);
        duration_hist.record(duration.as_secs_f64());

        result
    }
}

#[async_trait]
impl ObjectStoreTrait for S3ObjectStore {
    async fn put_snapshot(&self, snapshot_id: SnapshotID, data: Bytes) -> Result<(), StorageError> {
        let byte_stream = ByteStream::from(data);

        S3ObjectStore::record_s3_metrics("put_object", || async {
            self.client
                .put_object()
                .bucket(self.bucket_name.clone())
                .key(format!("snapshots/{snapshot_id}"))
                .body(byte_stream)
                .send()
                .await
                .map(|_| ())
        })
        .await
        .map_err(|err| StorageError::PutObjectError(Box::new(err)))
    }

    async fn put_run(&self, run_id: RunId, data: Bytes) -> Result<(), StorageError> {
        let byte_stream = ByteStream::from(data);

        S3ObjectStore::record_s3_metrics("put_object", || async {
            self.client
                .put_object()
                .bucket(self.bucket_name.clone())
                .key(format!("runs/{run_id}"))
                .body(byte_stream)
                .if_none_match("*".to_string())
                .send()
                .await
                .map(|_| ())
        })
        .await
        .map_err(|err| StorageError::PutObjectError(Box::new(err)))
    }

    async fn get_snapshot(&self, snapshot_id: SnapshotID) -> Result<Bytes, StorageError> {
        let key = format!("snapshots/{snapshot_id}");

        let response = S3ObjectStore::record_s3_metrics("get_object", || async {
            self.client
                .get_object()
                .bucket(self.bucket_name.clone())
                .key(key.clone())
                .send()
                .await
        })
        .await
        .map_err(|err| {
            if let SdkError::ServiceError(ref inner) = err {
                if let GetObjectError::NoSuchKey(_) = inner.err() {
                    return StorageError::NotFound(key.clone());
                }
            }
            StorageError::GetObjectError(Box::new(err))
        })?;

        let bytes = response.body.collect().await?.into_bytes();
        Ok(bytes)
    }

    async fn get_run(&self, run_id: RunId) -> Result<ByteStream, StorageError> {
        let key = format!("runs/{run_id}");

        let response = S3ObjectStore::record_s3_metrics("get_object", || async {
            self.client
                .get_object()
                .bucket(self.bucket_name.clone())
                .key(key.clone())
                .send()
                .await
        })
        .await
        .map_err(|err| {
            if let SdkError::ServiceError(ref inner) = err {
                if let GetObjectError::NoSuchKey(_) = inner.err() {
                    return StorageError::NotFound(key.clone());
                }
            }
            StorageError::GetObjectError(Box::new(err))
        })?;

        Ok(response.body)
    }
}

#[derive(Error, Debug, Clone)]
pub enum StorageCacheError {
    #[error("Storage error: {0}")]
    StorageError(#[from] Arc<StorageError>),

    #[error("Storage cache byte stream error: {0}")]
    StorageCacheByteStreamError(#[from] Arc<ByteStreamError>),

    #[error("Storage cache error: {0}")]
    StorageCacheBroadcastError(#[from] tokio::sync::broadcast::error::RecvError),

    #[error("Storage disk cache error: {0}")]
    StorageCacheMmapError(#[from] Arc<anyhow::Error>),
}

/// Type alias for the inflight request broadcast sender
type InflightSender = broadcast::Sender<Result<Bytes, StorageCacheError>>;

/// A simple cache for the object store that caches run data in memory
pub struct StorageCache {
    /// The underlying storage system
    storage: ObjectStore,

    /// The cache of run data
    cache: DiskCache,

    /// Map of inflight requests to prevent duplicate fetches
    /// Each entry contains a broadcast sender that will notify all waiters when
    /// the request completes (either success or failure)
    inflight: Arc<RwLock<HashMap<RunId, InflightSender>>>,
}

impl StorageCache {
    /// Create a new StorageCache
    pub async fn new(
        storage: ObjectStore,
        dir: PathBuf,
        disk_usage_percentage: f64,
    ) -> Result<Self, StorageCacheError> {
        let cache = DiskCache::new(dir, disk_usage_percentage)
            .await
            .map_err(|err| StorageCacheError::StorageCacheMmapError(Arc::new(err)))?;

        Ok(Self {
            storage,
            cache,
            inflight: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Get run data from cache or storage if not cached
    pub async fn get_run(&self, run_id: RunId) -> Result<RunView, StorageCacheError> {
        // First check if the run is in the cache
        {
            let cache = self
                .cache
                .get_mmap(run_id.as_ref())
                .await
                .map_err(|err| StorageCacheError::StorageCacheMmapError(Arc::new(err)))?;
            if let Some(run_data) = cache {
                return Ok(RunView::Mmap(run_data));
            }
        }

        // Check if there's already an inflight request for this run
        let receiver_opt = {
            let mut inflight = self.inflight.write().unwrap();

            if let Some(sender) = inflight.get(&run_id) {
                // There's already an inflight request, subscribe to it
                Some(sender.subscribe())
            } else {
                // No inflight request, create a new broadcast channel
                let (sender, _receiver) = broadcast::channel(1);
                inflight.insert(run_id.clone(), sender);
                None
            }
        };

        // If we have a receiver, wait for the inflight request to complete
        if let Some(mut receiver) = receiver_opt {
            return receiver.recv().await?.map(RunView::Bytes);
        }

        // We're the first request for this run, fetch from storage
        let result = async {
            let run_stream = self
                .storage
                .get_run(run_id.clone())
                .await
                .map_err(|err| StorageCacheError::StorageError(Arc::new(err)))?;
            let collected = run_stream
                .collect()
                .await
                .map_err(|err| StorageCacheError::StorageCacheByteStreamError(Arc::new(err)))?;
            Ok(bytes::Bytes::from(collected.to_vec()))
        }
        .await;

        if let Ok(data) = result.as_ref() {
            self.cache
                .put(run_id.to_string(), data)
                .await
                .map_err(|err| StorageCacheError::StorageCacheMmapError(Arc::new(err)))?;
        }

        // Notify all waiters and remove from inflight map
        {
            let mut inflight = self.inflight.write().unwrap();
            let sender = inflight.remove(&run_id).expect("Sender should exist");
            let _ = sender.send(result.clone());
        }

        result.map(RunView::Bytes)
    }
}
