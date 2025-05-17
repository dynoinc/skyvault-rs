use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::{ByteStream, ByteStreamError};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use bytes::Bytes;
use thiserror::Error;

use crate::metadata::SnapshotID;
use crate::runs::RunId;

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
    pub async fn new(client: S3Client, bucket_name: &str) -> Result<Self, StorageError> {
        // Create the bucket if it doesn't exist
        // Ignore the error if the bucket already exists
        match client.create_bucket().bucket(bucket_name).send().await {
            Ok(_) => {},
            Err(SdkError::ServiceError(err))
                if err.err().is_bucket_already_exists()
                    || err.err().is_bucket_already_owned_by_you() => {},
            Err(err) => return Err(StorageError::CreateBucketError(Box::new(err))),
        }

        Ok(Self {
            client,
            bucket_name: bucket_name.to_string(),
        })
    }
}

#[async_trait]
impl ObjectStoreTrait for S3ObjectStore {
    async fn put_snapshot(&self, snapshot_id: SnapshotID, data: Bytes) -> Result<(), StorageError> {
        let byte_stream = ByteStream::from(data);

        match self
            .client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(format!("snapshots/{}", snapshot_id))
            .body(byte_stream)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(StorageError::PutObjectError(Box::new(err))),
        }
    }

    async fn put_run(&self, run_id: RunId, data: Bytes) -> Result<(), StorageError> {
        let byte_stream = ByteStream::from(data);

        match self
            .client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(format!("runs/{}", run_id))
            .body(byte_stream)
            .if_none_match("*".to_string())
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(StorageError::PutObjectError(Box::new(err))),
        }
    }

    async fn get_snapshot(&self, snapshot_id: SnapshotID) -> Result<Bytes, StorageError> {
        let key = format!("snapshots/{}", snapshot_id);
        let response = match self
            .client
            .get_object()
            .bucket(self.bucket_name.clone())
            .key(key.clone())
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                if let SdkError::ServiceError(ref inner) = err {
                    if let GetObjectError::NoSuchKey(_) = inner.err() {
                        return Err(StorageError::NotFound(key));
                    }
                }
                return Err(StorageError::GetObjectError(Box::new(err)));
            },
        };

        let bytes = response.body.collect().await?.into_bytes();
        Ok(bytes)
    }

    async fn get_run(&self, run_id: RunId) -> Result<ByteStream, StorageError> {
        let key = format!("runs/{}", run_id);
        let response = match self
            .client
            .get_object()
            .bucket(self.bucket_name.clone())
            .key(key.clone())
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                if let SdkError::ServiceError(ref inner) = err {
                    if let GetObjectError::NoSuchKey(_) = inner.err() {
                        return Err(StorageError::NotFound(key));
                    }
                }
                return Err(StorageError::GetObjectError(Box::new(err)));
            },
        };

        Ok(response.body)
    }
}

/// A simple cache for the object store that caches run data in memory
pub struct StorageCache {
    /// The underlying storage system
    storage: ObjectStore,

    /// In-memory cache of run data
    cache: Arc<RwLock<HashMap<RunId, Bytes>>>,
}

impl StorageCache {
    /// Create a new StorageCache
    pub fn new(storage: ObjectStore) -> Self {
        Self {
            storage,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get run data from cache or storage if not cached
    pub async fn get_run(&self, run_id: RunId) -> Result<Bytes, StorageError> {
        // First check if the run is in the cache
        {
            let cache = self.cache.read().unwrap();
            if let Some(run_data) = cache.get(&run_id) {
                return Ok(run_data.clone());
            }
        }

        // Not in cache, fetch from storage
        let run_stream = self.storage.get_run(run_id.clone()).await?;
        let run_data = bytes::Bytes::from(run_stream.collect().await?.to_vec());

        // Update cache
        {
            let mut cache = self.cache.write().unwrap();
            cache.insert(run_id, run_data.clone());
        }

        Ok(run_data)
    }
}
