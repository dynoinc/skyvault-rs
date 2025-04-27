use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::ByteStreamError;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Create bucket error: {0}")]
    CreateBucketError(#[from] SdkError<CreateBucketError, HttpResponse>),

    #[error("Put object error: {0}")]
    PutObjectError(#[from] SdkError<PutObjectError, HttpResponse>),

    #[error("Get object error: {0}")]
    GetObjectError(#[from] SdkError<GetObjectError, HttpResponse>),

    #[error("Get object failed to read body: {0}")]
    GetObjectReadBodyError(#[from] ByteStreamError),

    #[error("Object not found: {0}")]
    NotFound(String),
}

#[derive(Clone)]
pub struct ObjectStore {
    client: S3Client,
    bucket_name: String,
}

impl ObjectStore {
    pub async fn new(client: S3Client, bucket_name: &str) -> Result<Self, StorageError> {
        // Create the bucket if it doesn't exist
        // Ignore the error if the bucket already exists
        match client.create_bucket().bucket(bucket_name).send().await {
            Ok(_) => {},
            Err(SdkError::ServiceError(err))
                if err.err().is_bucket_already_exists()
                    || err.err().is_bucket_already_owned_by_you() => {},
            Err(err) => return Err(StorageError::CreateBucketError(err)),
        }

        Ok(Self {
            client,
            bucket_name: bucket_name.to_string(),
        })
    }
}

impl ObjectStore {
    pub async fn put_run(&self, run_id: &str, value: Vec<u8>) -> Result<(), StorageError> {
        match self
            .client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(format!("runs/{}", run_id))
            .body(value.into())
            .if_none_match("*".to_string())
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(StorageError::PutObjectError(err)),
        }
    }

    pub async fn get_run(&self, run_id: &str) -> Result<Vec<u8>, StorageError> {
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
                return Err(StorageError::GetObjectError(err));
            },
        };

        let data = response.body.collect().await?.to_vec();

        Ok(data)
    }
}

/// A simple cache for the object store that caches run data in memory
pub struct StorageCache {
    /// The underlying storage system
    storage: ObjectStore,

    /// In-memory cache of run data
    cache: Arc<RwLock<HashMap<String, Arc<Vec<u8>>>>>,
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
    pub async fn get_run(&self, run_id: &str) -> Result<Arc<Vec<u8>>, StorageError> {
        // First check if the run is in the cache
        {
            let cache = self.cache.read().unwrap();
            if let Some(run_data) = cache.get(run_id) {
                return Ok(run_data.clone());
            }
        }

        // Not in cache, fetch from storage
        let run_data = Arc::new(self.storage.get_run(run_id).await?);

        // Update cache
        {
            let mut cache = self.cache.write().unwrap();
            cache.insert(run_id.to_string(), run_data.clone());
        }

        Ok(run_data)
    }
}
