use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Create bucket error: {0}")]
    CreateBucketError(#[from] SdkError<CreateBucketError, HttpResponse>),
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
    pub async fn put(&self, key: &str, value: Vec<u8>) -> Result<(), StorageError> {
        let _ = self
            .client
            .put_object()
            .bucket(self.bucket_name.clone())
            .key(key)
            .body(value.into())
            .send()
            .await;

        Ok(())
    }
}
