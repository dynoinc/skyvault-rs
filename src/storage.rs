use aws_sdk_s3::Client as S3Client;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {}

#[derive(Clone)]
pub struct ObjectStore {
    #[allow(dead_code)]
    client: S3Client,
}

impl ObjectStore {
    pub fn new(client: S3Client) -> Self {
        Self { client }
    }
}
