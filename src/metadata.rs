use aws_sdk_dynamodb::Client as DynamoDbClient;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MetadataError {}

#[derive(Clone)]
pub struct MetadataStore {
    #[allow(dead_code)]
    client: DynamoDbClient,
}

impl MetadataStore {
    #[must_use]
    pub fn new(client: DynamoDbClient) -> Self {
        Self { client }
    }
}
