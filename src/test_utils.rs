use aws_sdk_dynamodb::client::Client as DynamoDbClient;
use aws_sdk_dynamodb::config::{Credentials, Region};
use testcontainers_modules::dynamodb_local::DynamoDb;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::metadata::{MetadataError, MetadataStore};

/// Sets up a test DynamoDB instance in a container for testing.
/// Returns the metadata store connected to the test DB and the container handle.
pub async fn setup_test_db() -> Result<(MetadataStore, ContainerAsync<DynamoDb>), MetadataError> {
    let container = DynamoDb::default()
        .start()
        .await
        .expect("Failed to start DynamoDB local container");
    let port = container
        .get_host_port_ipv4(8000)
        .await
        .expect("Failed to get port");
    let endpoint_url = format!("http://localhost:{}", port);

    let config = aws_sdk_dynamodb::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint_url)
        .credentials_provider(Credentials::new("dummy", "dummy", None, None, "dummy"))
        .build();

    let client = DynamoDbClient::from_conf(config);

    let metadata_store = MetadataStore::new(client)
        .await
        .expect("Failed to create metadata store");

    Ok((metadata_store, container))
}
