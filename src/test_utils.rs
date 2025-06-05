use std::{
    process::{
        Command,
        Stdio,
    },
    sync::Arc,
};

use aws_config::{
    BehaviorVersion,
    Region,
    SdkConfig,
};
use aws_sdk_s3::config::{
    Credentials,
    SharedCredentialsProvider,
};
use testcontainers_modules::{
    minio,
    postgres::Postgres,
    testcontainers::{
        ContainerAsync,
        runners::AsyncRunner,
    },
};

use crate::{
    metadata::{
        MetadataError,
        MetadataStore,
        PostgresMetadataStore,
    },
    storage::{
        ObjectStore,
        S3ObjectStore,
        StorageError,
    },
};

/// Returns `true` if Docker is available and responding, otherwise `false`.
pub fn docker_is_available() -> bool {
    Command::new("docker")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Macro to skip tests when Docker is not available.
#[macro_export]
macro_rules! requires_docker {
    () => {
        if !$crate::test_utils::docker_is_available() {
            eprintln!("Docker not running - skipping test");
            return;
        }
    };
}

/// Sets up a test PostgreSQL instance in a container for testing.
/// Returns the metadata store connected to the test DB and the container
/// handle.
pub async fn setup_test_db() -> Result<(MetadataStore, ContainerAsync<Postgres>), MetadataError> {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start PostgreSQL container");

    let port = container.get_host_port_ipv4(5432).await.expect("Failed to get port");

    // Create PostgreSQL connection string
    let postgres_url = format!("postgres://postgres:postgres@localhost:{}/postgres", port);

    let metadata_store = PostgresMetadataStore::from_url(postgres_url).await?;

    Ok((metadata_store, container))
}

/// Sets up a test MinIO instance in a container for testing S3ObjectStore.
/// Returns the object store connected to the test MinIO and the container
/// handle.
pub async fn setup_test_object_store() -> Result<(ObjectStore, ContainerAsync<minio::MinIO>), StorageError> {
    let container = minio::MinIO::default()
        .start()
        .await
        .expect("Failed to start Minio container");

    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("Failed to get Minio port");

    // Create MinIO connection details
    let endpoint_url = format!("http://localhost:{}", port);
    let region = Region::new("us-east-1"); // MinIO requires a region
    let bucket_name = "test-bucket";

    // Dummy credentials for local MinIO
    let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "local");

    // Create S3 client config pointing to local MinIO
    let config = SdkConfig::builder()
        .endpoint_url(endpoint_url)
        .region(region)
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        .behavior_version(BehaviorVersion::latest())
        .build();
    let s3_config = aws_sdk_s3::config::Builder::from(&config)
        .force_path_style(true)
        .build();

    // Create the S3ObjectStore
    let object_store = S3ObjectStore::new(s3_config, bucket_name).await?;

    Ok((Arc::new(object_store), container))
}
