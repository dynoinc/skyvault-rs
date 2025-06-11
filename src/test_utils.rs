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
use once_cell::sync::OnceCell;
use testcontainers_modules::{
    minio,
    postgres::Postgres,
    testcontainers::{
        ContainerAsync,
        runners::AsyncRunner,
    },
};
use tokio::sync::Mutex;
use uuid::Uuid;

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

// Global shared PostgreSQL container for all tests
static SHARED_PG_CONTAINER: OnceCell<Arc<Mutex<Option<ContainerAsync<Postgres>>>>> = OnceCell::new();

// Global shared MinIO container for all tests
static SHARED_MINIO_CONTAINER: OnceCell<Arc<Mutex<Option<ContainerAsync<minio::MinIO>>>>> = OnceCell::new();

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

/// Sets up a test PostgreSQL instance in a shared container.
/// Creates a unique database for each test to avoid conflicts.
/// Returns the metadata store connected to the test DB.
pub async fn setup_test_db() -> Result<MetadataStore, MetadataError> {
    let container_lock = SHARED_PG_CONTAINER.get_or_init(|| Arc::new(Mutex::new(None)));
    let mut container_guard = container_lock.lock().await;

    // Initialize container if it doesn't exist
    if container_guard.is_none() {
        let container = Postgres::default()
            .start()
            .await
            .expect("Failed to start PostgreSQL container");
        *container_guard = Some(container);
    }

    let container = container_guard.as_ref().unwrap();
    let port = container.get_host_port_ipv4(5432).await.expect("Failed to get port");

    // Create a unique database name for this test
    let db_name = format!("test_db_{}", Uuid::new_v4().to_string().replace('-', ""));

    // First connect to postgres database to create our test database
    let admin_url = format!("postgres://postgres:postgres@localhost:{port}/postgres");
    let admin_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(30))
        .connect(&admin_url)
        .await
        .map_err(MetadataError::DatabaseError)?;

    // Create the test database
    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&admin_pool)
        .await
        .map_err(MetadataError::DatabaseError)?;

    // Close admin pool to free connections
    admin_pool.close().await;

    // Now connect to our test database
    let test_url = format!("postgres://postgres:postgres@localhost:{port}/{db_name}");
    let test_store = PostgresMetadataStore::from_url(test_url).await?;

    drop(container_guard); // Release the lock

    Ok(test_store)
}

/// Sets up a test MinIO instance in a shared container for testing
/// S3ObjectStore. Creates a unique bucket for each test to avoid conflicts.
/// Returns the object store connected to the test MinIO.
pub async fn setup_test_object_store() -> Result<ObjectStore, StorageError> {
    let container_lock = SHARED_MINIO_CONTAINER.get_or_init(|| Arc::new(Mutex::new(None)));
    let mut container_guard = container_lock.lock().await;

    // Initialize container if it doesn't exist
    if container_guard.is_none() {
        let container = minio::MinIO::default()
            .start()
            .await
            .expect("Failed to start Minio container");
        *container_guard = Some(container);
    }

    let container = container_guard.as_ref().unwrap();
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("Failed to get Minio port");

    drop(container_guard); // Release the lock

    // Create MinIO connection details
    let endpoint_url = format!("http://localhost:{port}");
    let region = Region::new("us-east-1"); // MinIO requires a region

    // Create a unique bucket name for this test
    let bucket_name = format!("test-bucket-{}", Uuid::new_v4().to_string().replace('-', ""));

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
    let object_store = S3ObjectStore::new(s3_config, &bucket_name).await?;

    Ok(Arc::new(object_store))
}
