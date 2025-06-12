use std::{
    process::{
        Command,
        Stdio,
    },
    sync::{
        Arc,
        OnceLock,
        Weak,
    },
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
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    metadata::{
        MetadataError,
        MetadataStore,
        MetadataStoreTrait,
        PostgresMetadataStore,
    },
    runs::{
        RunID,
        Stats,
    },
    storage::{
        ObjectStore,
        ObjectStoreTrait,
        S3ObjectStore,
        StorageError,
    },
};

// Global shared PostgreSQL container using weak references for automatic
// cleanup
static SHARED_PG_CONTAINER: OnceLock<Mutex<Weak<ContainerAsync<Postgres>>>> = OnceLock::new();

// Global shared MinIO container using weak references for automatic cleanup
static SHARED_MINIO_CONTAINER: OnceLock<Mutex<Weak<ContainerAsync<minio::MinIO>>>> = OnceLock::new();

/// Wrapper that holds both the container (to keep it alive) and the metadata
/// store
pub struct TestMetadataStore {
    _container: Arc<ContainerAsync<Postgres>>,
    inner: MetadataStore,
}

impl TestMetadataStore {
    pub fn new(container: Arc<ContainerAsync<Postgres>>, inner: MetadataStore) -> Self {
        Self {
            _container: container,
            inner,
        }
    }
}

#[async_trait::async_trait]
impl MetadataStoreTrait for TestMetadataStore {
    async fn get_latest_snapshot_id(
        &self,
    ) -> Result<Option<(crate::metadata::SnapshotID, crate::metadata::SeqNo)>, MetadataError> {
        self.inner.get_latest_snapshot_id().await
    }

    async fn get_latest_snapshot(
        &self,
    ) -> Result<
        (
            Option<crate::metadata::SnapshotID>,
            Vec<crate::metadata::ChangelogEntryWithID>,
        ),
        MetadataError,
    > {
        self.inner.get_latest_snapshot().await
    }

    async fn persist_snapshot(
        &self,
        snapshot_id: crate::metadata::SnapshotID,
        seq_no: crate::metadata::SeqNo,
    ) -> Result<(), MetadataError> {
        self.inner.persist_snapshot(snapshot_id, seq_no).await
    }

    async fn stream_changelog(
        &self,
    ) -> Result<
        (
            Option<crate::metadata::SnapshotID>,
            std::pin::Pin<
                std::boxed::Box<
                    dyn futures::Stream<Item = Result<crate::metadata::ChangelogEntryWithID, MetadataError>>
                        + Send
                        + 'static,
                >,
            >,
        ),
        MetadataError,
    > {
        self.inner.stream_changelog().await
    }

    async fn get_changelog(
        &self,
        from_seq_no: crate::metadata::SeqNo,
    ) -> Result<Vec<crate::metadata::ChangelogEntryWithID>, MetadataError> {
        self.inner.get_changelog(from_seq_no).await
    }

    async fn append_wal(&self, run_ids: Vec<(RunID, Stats)>) -> Result<crate::metadata::SeqNo, MetadataError> {
        self.inner.append_wal(run_ids).await
    }

    async fn append_wal_compaction(
        &self,
        job_id: Option<crate::metadata::JobID>,
        compacted: Vec<RunID>,
        new_table_runs: Vec<(RunID, crate::metadata::TableID, Stats)>,
    ) -> Result<crate::metadata::SeqNo, MetadataError> {
        self.inner
            .append_wal_compaction(job_id, compacted, new_table_runs)
            .await
    }

    async fn append_table_compaction(
        &self,
        job_id: crate::metadata::JobID,
        compacted: Vec<RunID>,
        new_runs: Vec<crate::metadata::RunMetadata>,
    ) -> Result<crate::metadata::SeqNo, MetadataError> {
        self.inner.append_table_compaction(job_id, compacted, new_runs).await
    }

    async fn get_run_metadata_batch(
        &self,
        run_ids: Vec<RunID>,
    ) -> Result<std::collections::HashMap<RunID, crate::metadata::RunMetadata>, MetadataError> {
        self.inner.get_run_metadata_batch(run_ids).await
    }

    async fn schedule_job(
        &self,
        job_params: crate::metadata::JobParams,
    ) -> Result<crate::metadata::JobID, MetadataError> {
        self.inner.schedule_job(job_params).await
    }

    async fn get_pending_jobs(
        &self,
    ) -> Result<Vec<(crate::metadata::JobID, crate::metadata::JobParams)>, MetadataError> {
        self.inner.get_pending_jobs().await
    }

    async fn get_job(&self, job_id: crate::metadata::JobID) -> Result<crate::metadata::Job, MetadataError> {
        self.inner.get_job(job_id).await
    }

    async fn list_jobs(&self, limit: i64) -> Result<Vec<crate::metadata::Job>, MetadataError> {
        self.inner.list_jobs(limit).await
    }

    async fn mark_job_failed(&self, job_id: crate::metadata::JobID) -> Result<(), MetadataError> {
        self.inner.mark_job_failed(job_id).await
    }

    async fn create_table(
        &self,
        config: crate::metadata::TableConfig,
    ) -> Result<crate::metadata::SeqNo, MetadataError> {
        self.inner.create_table(config).await
    }

    async fn drop_table(
        &self,
        table_name: crate::metadata::TableName,
    ) -> Result<crate::metadata::SeqNo, MetadataError> {
        self.inner.drop_table(table_name).await
    }

    async fn list_tables(&self) -> Result<Vec<crate::metadata::TableConfig>, MetadataError> {
        self.inner.list_tables().await
    }

    async fn get_table(
        &self,
        table_name: crate::metadata::TableName,
    ) -> Result<crate::metadata::TableConfig, MetadataError> {
        self.inner.get_table(table_name).await
    }

    async fn get_table_by_id(
        &self,
        table_id: crate::metadata::TableID,
    ) -> Result<crate::metadata::TableConfig, MetadataError> {
        self.inner.get_table_by_id(table_id).await
    }
}

/// Wrapper that holds both the container (to keep it alive) and the object
/// store
pub struct TestObjectStore {
    _container: Arc<ContainerAsync<minio::MinIO>>,
    inner: ObjectStore,
}

impl TestObjectStore {
    pub fn new(container: Arc<ContainerAsync<minio::MinIO>>, inner: ObjectStore) -> Self {
        Self {
            _container: container,
            inner,
        }
    }
}

#[async_trait::async_trait]
impl ObjectStoreTrait for TestObjectStore {
    async fn put_run(&self, run_id: crate::runs::RunID, data: bytes::Bytes) -> Result<(), StorageError> {
        self.inner.put_run(run_id, data).await
    }

    async fn get_run(&self, run_id: crate::runs::RunID) -> Result<aws_sdk_s3::primitives::ByteStream, StorageError> {
        self.inner.get_run(run_id).await
    }

    async fn put_snapshot(
        &self,
        snapshot_id: crate::metadata::SnapshotID,
        data: bytes::Bytes,
    ) -> Result<(), StorageError> {
        self.inner.put_snapshot(snapshot_id, data).await
    }

    async fn get_snapshot(&self, snapshot_id: crate::metadata::SnapshotID) -> Result<bytes::Bytes, StorageError> {
        self.inner.get_snapshot(snapshot_id).await
    }
}

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

/// Sets up a test PostgreSQL instance using shared container with automatic
/// cleanup. Creates a unique database for each test to avoid conflicts.
/// Returns a wrapper that keeps the container alive and delegates to the
/// metadata store.
pub async fn setup_test_db() -> Result<MetadataStore, MetadataError> {
    let container_lock = SHARED_PG_CONTAINER.get_or_init(|| Mutex::new(Weak::new()));
    let mut guard = container_lock.lock().await;

    let container = if let Some(existing) = guard.upgrade() {
        // Reuse existing container
        existing
    } else {
        // Create new container
        let new_container = Arc::new(
            Postgres::default()
                .start()
                .await
                .expect("Failed to start PostgreSQL container"),
        );
        *guard = Arc::downgrade(&new_container);
        new_container
    };

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
    let inner_store = PostgresMetadataStore::from_url(test_url).await?;

    Ok(Arc::new(TestMetadataStore::new(container, inner_store)))
}

/// Sets up a test MinIO instance using shared container with automatic cleanup.
/// Creates a unique bucket for each test to avoid conflicts.
/// Returns a wrapper that keeps the container alive and delegates to the object
/// store.
pub async fn setup_test_object_store() -> Result<ObjectStore, StorageError> {
    let container_lock = SHARED_MINIO_CONTAINER.get_or_init(|| Mutex::new(Weak::new()));
    let mut guard = container_lock.lock().await;

    let container = if let Some(existing) = guard.upgrade() {
        // Reuse existing container
        existing
    } else {
        // Create new container
        let new_container = Arc::new(
            minio::MinIO::default()
                .start()
                .await
                .expect("Failed to start Minio container"),
        );
        *guard = Arc::downgrade(&new_container);
        new_container
    };

    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("Failed to get Minio port");

    drop(guard); // Release the lock

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
    let inner_store = S3ObjectStore::from(s3_config, &bucket_name).await?;

    Ok(Arc::new(TestObjectStore::new(container, inner_store)))
}
