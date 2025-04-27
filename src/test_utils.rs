use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use crate::metadata::{MetadataError, MetadataStore};

/// Sets up a test PostgreSQL instance in a container for testing.
/// Returns the metadata store connected to the test DB and the container handle.
pub async fn setup_test_db() -> Result<(MetadataStore, ContainerAsync<Postgres>), MetadataError> {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start PostgreSQL container");

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get port");

    // Create PostgreSQL connection string
    let postgres_url = format!("postgres://postgres:postgres@localhost:{}/postgres", port);

    let metadata_store = MetadataStore::new(postgres_url)
        .await
        .expect("Failed to create metadata store");

    Ok((metadata_store, container))
}
