use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use skyvault::{metadata, storage};
use structopt::StructOpt;
use tracing::info;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Debug, StructOpt)]
#[structopt(name = "skyvault", about = "A gRPC server for skyvault.")]
pub struct Config {
    #[structopt(long, env = "SKYVAULT_GRPC_ADDR", default_value = "0.0.0.0:50051")]
    pub grpc_addr: String,

    #[structopt(
        long,
        env = "SKYVAULT_METADATA_DB_PATH",
        default_value = "target/metadata.db"
    )]
    pub metadata_db_path: String,

    #[structopt(long, env = "SKYVAULT_STORAGE_DIR", default_value = "target/storage")]
    pub storage_dir: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup tracing subscriber
    let subscriber = FmtSubscriber::builder()
        // Use EnvFilter to allow RUST_LOG environment variable to control level
        .with_env_filter(EnvFilter::from_default_env().add_directive("skyvault=info".parse()?))
        // Use pretty formatting for console output
        .pretty()
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Setting default tracing subscriber failed");

    let config = Config::from_args();
    info!(config = ?config, "Starting skyvault");

    let addr: SocketAddr = config
        .grpc_addr
        .parse()
        .with_context(|| format!("Failed to parse gRPC address: {}", config.grpc_addr))?;

    let metadata = Arc::new(
        metadata::SqliteMetadataStore::new(config.metadata_db_path)
            .with_context(|| "Failed to create metadata store")?,
    );
    let storage = Arc::new(
        storage::LocalObjectStore::new(config.storage_dir)
            .with_context(|| "Failed to create storage")?,
    );

    info!(address = %addr, "Starting gRPC server");
    skyvault::server(addr, metadata, storage)
        .await
        .with_context(|| "Failed to start gRPC server")?;
    info!("gRPC server stopped");
    Ok(())
}
