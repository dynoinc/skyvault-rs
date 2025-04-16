use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use skyvault::{metadata, storage};
use slog::{Drain, Logger, info, o};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "skyvault", about = "A gRPC server for skyvault.")]
pub struct Config {
    #[structopt(long, env = "SKYVAULT_GRPC_ADDR", default_value = "0.0.0.0:50051")]
    pub grpc_addr: String,

    #[structopt(long, env = "SKYVAULT_METADATA_DB_PATH", default_value = "target/metadata.db")]
    pub metadata_db_path: String,

    #[structopt(long, env = "SKYVAULT_STORAGE_DIR", default_value = "target/storage")]
    pub storage_dir: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup slog logger
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let root_logger = Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));

    // Set the global logger
    let _guard = slog_scope::set_global_logger(root_logger.clone());
    // Also redirect standard log crate to slog
    slog_stdlog::init().unwrap();

    let config = Config::from_args();
    info!(root_logger, "Starting skyvault"; "config" => ?config);

    let addr: SocketAddr = config
        .grpc_addr
        .parse()
        .with_context(|| format!("Failed to parse gRPC address: {}", config.grpc_addr))?;

    let metadata = Arc::new(metadata::SqliteMetadataStore::new(config.metadata_db_path).with_context(|| "Failed to create metadata store")?);
    let storage = Arc::new(storage::LocalObjectStore::new(config.storage_dir).with_context(|| "Failed to create storage")?);

    info!(root_logger, "Starting gRPC server"; "address" => %addr);
    skyvault::server(addr, metadata, storage)
        .await
        .with_context(|| "Failed to start gRPC server")?;
    info!(root_logger, "gRPC server stopped");
    Ok(())
}
