use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use rustls::crypto::aws_lc_rs;
use skyvault::config::{PostgresConfig, S3Config};
use skyvault::{k8s, metadata, metrics, storage, telemetry};
use tracing::info;

#[derive(Debug, Parser, Clone)]
#[command(name = "skyvault", about = "A gRPC server for skyvault.")]
pub struct Config {
    #[arg(long, env = "SKYVAULT_GRPC_ADDR", value_parser = clap::value_parser!(SocketAddr), default_value = "0.0.0.0:50051")]
    pub grpc_addr: SocketAddr,

    #[clap(flatten)]
    pub postgres: PostgresConfig,

    #[clap(flatten)]
    pub s3: S3Config,

    #[arg(long, env = "SKYVAULT_ENABLE_WRITER", default_value = "true")]
    pub enable_writer: bool,

    #[arg(long, env = "SKYVAULT_ENABLE_READER", default_value = "true")]
    pub enable_reader: bool,

    #[arg(long, env = "SKYVAULT_ENABLE_ORCHESTRATOR", default_value = "true")]
    pub enable_orchestrator: bool,

    #[arg(long, env = "SKYVAULT_METRICS_ADDR", value_parser = clap::value_parser!(SocketAddr), default_value = "0.0.0.0:9095")]
    pub metrics_addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _sentry = telemetry::init();

    aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install aws-lc-rs CryptoProvider");

    let config = Config::parse();
    let version = env!("CARGO_PKG_VERSION");

    let handle = metrics::init_recorder();
    tokio::spawn(metrics::serve(config.metrics_addr, handle.clone()));

    // Initialize K8s client
    let current_namespace = k8s::get_namespace()
        .await
        .context("Failed to get namespace")?;
    let k8s_client = k8s::create_k8s_client()
        .await
        .context("Failed to create Kubernetes client")?;

    info!(config = ?config, version = version, current_namespace = %current_namespace, "Starting skyvault");

    // Create metadata client
    let metadata_url = config
        .postgres
        .to_url(k8s_client.clone(), &current_namespace)
        .await?;
    let metadata = Arc::new(metadata::PostgresMetadataStore::new(metadata_url).await?);

    // Create storage client
    let s3_config = config
        .s3
        .to_config(k8s_client.clone(), &current_namespace)
        .await?;
    let storage = Arc::new(storage::S3ObjectStore::new(s3_config, &config.s3.bucket_name).await?);

    skyvault::Builder::new(config.grpc_addr, metadata, storage)
        .with_writer(config.enable_writer)
        .with_reader(config.enable_reader)
        .with_orchestrator(config.enable_orchestrator)
        .start()
        .await
        .with_context(|| "Failed to start gRPC server")?;
    info!("gRPC server stopped");
    Ok(())
}
