use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use clap::Parser;
use rustls::crypto::aws_lc_rs;
use skyvault::{Config, metadata, storage};
use std::sync::Arc;
use tracing::info;
use tracing::level_filters::LevelFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install aws-lc-rs CryptoProvider");

    let config = Config::parse();
    let version = env!("CARGO_PKG_VERSION");
    info!(config = ?config, version = version, "Starting skyvault");

    // Create metadata client
    let metadata = Arc::new(metadata::PostgresMetadataStore::new(config.metadata_url.clone()).await?);

    // Create S3 client with path style URLs
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
        .force_path_style(true)
        .build();
    let s3_client = S3Client::from_conf(s3_config);
    let storage = Arc::new(storage::S3ObjectStore::new(s3_client, &config.bucket_name).await?);

    skyvault::server(config, metadata, storage)
        .await
        .with_context(|| "Failed to start gRPC server")?;
    info!("gRPC server stopped");
    Ok(())
}
