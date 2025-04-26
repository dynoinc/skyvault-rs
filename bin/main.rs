use std::net::SocketAddr;

use anyhow::{Context, Result};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_s3::Client as S3Client;
use clap::Parser;
use rustls::crypto::aws_lc_rs;
use skyvault::{metadata, storage};
use tracing::info;
use tracing::level_filters::LevelFilter;

#[derive(Debug, Parser)]
#[command(name = "skyvault", about = "A gRPC server for skyvault.")]
pub struct Config {
    #[arg(long, env = "SKYVAULT_GRPC_ADDR", default_value = "0.0.0.0:50051")]
    pub grpc_addr: String,

    #[arg(long, env = "SKYVAULT_BUCKET_NAME", default_value = "skyvault-bucket")]
    pub bucket_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    aws_lc_rs::default_provider().install_default().expect("failed to install aws-lc-rs CryptoProvider");

    let config = Config::parse();
    let version = env!("CARGO_PKG_VERSION");
    info!(config = ?config, version = version, "Starting skyvault");

    let addr: SocketAddr = config
        .grpc_addr
        .parse()
        .with_context(|| format!("Failed to parse gRPC address: {}", config.grpc_addr))?;

    // Initialize AWS SDK
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

    // Create DynamoDB client
    let dynamodb_client = DynamoDbClient::new(&aws_config);
    let metadata = metadata::MetadataStore::new(dynamodb_client).await?;

    // Create S3 client with path style URLs
    let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
        .force_path_style(true)
        .build();
    let s3_client = S3Client::from_conf(s3_config);
    let storage = storage::ObjectStore::new(s3_client, &config.bucket_name).await?;

    info!(address = %addr, "Starting gRPC server");
    skyvault::server(addr, metadata, storage)
        .await
        .with_context(|| "Failed to start gRPC server")?;
    info!("gRPC server stopped");
    Ok(())
}
