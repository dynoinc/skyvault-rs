use std::sync::Arc;

use anyhow::Result;
use aws_sdk_s3::Client as S3Client;
use clap::Parser;
use rustls::crypto::aws_lc_rs;
use skyvault::metadata::JobId;
use skyvault::{jobs, metadata, storage};
use tracing::info;
use tracing::level_filters::LevelFilter;

#[derive(Debug, Parser)]
#[command(name = "worker", about = "A worker for skyvault.")]
pub struct Config {
    #[arg(
        long,
        env = "SKYVAULT_METADATA_URL",
        default_value = "postgres://postgres:postgres@localhost:5432/skyvault"
    )]
    pub metadata_url: String,

    #[arg(long, env = "SKYVAULT_BUCKET_NAME", default_value = "skyvault-bucket")]
    pub bucket_name: String,

    #[arg(long, value_parser = parse_job_id)]
    pub job_id: JobId,
}

// Custom parser function for JobId
fn parse_job_id(s: &str) -> Result<JobId, String> {
    s.parse::<i64>()
        .map(JobId::from)
        .map_err(|e| format!("Failed to parse job_id '{}' as integer: {}", s, e))
}

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
    info!(config = ?config, version = version, job_id = ?config.job_id, "Starting worker");

    // Create metadata client
    let metadata = Arc::new(metadata::PostgresMetadataStore::new(config.metadata_url).await?);

    // Create S3 client with path style URLs
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
        .force_path_style(true)
        .build();
    let s3_client = S3Client::from_conf(s3_config);
    let storage = Arc::new(storage::S3ObjectStore::new(s3_client, &config.bucket_name).await?);

    jobs::execute(metadata, storage, config.job_id).await?;
    info!("Complete!");
    Ok(())
}
