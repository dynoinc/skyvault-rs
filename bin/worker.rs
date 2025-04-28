use anyhow::Result;
use aws_sdk_s3::Client as S3Client;
use clap::Parser;
use rustls::crypto::aws_lc_rs;
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

    #[arg(long)]
    pub job_id: i64,
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
    info!(config = ?config, version = version, job_id = config.job_id, "Starting worker");

    // Create metadata client
    let _metadata = metadata::MetadataStore::new(config.metadata_url).await?;

    // Create S3 client with path style URLs
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
        .force_path_style(true)
        .build();
    let s3_client = S3Client::from_conf(s3_config);
    let _storage = storage::ObjectStore::new(s3_client, &config.bucket_name).await?;

    jobs::execute(_metadata, _storage, config.job_id).await?;
    info!("Complete!");
    Ok(())
}
