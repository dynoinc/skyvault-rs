use std::sync::Arc;

use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::SharedCredentialsProvider;
use clap::Parser;
use rustls::crypto::aws_lc_rs;
use skyvault::metadata::JobId;
use skyvault::{PostgresConfig, jobs, k8s, metadata, storage};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "worker", about = "A worker for skyvault.")]
pub struct Config {
    #[clap(flatten)]
    pub postgres: PostgresConfig,

    #[arg(long, env = "SKYVAULT_BUCKET_NAME", default_value = "skyvault-bucket")]
    pub bucket_name: String,

    #[arg(long, value_parser = parse_job_id)]
    pub job_id: JobId,
}

// Custom parser function for JobId
fn parse_job_id(s: &str) -> Result<JobId, String> {
    s.parse::<i64>()
        .map(JobId::from)
        .map_err(|e| format!("Failed to parse job_id '{s}' as integer: {e}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install aws-lc-rs CryptoProvider");

    let config = Config::parse();
    let version = env!("CARGO_PKG_VERSION");

    // Initialize K8s client
    let current_namespace = k8s::get_namespace()
        .await
        .context("Failed to get namespace")?;
    let k8s_client = k8s::create_k8s_client()
        .await
        .context("Failed to create Kubernetes client")?;

    info!(config = ?config, version = version, job_id = ?config.job_id, namespace = %current_namespace, "Starting worker");

    // Resolve metadata_url using K8s secret
    let metadata_url = config
        .postgres
        .resolve_metadata_url(k8s_client.clone(), &current_namespace)
        .await
        .context("Failed to resolve metadata URL from Kubernetes secret")?;

    // Create metadata client
    let metadata_store = Arc::new(metadata::PostgresMetadataStore::new(metadata_url).await?);

    // Create S3 client with path style URLs
    let aws_creds = k8s::get_aws_credentials(k8s_client.clone(), &current_namespace)
        .await
        .context("Failed to load AWS credentials from Kubernetes secret")?;
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .credentials_provider(SharedCredentialsProvider::new(aws_creds))
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
        .force_path_style(true)
        .build();
    let s3_client = S3Client::from_conf(s3_config);
    let storage = Arc::new(storage::S3ObjectStore::new(s3_client, &config.bucket_name).await?);

    jobs::execute(metadata_store, storage, config.job_id).await?;
    info!("Complete!");
    Ok(())
}
