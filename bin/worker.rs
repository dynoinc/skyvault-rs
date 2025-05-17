use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use rustls::crypto::aws_lc_rs;
use skyvault::config::{PostgresConfig, S3Config};
use skyvault::metadata::JobID;
use skyvault::{jobs, k8s, metadata, storage};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "worker", about = "A worker for skyvault.")]
pub struct Config {
    #[clap(flatten)]
    pub postgres: PostgresConfig,

    #[clap(flatten)]
    pub s3: S3Config,

    #[arg(long)]
    pub job_id: JobID,
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

    // Create metadata client
    let metadata_url = config
        .postgres
        .to_url(k8s_client.clone(), &current_namespace)
        .await?;
    let metadata_store = Arc::new(metadata::PostgresMetadataStore::new(metadata_url).await?);

    // Create storage client
    let s3_config = config
        .s3
        .to_config(k8s_client.clone(), &current_namespace)
        .await?;
    let storage = Arc::new(storage::S3ObjectStore::new(s3_config, &config.s3.bucket_name).await?);

    jobs::execute(metadata_store, storage, config.job_id).await?;
    info!("Complete!");
    Ok(())
}
