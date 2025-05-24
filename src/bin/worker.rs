use std::sync::Arc;

use anyhow::{
    Context,
    Result,
};
use clap::Parser;
use rustls::crypto::aws_lc_rs;
use skyvault::{
    config::{
        PostgresConfig,
        S3Config,
    },
    dynamic_config,
    jobs,
    k8s,
    metadata,
    metadata::JobID,
    observability,
    storage,
};

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
    let _sentry = observability::init_tracing_and_sentry();

    aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install aws-lc-rs CryptoProvider");

    let config = Config::parse();
    let version = env!("CARGO_PKG_VERSION");

    // Initialize K8s client
    let current_namespace = k8s::get_namespace().await.context("Failed to get namespace")?;
    let k8s_client = k8s::create_k8s_client()
        .await
        .context("Failed to create Kubernetes client")?;

    tracing::info!(config = ?config, version = version, job_id = ?config.job_id, namespace = %current_namespace, "Starting worker");

    // Initialize Dynamic Configuration
    let _dynamic_app_config = dynamic_config::initialize_dynamic_config(k8s_client.clone(), &current_namespace)
        .await
        .context("Failed to initialize dynamic configuration")?;

    // Create metadata client
    let metadata_url = config.postgres.to_url(k8s_client.clone(), &current_namespace).await?;
    let metadata_store = Arc::new(metadata::PostgresMetadataStore::new(metadata_url).await?);

    // Create storage client
    let s3_config = config.s3.to_config(k8s_client.clone(), &current_namespace).await?;
    let storage = Arc::new(storage::S3ObjectStore::new(s3_config, &config.s3.bucket_name).await?);

    jobs::execute(metadata_store, storage, config.job_id).await?;
    tracing::info!("Complete!");
    Ok(())
}
