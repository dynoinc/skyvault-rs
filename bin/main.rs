use std::sync::Arc;

use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use clap::Parser;
use kube::Client;
use rustls::crypto::aws_lc_rs;
use skyvault::{Config, metadata, storage};
use tokio::fs;
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

    // Initialize K8s client
    let k8s_client = Client::try_default().await.context("Failed to create Kubernetes client")?;

    // Determine namespace - we're always running in Kubernetes
    let namespace_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
    let current_namespace = fs::read_to_string(namespace_path).await
        .context(format!("Failed to read namespace from {}. This worker must run within a Kubernetes pod with a service account.", namespace_path))?
        .trim().to_string();

    info!(config = ?config, version = version, current_namespace = %current_namespace, "Starting skyvault");

    // Resolve metadata_url using K8s secret
    let metadata_url = config.postgres.resolve_metadata_url(k8s_client.clone(), &current_namespace).await
        .context("Failed to resolve metadata URL from Kubernetes secret")?;

    // Create metadata client
    let metadata = Arc::new(metadata::PostgresMetadataStore::new(metadata_url).await?);

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
