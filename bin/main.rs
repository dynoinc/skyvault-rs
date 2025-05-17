use std::sync::Arc;

use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::SharedCredentialsProvider;
use clap::Parser;
use rustls::crypto::aws_lc_rs;
use skyvault::k8s;
use skyvault::{Config, metadata, storage};
use tokio::fs;
use tracing::info;
use tracing_subscriber::EnvFilter;

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
    let k8s_client = k8s::create_k8s_client()
        .await
        .context("Failed to create Kubernetes client")?;

    // Determine namespace - we're always running in Kubernetes
    let namespace_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
    let current_namespace = fs::read_to_string(namespace_path)
        .await
        .context(format!(
            "Failed to read namespace from {}. This worker must run within a Kubernetes pod with \
             a service account.",
            namespace_path
        ))?
        .trim()
        .to_string();

    info!(config = ?config, version = version, current_namespace = %current_namespace, "Starting skyvault");

    // Resolve metadata_url using K8s secret
    let metadata_url = match config
        .postgres
        .resolve_metadata_url(k8s_client.clone(), &current_namespace)
        .await
        .context("Failed to resolve metadata URL from Kubernetes secret")
    {
        Ok(url) => url,
        Err(e) => {
            eprintln!("Failed to resolve metadata URL: {}", e);
            tokio::time::sleep(std::time::Duration::from_secs(600)).await;
            return Err(e);
        },
    };

    // Create metadata client
    let metadata = Arc::new(metadata::PostgresMetadataStore::new(metadata_url).await?);

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

    skyvault::server(config, metadata, storage)
        .await
        .with_context(|| "Failed to start gRPC server")?;
    info!("gRPC server stopped");
    Ok(())
}
