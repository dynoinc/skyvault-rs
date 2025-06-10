use std::sync::Arc;

use anyhow::{
    Context,
    Result,
};
use clap::Parser;
use rustls::crypto::aws_lc_rs;
use skyvault::{
    config::{
        OtelConfig,
        PostgresConfig,
        S3Config,
        SentryConfig,
    },
    dynamic_config,
    jobs,
    k8s,
    metadata::{
        self,
        JobID,
    },
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

    #[clap(flatten)]
    pub sentry: SentryConfig,

    #[clap(flatten)]
    pub otel: OtelConfig,

    #[arg(long)]
    pub job_id: JobID,
}

#[tokio::main]
async fn main() -> Result<()> {
    aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install aws-lc-rs CryptoProvider");

    let config = Config::parse();
    let version = env!("CARGO_PKG_VERSION");
    let _sentry = observability::init_tracing_and_sentry(config.sentry.clone());

    // Initialize OpenTelemetry metrics
    observability::init_otel_metrics(config.otel.clone()).expect("Failed to initialize OpenTelemetry metrics");

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
    let metadata_store = metadata::PostgresMetadataStore::from_url(metadata_url).await?;

    // Create storage client
    let s3_config = config.s3.to_config(k8s_client.clone(), &current_namespace).await?;
    let storage = Arc::new(storage::S3ObjectStore::new(s3_config, &config.s3.bucket_name).await?);

    // Start a trace for the whole job execution
    let tx_ctx = sentry::TransactionContext::new(&format!("job-execution-{}", config.job_id), "job.execute");
    let transaction = sentry::start_transaction(tx_ctx);

    // Set transaction as current span and add metadata
    sentry::configure_scope(|scope| scope.set_span(Some(transaction.clone().into())));
    transaction.set_data("job_id", sentry::protocol::Value::String(config.job_id.to_string()));
    transaction.set_data("worker_version", sentry::protocol::Value::String(version.to_string()));

    let result = jobs::execute(metadata_store, storage, config.job_id).await;

    match &result {
        Ok(_) => {
            transaction.set_status(sentry::protocol::SpanStatus::Ok);
            tracing::info!("Job completed successfully");
        },
        Err(e) => {
            transaction.set_status(sentry::protocol::SpanStatus::InternalError);
            transaction.set_data("error", sentry::protocol::Value::String(e.to_string()));
            tracing::error!(error = %e, "Job failed");
        },
    }

    transaction.finish();
    Ok(result?)
}
