use std::{
    net::SocketAddr,
    sync::Arc,
};

use anyhow::{
    Context,
    Result,
};
use clap::{
    Parser,
    ValueEnum,
};
use rustls::crypto::aws_lc_rs;
use skyvault::{
    cache_service,
    config::{
        PostgresConfig,
        S3Config,
    },
    dynamic_config,
    k8s,
    metadata,
    observability,
    orchestrator_service,
    proto,
    reader_service,
    storage,
    writer_service,
};
use tonic::transport::Server;
use tonic_health::ServingStatus;
use tracing::info;

#[derive(Copy, Clone, ValueEnum, Debug)]
enum Service {
    Reader,
    Cache,
    Orchestrator,
    Writer,
}

#[derive(Debug, Parser, Clone)]
#[command(name = "skyvault", about = "A gRPC server for skyvault.")]
struct Config {
    #[arg(long, env = "SKYVAULT_GRPC_ADDR", value_parser = clap::value_parser!(SocketAddr), default_value = "0.0.0.0:50051")]
    grpc_addr: SocketAddr,

    #[arg(long, env = "SKYVAULT_METRICS_ADDR", value_parser = clap::value_parser!(SocketAddr), default_value = "0.0.0.0:9095")]
    metrics_addr: SocketAddr,

    #[clap(flatten)]
    postgres: PostgresConfig,

    #[clap(flatten)]
    s3: S3Config,

    #[arg(long, env = "SKYVAULT_SERVICE", default_value = "reader")]
    service: Service,

    #[clap(flatten)]
    reader_config: reader_service::ReaderConfig,

    #[clap(flatten)]
    cache_config: cache_service::CacheConfig,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    let _sentry = observability::init_tracing_and_sentry();

    aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to install aws-lc-rs CryptoProvider");

    let version = env!("CARGO_PKG_VERSION");

    let handle = observability::init_metrics_recorder();

    let _metrics_server_handle = {
        let handle_clone = handle.clone();
        let metrics_addr = config.metrics_addr;
        tokio::spawn(async move {
            observability::serve_metrics(metrics_addr, handle_clone)
                .await
                .expect("Failed to start metrics server");
        })
    };

    // Initialize K8s client
    let current_namespace = k8s::get_namespace()
        .await
        .context("Failed to get current Kubernetes namespace")?;
    let k8s_client = k8s::create_k8s_client()
        .await
        .context("Failed to create Kubernetes client")?;

    info!(config = ?config, version = version, current_namespace = %current_namespace, "Starting skyvault");

    // Initialize Dynamic Configuration
    let dynamic_app_config = dynamic_config::initialize_dynamic_config(k8s_client.clone(), &current_namespace)
        .await
        .context("Failed to initialize dynamic configuration")?;

    // Create metadata client
    let metadata_url = config.postgres.to_url(k8s_client.clone(), &current_namespace).await?;
    let metadata = metadata::PostgresMetadataStore::from_url(metadata_url).await?;

    // Create storage client
    let s3_config = config.s3.to_config(k8s_client.clone(), &current_namespace).await?;
    let storage = Arc::new(storage::S3ObjectStore::new(s3_config, &config.s3.bucket_name).await?);

    // Start gRPC server
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("Failed to build reflection service");

    let mut builder = Server::builder()
        .layer(observability::MetricsLayer)
        .add_service(reflection_service);

    match config.service {
        Service::Writer => {
            let writer =
                writer_service::MyWriter::new(metadata.clone(), storage.clone(), dynamic_app_config.clone()).await?;
            health_reporter
                .set_service_status(proto::writer_service_server::SERVICE_NAME, ServingStatus::Serving)
                .await;

            builder = builder.add_service(proto::writer_service_server::WriterServiceServer::new(writer));
        },
        Service::Reader => {
            let reader = reader_service::MyReader::new(
                metadata.clone(),
                storage.clone(),
                config.reader_config.clone(),
                k8s_client.clone(),
                current_namespace.clone(),
            )
            .await?;
            health_reporter
                .set_service_status(proto::reader_service_server::SERVICE_NAME, ServingStatus::Serving)
                .await;

            builder = builder.add_service(proto::reader_service_server::ReaderServiceServer::new(reader));
        },
        Service::Cache => {
            let cache = cache_service::MyCache::new(storage.clone(), config.cache_config.clone()).await?;
            health_reporter
                .set_service_status(proto::cache_service_server::SERVICE_NAME, ServingStatus::Serving)
                .await;

            builder = builder.add_service(proto::cache_service_server::CacheServiceServer::new(cache));
        },
        Service::Orchestrator => {
            let orchestrator = orchestrator_service::MyOrchestrator::new(
                metadata.clone(),
                storage.clone(),
                k8s_client.clone(),
                current_namespace.clone(),
                dynamic_app_config.clone(),
            )
            .await?;
            health_reporter
                .set_service_status(proto::orchestrator_service_server::SERVICE_NAME, ServingStatus::Serving)
                .await;

            builder = builder.add_service(proto::orchestrator_service_server::OrchestratorServiceServer::new(
                orchestrator,
            ));
        },
    }

    builder
        .add_service(health_service)
        .serve_with_shutdown(config.grpc_addr, async {
            let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Failed to install SIGTERM handler");

            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Received SIGINT, shutting down");
                },
                _ = terminate.recv() => {
                    tracing::info!("Received SIGTERM, shutting down");
                },
            }
        })
        .await?;

    info!("gRPC server stopped");
    Ok(())
}
