//! Library entry for skyvault2. Re-exports generated proto and service stubs.

use std::net::SocketAddr;

use clap::Parser;
use proto::cache_service_server::CacheServiceServer;
use proto::orchestrator_service_server::OrchestratorServiceServer;
use proto::reader_service_server::ReaderServiceServer;
use proto::writer_service_server::WriterServiceServer;
use tonic::transport::Server;
use tonic_health::ServingStatus;

pub mod proto {
    tonic::include_proto!("skyvault");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("skyvault_descriptor");
}

pub mod cache_service;
pub mod orchestrator_service;
pub mod reader_service;
pub mod writer_service;

mod consistent_hashring;
mod forest;
pub mod jobs;
mod k_way;
pub mod metadata;
mod pod_watcher;
mod runs;
pub mod storage;
#[cfg(test)]
pub mod test_utils;

#[derive(Debug, Parser, Clone)]
#[command(name = "skyvault", about = "A gRPC server for skyvault.")]
pub struct Config {
    #[arg(long, env = "SKYVAULT_GRPC_ADDR", value_parser = clap::value_parser!(SocketAddr), default_value = "0.0.0.0:50051")]
    pub grpc_addr: SocketAddr,

    #[arg(
        long,
        env = "SKYVAULT_METADATA_URL",
        default_value = "postgres://postgres:postgres@localhost:5432/skyvault"
    )]
    pub metadata_url: String,

    #[arg(long, env = "SKYVAULT_BUCKET_NAME", default_value = "skyvault-bucket")]
    pub bucket_name: String,

    #[arg(long, env = "SKYVAULT_IMAGE_ID", default_value = "skyvault")]
    pub image_id: String,

    #[arg(long, env = "SKYVAULT_ENABLE_WRITER", default_value = "true")]
    pub enable_writer: bool,

    #[arg(long, env = "SKYVAULT_ENABLE_READER", default_value = "true")]
    pub enable_reader: bool,

    #[arg(long, env = "SKYVAULT_ENABLE_ORCHESTRATOR", default_value = "true")]
    pub enable_orchestrator: bool,
}

/// Error types for the skyvault2 library.
#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    /// Errors from the transport layer.
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// Errors from the Forest component.
    #[error("Forest error: {0}")]
    Forest(#[from] forest::ForestError),

    /// Errors from the Index component.
    #[error("Index error: {0}")]
    Index(#[from] reader_service::ReaderServiceError),

    /// Errors from the Orchestrator component.
    #[error("Orchestrator error: {0}")]
    Orchestrator(#[from] orchestrator_service::OrchestratorError),

    /// Errors from the Cache component.
    #[error("Cache error: {0}")]
    Cache(#[from] cache_service::CacheServiceError),
}

pub async fn server(
    config: Config,
    metadata: metadata::MetadataStore,
    storage: storage::ObjectStore,
) -> Result<(), ServerError> {
    let (health_reporter, health_service) = tonic_health::server::health_reporter();

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("Failed to build reflection service");

    let mut builder = Server::builder().add_service(reflection_service);

    if config.enable_writer {
        let writer = writer_service::MyWriter::new(metadata.clone(), storage.clone());
        health_reporter
            .set_service_status(
                proto::writer_service_server::SERVICE_NAME,
                ServingStatus::Serving,
            )
            .await;

        builder = builder.add_service(WriterServiceServer::new(writer));
    }

    if config.enable_reader {
        let reader =
            reader_service::MyReader::new(metadata.clone(), config.grpc_addr.port()).await?;
        health_reporter
            .set_service_status(
                proto::reader_service_server::SERVICE_NAME,
                ServingStatus::Serving,
            )
            .await;

        let cache = cache_service::MyCache::new(storage.clone()).await?;
        health_reporter
            .set_service_status(
                proto::cache_service_server::SERVICE_NAME,
                ServingStatus::Serving,
            )
            .await;

        builder = builder.add_service(ReaderServiceServer::new(reader));
        builder = builder.add_service(CacheServiceServer::new(cache));
    }

    if config.enable_orchestrator {
        let orchestrator =
            orchestrator_service::MyOrchestrator::new(metadata.clone(), config.clone()).await?;
        health_reporter
            .set_service_status(
                proto::orchestrator_service_server::SERVICE_NAME,
                ServingStatus::Serving,
            )
            .await;

        builder = builder.add_service(OrchestratorServiceServer::new(orchestrator));
    }

    builder
        .add_service(health_service)
        .serve_with_shutdown(config.grpc_addr, async {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
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

    Ok(())
}
