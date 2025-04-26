//! Library entry for skyvault2. Re-exports generated proto and service stubs.

use std::net::SocketAddr;

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

pub mod orchestrator_service;
pub mod reader_service;
pub mod writer_service;

mod consistent_hashring;
mod forest;
pub mod metadata;
mod pod_watcher;
mod runs;
mod runs_cache;
pub mod storage;
#[cfg(test)]
pub mod test_utils;

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
}

pub async fn server(
    addr: SocketAddr,
    metadata: metadata::MetadataStore,
    storage: storage::ObjectStore,
) -> Result<(), ServerError> {
    let (health_reporter, health_service) = tonic_health::server::health_reporter();

    let writer = writer_service::MyWriter::new(metadata.clone(), storage.clone());
    health_reporter
        .set_service_status(
            proto::writer_service_server::SERVICE_NAME,
            ServingStatus::Serving,
        )
        .await;

    let reader =
        reader_service::MyReader::new(metadata.clone(), storage.clone(), addr.port()).await?;
    health_reporter
        .set_service_status(
            proto::reader_service_server::SERVICE_NAME,
            ServingStatus::Serving,
        )
        .await;

    let orchestrator = orchestrator_service::MyOrchestrator::new(metadata.clone()).await?;
    health_reporter
        .set_service_status(
            proto::orchestrator_service_server::SERVICE_NAME,
            ServingStatus::Serving,
        )
        .await;

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("Failed to build reflection service");

    Server::builder()
        .add_service(WriterServiceServer::new(writer))
        .add_service(ReaderServiceServer::new(reader))
        .add_service(OrchestratorServiceServer::new(orchestrator))
        .add_service(health_service)
        .add_service(reflection_service)
        .serve_with_shutdown(addr, async {
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
