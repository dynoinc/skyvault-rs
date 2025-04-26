//! Library entry for skyvault2. Re-exports generated proto and service stubs.

use std::net::SocketAddr;

use proto::batcher_server::{self, Batcher, BatcherServer};
use proto::index_server::{self, IndexServer};
use proto::orchestrator_server::{self, OrchestratorServer};
use tonic::transport::Server;
use tonic_health::ServingStatus;

pub mod proto {
    tonic::include_proto!("skyvault");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("skyvault_descriptor");
}

pub mod batcher_service;
pub mod index_service;
pub mod orchestrator_service;

mod forest;
pub mod metadata;
mod runs;
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
}

pub async fn server(
    addr: SocketAddr,
    metadata: metadata::MetadataStore,
    storage: storage::ObjectStore,
) -> Result<(), ServerError> {
    let (health_reporter, health_service) = tonic_health::server::health_reporter();

    let batcher = batcher_service::MyBatcher::new(metadata.clone(), storage.clone());
    health_reporter
        .set_service_status(batcher_server::SERVICE_NAME, ServingStatus::Serving)
        .await;

    let index = index_service::MyIndex::new(metadata.clone(), storage.clone());
    health_reporter
        .set_service_status(index_server::SERVICE_NAME, ServingStatus::Serving)
        .await;

    let orchestrator = orchestrator_service::MyOrchestrator::new(metadata.clone()).await?;
    health_reporter
        .set_service_status(orchestrator_server::SERVICE_NAME, ServingStatus::Serving)
        .await;

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .unwrap();

    Server::builder()
        .add_service(BatcherServer::new(batcher))
        .add_service(IndexServer::new(index))
        .add_service(OrchestratorServer::new(orchestrator))
        .add_service(health_service)
        .add_service(reflection_service)
        .serve_with_shutdown(addr, async {
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

    Ok(())
}
