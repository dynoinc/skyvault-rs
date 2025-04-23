//! Library entry for skyvault2. Re-exports generated proto and service stubs.

use std::net::SocketAddr;
use std::sync::Arc;

use proto::batcher_server::{self, Batcher, BatcherServer};
use proto::index_server::{self, IndexServer};
use proto::orchestrator_server::{self, OrchestratorServer};
use tonic::transport::Server;
use tonic_health::ServingStatus;
use tracing::info;

pub mod proto {
    tonic::include_proto!("skyvault");
}

pub mod batcher_service;
pub mod index_service;
pub mod orchestrator_service;

pub mod metadata;
pub mod storage;
pub mod runs;

pub async fn server(
    addr: SocketAddr,
    metadata: Arc<dyn metadata::MetadataStore>,
    storage: Arc<dyn storage::ObjectStore>,
) -> Result<(), tonic::transport::Error> {
    let (health_reporter, health_service) = tonic_health::server::health_reporter();

    let batcher = batcher_service::MyBatcher::new(metadata.clone(), storage.clone());
    health_reporter
        .set_service_status(batcher_server::SERVICE_NAME, ServingStatus::Serving)
        .await;

    let index = index_service::MyIndex::new(metadata.clone(), storage.clone());
    health_reporter
        .set_service_status(index_server::SERVICE_NAME, ServingStatus::Serving)
        .await;

    let orchestrator = orchestrator_service::MyOrchestrator::new(metadata.clone());
    health_reporter
        .set_service_status(orchestrator_server::SERVICE_NAME, ServingStatus::Serving)
        .await;

    info!(address = %addr, services = "batcher,index,orchestrator", "Building gRPC server");
    Server::builder()
        .add_service(BatcherServer::new(batcher))
        .add_service(IndexServer::new(index))
        .add_service(OrchestratorServer::new(orchestrator))
        .add_service(health_service)
        .serve(addr)
        .await
}
