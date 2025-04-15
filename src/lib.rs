//! Library entry for skyvault2. Re-exports generated proto and service stubs.

use std::net::SocketAddr;

pub use skyvault::batcher_client::BatcherClient;
pub use skyvault::batcher_server::{self, Batcher, BatcherServer};
pub use skyvault::index_client::IndexClient;
pub use skyvault::index_server::{self, Index, IndexServer};
pub use skyvault::orchestrator_client::OrchestratorClient;
pub use skyvault::orchestrator_server::{self, Orchestrator, OrchestratorServer};
pub use skyvault::{BatchRequest, IndexRequest, OrchestrateRequest};
use slog::info;
use tonic::transport::Server;
use tonic_health::ServingStatus;

pub mod skyvault {
    tonic::include_proto!("skyvault");
}

pub mod batcher_service;
pub mod index_service;
pub mod orchestrator_service;
pub mod storage;
pub mod metadata;

pub async fn server(addr: SocketAddr) -> Result<(), tonic::transport::Error> {
    // Get logger from slog_scope
    let log = slog_scope::logger();
    let (health_reporter, health_service) = tonic_health::server::health_reporter();

    let batcher = batcher_service::MyBatcher;
    health_reporter
        .set_service_status(batcher_server::SERVICE_NAME, ServingStatus::Serving)
        .await;

    let index = index_service::MyIndex;
    health_reporter
        .set_service_status(index_server::SERVICE_NAME, ServingStatus::Serving)
        .await;

    let orchestrator = orchestrator_service::MyOrchestrator;
    health_reporter
        .set_service_status(orchestrator_server::SERVICE_NAME, ServingStatus::Serving)
        .await;

    info!(log, "Building gRPC server"; "services" => "batcher,index,orchestrator,health");
    Server::builder()
        .add_service(BatcherServer::new(batcher))
        .add_service(IndexServer::new(index))
        .add_service(OrchestratorServer::new(orchestrator))
        .add_service(health_service)
        .serve(addr)
        .await
}
