use std::net::SocketAddr;

use anyhow::Result;
use tonic::transport::Server;
use tonic_health::ServingStatus;

pub mod proto {
    tonic::include_proto!("skyvault.v1");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("skyvault_descriptor");
}

mod cache_service;
pub mod config;
mod consistent_hashring;
pub mod dynamic_config;
mod forest;
pub mod jobs;
pub mod k8s;
mod k_way;
pub mod metadata;
pub mod observability;
pub mod orchestrator_service;
mod pod_watcher;
pub mod reader_service;
mod runs;
pub mod storage;
pub mod writer_service;

#[cfg(test)]
pub mod test_utils;

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

pub struct Builder {
    grpc_addr: SocketAddr,
    metadata: metadata::MetadataStore,
    storage: storage::ObjectStore,
    k8s_client: kube::Client,
    namespace: String,
    dynamic_config: dynamic_config::SharedAppConfig,

    enable_writer: bool,
    enable_reader: bool,
    enable_orchestrator: bool,
}

impl Builder {
    pub fn new(
        grpc_addr: SocketAddr,
        metadata: metadata::MetadataStore,
        storage: storage::ObjectStore,
        k8s_client: kube::Client,
        namespace: String,
        dynamic_config: dynamic_config::SharedAppConfig,
    ) -> Self {
        Self {
            grpc_addr,
            metadata,
            storage,
            k8s_client,
            namespace,
            dynamic_config,
            enable_writer: false,
            enable_reader: false,
            enable_orchestrator: false,
        }
    }

    pub fn with_writer(&mut self, enable: bool) -> &mut Self {
        self.enable_writer = enable;
        self
    }

    pub fn with_reader(&mut self, enable: bool) -> &mut Self {
        self.enable_reader = enable;
        self
    }

    pub fn with_orchestrator(&mut self, enable: bool) -> &mut Self {
        self.enable_orchestrator = enable;
        self
    }

    pub async fn start(&self) -> Result<(), ServerError> {
        let (health_reporter, health_service) = tonic_health::server::health_reporter();

        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
            .build_v1()
            .expect("Failed to build reflection service");

        let mut builder = Server::builder()
            .layer(observability::MetricsLayer)
            .add_service(reflection_service);

        if self.enable_writer {
            let writer = writer_service::MyWriter::new(
                self.metadata.clone(),
                self.storage.clone(),
                self.dynamic_config.clone(),
            );
            health_reporter
                .set_service_status(
                    proto::writer_service_server::SERVICE_NAME,
                    ServingStatus::Serving,
                )
                .await;

            builder = builder.add_service(proto::writer_service_server::WriterServiceServer::new(
                writer,
            ));
        }

        if self.enable_reader {
            let reader = reader_service::MyReader::new(
                self.metadata.clone(),
                self.storage.clone(),
                self.k8s_client.clone(),
                self.namespace.clone(),
                self.grpc_addr.port(),
            )
            .await?;
            health_reporter
                .set_service_status(
                    proto::reader_service_server::SERVICE_NAME,
                    ServingStatus::Serving,
                )
                .await;

            let cache = cache_service::MyCache::new(self.storage.clone()).await?;
            health_reporter
                .set_service_status(
                    proto::cache_service_server::SERVICE_NAME,
                    ServingStatus::Serving,
                )
                .await;

            builder = builder.add_service(proto::reader_service_server::ReaderServiceServer::new(
                reader,
            ));
            builder =
                builder.add_service(proto::cache_service_server::CacheServiceServer::new(cache));
        }

        if self.enable_orchestrator {
            let orchestrator = orchestrator_service::MyOrchestrator::new(
                self.metadata.clone(),
                self.storage.clone(),
                self.k8s_client.clone(),
                self.dynamic_config.clone(),
            )
            .await?;
            health_reporter
                .set_service_status(
                    proto::orchestrator_service_server::SERVICE_NAME,
                    ServingStatus::Serving,
                )
                .await;

            builder = builder.add_service(
                proto::orchestrator_service_server::OrchestratorServiceServer::new(orchestrator),
            );
        }

        builder
            .add_service(health_service)
            .serve_with_shutdown(self.grpc_addr, async {
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
}
