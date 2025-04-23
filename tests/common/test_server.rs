use std::net::{SocketAddr, TcpListener};
use std::sync::OnceLock;

use anyhow::Context;
use skyvault::{metadata, storage};
use tokio_retry::Retry;
use tokio_retry::strategy::FixedInterval;
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tracing::Level;
use tracing_subscriber::fmt;

pub struct ServerState {
    _temp_dir: tempfile::TempDir,
    pub addr: SocketAddr,
    server_handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl ServerState {
    pub async fn shutdown(self) {
        // Allow server to complete shutdown
        let _ = self.server_handle.abort();
    }
}

static TRACING_INIT: OnceLock<()> = OnceLock::new();

pub fn init_tracing() {
    TRACING_INIT.get_or_init(|| {
        fmt().with_max_level(Level::INFO).init();
    });
}

fn find_free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
    listener.local_addr().expect("Failed to get local address")
}

pub async fn start_test_server() -> ServerState {
    init_tracing();
    tracing::info!("Starting a new test server instance...");

    let temp_dir = tempfile::TempDir::new()
        .with_context(|| "Failed to create temporary directory")
        .unwrap();
    let metadata_path = temp_dir.path().join("metadata.db");
    let storage_path = temp_dir.path().join("storage");

    tracing::debug!("TempDir created at: {:?}", temp_dir.path());

    let addr = find_free_port();
    tracing::info!("Using address: {}", addr);

    let metadata = metadata::SqliteMetadataStore::new(metadata_path.to_str().unwrap())
        .with_context(|| "Failed to create metadata store")
        .unwrap();
    let storage = storage::LocalObjectStore::new(storage_path.to_str().unwrap())
        .with_context(|| "Failed to create storage")
        .unwrap();

    let metadata = std::sync::Arc::new(metadata);
    let storage = std::sync::Arc::new(storage);

    let server_metadata = std::sync::Arc::clone(&metadata);
    let server_storage = std::sync::Arc::clone(&storage);
    let server_addr = addr;

    let server_handle = tokio::spawn(async move {
        tracing::info!("Starting test server task on {}", server_addr);
        skyvault::server(server_addr, server_metadata, server_storage).await
    });

    tracing::info!(address = %addr, "Starting health check polling");
    let retry_strategy = FixedInterval::from_millis(100).take(50);

    let health_check_result = Retry::spawn(retry_strategy, || async {
        init_tracing();
        type BoxedError = Box<dyn std::error::Error + Send + Sync>;

        let endpoint = format!("http://{}", addr);
        let channel = match Channel::from_shared(endpoint.clone())
            .expect("valid uri")
            .connect()
            .await
        {
            Ok(channel) => channel,
            Err(e) => {
                tracing::warn!("Health check connection error: {}", e);
                return Err(Box::new(e) as BoxedError);
            },
        };

        let mut client = HealthClient::new(channel);
        let req = tonic::Request::new(HealthCheckRequest {
            service: "".to_string(),
        });

        match client.check(req).await {
            Ok(resp) => {
                let status = resp.get_ref().status;
                if status == ServingStatus::Serving as i32 {
                    tracing::info!("Health check successful, server is ready.");
                    Ok(())
                } else {
                    tracing::debug!("Server not ready yet, status: {:?}", status);
                    Err("Server not in Serving state".into())
                }
            },
            Err(e) => {
                tracing::warn!("Health check rpc error: {}", e);
                Err(Box::new(e) as BoxedError)
            },
        }
    })
    .await;

    if let Err(e) = health_check_result {
        tracing::error!(
            "Server failed to become healthy after multiple attempts: {}",
            e
        );
        panic!(
            "Server failed to become healthy after multiple attempts: {}",
            e
        );
    }

    tracing::info!("Server initialization complete.");

    ServerState {
        _temp_dir: temp_dir,
        addr,
        server_handle,
    }
}
