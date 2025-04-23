use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;

use anyhow::Context;
use skyvault::{metadata, storage, proto};
use tokio::sync::OnceCell;
use tokio_retry::Retry;
use tokio_retry::strategy::FixedInterval;
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tracing::Level;
use tracing_subscriber::fmt;

struct ServerState {
    _temp_dir: tempfile::TempDir,
}

static SERVER_STATE: OnceCell<Arc<ServerState>> = OnceCell::const_new();
static TRACING_INIT: OnceLock<()> = OnceLock::new();

fn init_tracing() {
    TRACING_INIT.get_or_init(|| {
        fmt()
            .with_max_level(Level::INFO)
            .init();
    });
}

async fn start_test_server() -> Arc<ServerState> {
    init_tracing();
    tracing::info!("Attempting to start test server...");

    SERVER_STATE
        .get_or_init(|| async {
            init_tracing();
            tracing::info!("Initializing test server state...");

            let temp_dir = tempfile::TempDir::new()
                .with_context(|| "Failed to create temporary directory")
                .unwrap();
            let metadata_path = temp_dir.path().join("metadata.db");
            let storage_path = temp_dir.path().join("storage");

            tracing::debug!("TempDir created at: {:?}", temp_dir.path());

            let addr: SocketAddr = "127.0.0.1:50551".parse().unwrap();

            let metadata = Arc::new(
                metadata::SqliteMetadataStore::new(metadata_path.to_str().unwrap())
                    .with_context(|| "Failed to create metadata store")
                    .unwrap(),
            );
            let storage = Arc::new(
                storage::LocalObjectStore::new(storage_path.to_str().unwrap())
                    .with_context(|| "Failed to create storage")
                    .unwrap(),
            );

            let server_metadata = Arc::clone(&metadata);
            let server_storage = Arc::clone(&storage);

            tokio::spawn(async move {
                tracing::info!("Starting test server task on {}", addr);
                if let Err(e) = skyvault::server(addr, server_metadata, server_storage).await {
                    tracing::error!("Test server task failed: {}", e);
                } else {
                    tracing::info!("Test server task exited cleanly.");
                }
            });

            tracing::info!(address = %addr, "Starting health check polling");
            let retry_strategy = FixedInterval::from_millis(100).take(50);

            let health_check_result = Retry::spawn(retry_strategy, || async {
                init_tracing();
                type BoxedError = Box<dyn std::error::Error + Send + Sync>;

                let channel = match Channel::from_static("http://127.0.0.1:50551").connect().await {
                    Ok(channel) => channel,
                    Err(e) => {
                        tracing::warn!("Health check connection error: {}", e);
                        return Err(Box::new(e) as BoxedError);
                    }
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
                    }
                    Err(e) => {
                        tracing::warn!("Health check rpc error: {}", e);
                        Err(Box::new(e) as BoxedError)
                    }
                }
            })
            .await;

            if let Err(e) = health_check_result {
                tracing::error!("Server failed to become healthy after multiple attempts: {}", e);
                panic!(
                    "Server failed to become healthy after multiple attempts: {}",
                    e
                );
            }

            tracing::info!("Server initialization complete.");
            Arc::new(ServerState { _temp_dir: temp_dir })
        })
        .await
        .clone()
}

#[tokio::test]
async fn test_batcher_service() {
    init_tracing();
    tracing::info!("Starting test: test_batcher_service");
    let _server_state = start_test_server().await;
    tracing::info!("Test server ready for: test_batcher_service");

    let mut client = skyvault::proto::batcher_client::BatcherClient::connect("http://127.0.0.1:50551")
        .await
        .expect("connect");
    let req = tonic::Request::new(skyvault::proto::WriteBatchRequest {
        tables: vec![proto::TableBatch {
            table_name: "test-table".to_string(),
            items: vec![proto::WriteBatchItem {
                key: "test-key".to_string(),
                operation: Some(proto::write_batch_item::Operation::Value(vec![1, 2, 3])),
            }],
        }],
    });
    tracing::debug!("Sending WriteBatch request");
    let _resp = client.write_batch(req).await.expect("write_batch");
    tracing::info!("WriteBatch request successful");
    tracing::info!("Finished test: test_batcher_service");
}

#[tokio::test]
async fn test_index_service() {
    init_tracing();
    tracing::info!("Starting test: test_index_service");
    let _server_state = start_test_server().await;
    tracing::info!("Test server ready for: test_index_service");

    let mut client = skyvault::proto::index_client::IndexClient::connect("http://127.0.0.1:50551")
        .await
        .expect("connect");
    let req = tonic::Request::new(skyvault::proto::IndexRequest::default());
    tracing::debug!("Sending IndexDocument request");
    let _resp = client.index_document(req).await.expect("index_document");
    tracing::info!("IndexDocument request successful");
    tracing::info!("Finished test: test_index_service");
}

#[tokio::test]
async fn test_orchestrator_service() {
    init_tracing();
    tracing::info!("Starting test: test_orchestrator_service");
    let _server_state = start_test_server().await;
    tracing::info!("Test server ready for: test_orchestrator_service");

    let mut client = skyvault::proto::orchestrator_client::OrchestratorClient::connect("http://127.0.0.1:50551")
        .await
        .expect("connect");
    let req = tonic::Request::new(skyvault::proto::OrchestrateRequest::default());
    tracing::debug!("Sending Orchestrate request");
    let _resp = client.orchestrate(req).await.expect("orchestrate");
    tracing::info!("Orchestrate request successful");
    tracing::info!("Finished test: test_orchestrator_service");
}
