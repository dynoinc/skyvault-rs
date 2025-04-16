// Integration test for skyvault2 gRPC services

use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use skyvault::{metadata, storage};
use tokio::sync::OnceCell;
use tokio_retry::Retry;
use tokio_retry::strategy::FixedInterval;
use tonic::transport::Channel;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

static START_SERVER: OnceCell<()> = OnceCell::const_new();

async fn start_test_server() {
    START_SERVER
        .get_or_init(|| async {
            let addr: SocketAddr = "127.0.0.1:50551".parse().unwrap();

            let temp_dir = tempfile::TempDir::new().with_context(|| "Failed to create temporary directory").unwrap();
            let metadata = Arc::new(metadata::SqliteMetadataStore::new(temp_dir.path().join("metadata.db").to_str().unwrap()).with_context(|| "Failed to create metadata store").unwrap());
            let storage = Arc::new(storage::LocalObjectStore::new(temp_dir.path().join("storage").to_str().unwrap()).with_context(|| "Failed to create storage").unwrap());
            
            // Start the server using the lib.rs server function
            tokio::spawn(async move {
                println!("Starting test server on {}", addr);
                if let Err(e) = skyvault::server(addr, metadata, storage).await {
                    panic!("Failed to start server: {}", e);
                }
            });

            // Use tokio-retry for cleaner retry handling
            println!("Starting health check polling");

            // Configure retry strategy: try up to 30 times with 100ms between attempts
            let retry_strategy = FixedInterval::from_millis(100).take(30);

            let health_check = Retry::spawn(retry_strategy, || async {
                // Try to connect to the health service
                let channel = Channel::from_shared("http://127.0.0.1:50551".to_string())
                    .unwrap()
                    .connect()
                    .await
                    .map_err(|e| {
                        println!("Connection error: {}", e);
                        e
                    })?;

                let mut client = HealthClient::new(channel);
                let req = tonic::Request::new(HealthCheckRequest {
                    service: "".to_string(),
                });

                // Check if service is healthy
                let resp = client.check(req).await.map_err(|e| {
                    println!("Health check error: {}", e);
                    e
                })?;

                // If service is not serving, return an error to trigger retry
                if resp.get_ref().status != ServingStatus::Serving as i32 {
                    println!("Service not ready yet, status: {}", resp.get_ref().status);
                    return Err("Service not ready".into());
                }

                println!("Health check successful, server is ready");
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            })
            .await;

            // If we couldn't connect after all retries, panic
            if let Err(e) = health_check {
                panic!("Failed to connect to server after multiple attempts: {}", e);
            }
        })
        .await;
}

#[tokio::test]
async fn test_batcher_service() {
    start_test_server().await;

    let mut client = skyvault::BatcherClient::connect("http://127.0.0.1:50551")
        .await
        .expect("connect");
    let req = tonic::Request::new(skyvault::BatchRequest {});
    let _resp = client.batch_process(req).await.expect("batch_process");
}

#[tokio::test]
async fn test_index_service() {
    start_test_server().await;

    let mut client = skyvault::IndexClient::connect("http://127.0.0.1:50551")
        .await
        .expect("connect");
    let req = tonic::Request::new(skyvault::IndexRequest {});
    let _resp = client.index_document(req).await.expect("index_document");
}

#[tokio::test]
async fn test_orchestrator_service() {
    start_test_server().await;

    let mut client = skyvault::OrchestratorClient::connect("http://127.0.0.1:50551")
        .await
        .expect("connect");
    let req = tonic::Request::new(skyvault::OrchestrateRequest {});
    let _resp = client.orchestrate(req).await.expect("orchestrate");
}
