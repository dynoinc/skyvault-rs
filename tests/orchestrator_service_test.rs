mod common;

#[tokio::test]
async fn test_orchestrator_service() {
    let server_state = common::test_server::start_test_server().await;
    let endpoint = format!("http://{}", server_state.addr);

    let mut client = skyvault::proto::orchestrator_client::OrchestratorClient::connect(endpoint)
        .await
        .expect("connect");

    let req = tonic::Request::new(skyvault::proto::OrchestrateRequest::default());
    tracing::debug!("Sending Orchestrate request");
    let _resp = client.orchestrate(req).await.expect("orchestrate");
    tracing::info!("Orchestrate request successful");
    tracing::info!("Finished test: test_orchestrator_service");

    server_state.shutdown().await;
}
