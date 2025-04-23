mod common;

#[tokio::test]
async fn test_index_service() {
    let server_state = common::test_server::start_test_server().await;
    let endpoint = format!("http://{}", server_state.addr);

    let mut client = skyvault::proto::index_client::IndexClient::connect(endpoint)
        .await
        .expect("connect");

    let req = tonic::Request::new(skyvault::proto::IndexRequest::default());
    tracing::debug!("Sending IndexDocument request");
    let _resp = client.index_document(req).await.expect("index_document");
    tracing::info!("IndexDocument request successful");
    tracing::info!("Finished test: test_index_service");

    server_state.shutdown().await;
}
