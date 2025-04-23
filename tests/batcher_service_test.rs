use skyvault::proto;

mod common;

#[tokio::test]
async fn test_batcher_service() {
    let server_state = common::test_server::start_test_server().await;
    let endpoint = format!("http://{}", server_state.addr);

    let mut client = skyvault::proto::batcher_client::BatcherClient::connect(endpoint)
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

    server_state.shutdown().await;
}
