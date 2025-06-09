use std::time::Duration;

use anyhow::Result;
use skyvault::proto;
use tokio::{
    process::Child,
    time::sleep,
};
use tonic::transport::Channel;

mod common;

struct TestStubs {
    writer: proto::writer_service_client::WriterServiceClient<Channel>,
    reader: proto::reader_service_client::ReaderServiceClient<Channel>,
    orchestrator: proto::orchestrator_service_client::OrchestratorServiceClient<Channel>,

    _writer_child: Child,
    _reader_child: Child,
    _orchestrator_child: Child,
}

async fn setup_stubs() -> Result<TestStubs> {
    let (
        (writer_channel, writer_child),
        (reader_channel, reader_child),
        (orchestrator_channel, orchestrator_child),
    ) = tokio::try_join!(
        common::setup_connection("skyvault-writer"),
        common::setup_connection("skyvault-reader"),
        common::setup_connection("skyvault-orchestrator")
    )?;

    Ok(TestStubs {
        writer: proto::writer_service_client::WriterServiceClient::new(writer_channel),
        reader: proto::reader_service_client::ReaderServiceClient::new(reader_channel),
        orchestrator: proto::orchestrator_service_client::OrchestratorServiceClient::new(orchestrator_channel),

        _writer_child: writer_child,
        _reader_child: reader_child,
        _orchestrator_child: orchestrator_child,
    })
}

async fn create_table(
    orchestrator: &mut proto::orchestrator_service_client::OrchestratorServiceClient<Channel>,
    table_name: &str,
) -> Result<Option<i64>> {
    let request = proto::CreateTableRequest {
        config: Some(proto::TableConfig {
            table_id: 0, // This will be assigned by the server
            table_name: table_name.to_string(),
        }),
    };

    match orchestrator.create_table(request).await {
        Ok(response) => Ok(Some(response.into_inner().seq_no)),
        Err(e) if e.code() == tonic::Code::AlreadyExists => Ok(None),
        Err(e) => Err(e.into()),
    }
}

async fn perform_write(
    writer: &mut proto::writer_service_client::WriterServiceClient<Channel>,
    table_name: &str,
    key: &str,
    value_bytes: Vec<u8>,
) -> Result<i64> {
    let write_item = proto::WriteBatchItem {
        key: key.to_string(),
        operation: Some(proto::write_batch_item::Operation::Value(value_bytes)),
    };

    let table_request = proto::TableWriteBatchRequest {
        table_name: table_name.to_string(),
        items: vec![write_item],
    };

    let request = proto::WriteBatchRequest {
        tables: vec![table_request],
    };

    // Retry up to 5 times with 1 second delay
    for _ in 0..5 {
        match writer.write_batch(request.clone()).await {
            Ok(response) => return Ok(response.into_inner().seq_no),
            Err(e) if e.code() == tonic::Code::NotFound => {
                sleep(Duration::from_millis(100)).await;
                continue;
            },
            Err(e) => return Err(e.into()),
        }
    }

    Err(anyhow::anyhow!("Write failed after retries"))
}

async fn perform_read_with_retry(
    reader: &mut proto::reader_service_client::ReaderServiceClient<Channel>,
    table_name: &str,
    _seq_no: i64,
    key: &str,
    expected_value_bytes: &[u8],
    retries: u32,
    delay: Duration,
) -> Result<bool> {
    let table_request = proto::TableGetBatchRequest {
        table_name: table_name.to_string(),
        keys: vec![key.to_string()],
    };

    let request = proto::GetBatchRequest {
        seq_no: 0, // Use 0 to get latest data
        tables: vec![table_request],
    };

    for _ in 0..retries {
        match reader.get_batch(request.clone()).await {
            Ok(response) => {
                let response_inner = response.into_inner();
                if let Some(table_response) = response_inner.tables.first() {
                    if table_response.table_name == table_name {
                        for item in &table_response.items {
                            if item.key == key && item.value == expected_value_bytes {
                                return Ok(true);
                            }
                        }
                    }
                }
            },
            Err(e) if e.code() == tonic::Code::FailedPrecondition => {
                // Continue retrying for seq_no mismatch
            },
            Err(e) => return Err(e.into()),
        }

        sleep(delay).await;
    }

    Ok(false)
}

async fn trigger_wal_compaction(
    orchestrator: &mut proto::orchestrator_service_client::OrchestratorServiceClient<Channel>,
) -> Result<i64> {
    let request = proto::KickOffJobRequest {
        params: Some(proto::JobParams {
            params: Some(proto::job_params::Params::WalCompaction(())),
        }),
    };

    let response = orchestrator.kick_off_job(request).await?;
    let job_id = response.into_inner().job_id;

    // Wait for compaction job to complete with timeout
    let start_time = std::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(5) {
        let status_request = proto::GetJobStatusRequest { job_id };
        let status_response = orchestrator.get_job_status(status_request).await?;
        let status = status_response.into_inner().status;

        if let Some(status) = status {
            match status.status {
                Some(proto::job_status::Status::Pending(_)) => {
                    sleep(Duration::from_millis(500)).await;
                    continue;
                },
                Some(proto::job_status::Status::SeqNo(seq_no)) => {
                    return Ok(seq_no);
                },
                Some(proto::job_status::Status::Failed(_)) => {
                    return Err(anyhow::anyhow!("Compaction job failed"));
                },
                None => {
                    return Err(anyhow::anyhow!("Invalid job status"));
                },
            }
        }
    }

    Err(anyhow::anyhow!("Compaction job did not complete within 5 seconds"))
}

async fn persist_snapshot(
    orchestrator: &mut proto::orchestrator_service_client::OrchestratorServiceClient<Channel>,
) -> Result<i64> {
    let request = proto::PersistSnapshotRequest {};
    let response = orchestrator.persist_snapshot(request).await?;
    Ok(response.into_inner().seq_no)
}

#[tokio::test]
#[ignore]
async fn test_simple_write_and_read() -> Result<()> {
    let mut stubs = setup_stubs().await?;

    let table_name = "test_simple_write_and_read";
    let key = "test_simple_write_and_read_key";
    let value = b"test_simple_write_and_read_value".to_vec();

    create_table(&mut stubs.orchestrator, table_name).await?;

    // Write key-value pair
    let seq_no = perform_write(&mut stubs.writer, table_name, key, value.clone()).await?;

    // Verify read works
    let read_success = perform_read_with_retry(
        &mut stubs.reader,
        table_name,
        seq_no,
        key,
        &value,
        30,
        Duration::from_millis(100),
    )
    .await?;

    assert!(read_success, "Failed to read back key '{key}' after writing");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_write_compact_read() -> Result<()> {
    let mut stubs = setup_stubs().await?;

    let table_name = "test_write_compact_read";
    let key_one = "test_write_compact_read_key_one";
    let value_one = b"test_write_compact_read_value_one".to_vec();
    let key_two = "test_write_compact_read_key_two";
    let value_two = b"test_write_compact_read_value_two".to_vec();

    create_table(&mut stubs.orchestrator, table_name).await?;

    // Write both keys
    let _seq_no = perform_write(&mut stubs.writer, table_name, key_one, value_one.clone()).await?;
    let seq_no = perform_write(&mut stubs.writer, table_name, key_two, value_two.clone()).await?;

    // Verify second key is readable before compaction
    let read_success = perform_read_with_retry(
        &mut stubs.reader,
        table_name,
        seq_no,
        key_two,
        &value_two,
        30,
        Duration::from_millis(100),
    )
    .await?;

    assert!(read_success, "Failed to read back key '{key_two}' before compaction");

    // Trigger compaction
    let seq_no = trigger_wal_compaction(&mut stubs.orchestrator).await?;

    // Verify second key is still readable after compaction
    let read_success = perform_read_with_retry(
        &mut stubs.reader,
        table_name,
        seq_no,
        key_two,
        &value_two,
        30,
        Duration::from_millis(100),
    )
    .await?;

    assert!(read_success, "Failed to read back key '{key_two}' after compaction");

    // Verify first key is also still readable after compaction
    let read_success = perform_read_with_retry(
        &mut stubs.reader,
        table_name,
        seq_no,
        key_one,
        &value_one,
        30,
        Duration::from_millis(100),
    )
    .await?;

    assert!(read_success, "Failed to read back key '{key_one}' after compaction");

    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_snapshot_persistence() -> Result<()> {
    let mut stubs = setup_stubs().await?;

    let table_name = "test_snapshot_persistence";
    let key = "test_snapshot_persistence_key";
    let value = b"test_snapshot_persistence_value".to_vec();

    create_table(&mut stubs.orchestrator, table_name).await?;

    // Write some data
    let seq_no = perform_write(&mut stubs.writer, table_name, key, value.clone()).await?;

    // Trigger snapshot persistence
    persist_snapshot(&mut stubs.orchestrator).await?;

    // Verify snapshot is persisted
    let read_success = perform_read_with_retry(
        &mut stubs.reader,
        table_name,
        seq_no,
        key,
        &value,
        30,
        Duration::from_millis(100),
    )
    .await?;

    assert!(
        read_success,
        "Failed to read back key '{key}' after snapshot persistence"
    );

    Ok(())
}
