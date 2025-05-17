use std::collections::BTreeMap;

use futures::TryStreamExt;
use thiserror::Error;
use tonic::{Request, Response, Status};

use crate::metadata::TableName;
use crate::proto::writer_service_server::WriterService;
use crate::proto::{self};
use crate::{metadata, storage};

#[derive(Error, Debug)]
pub enum WriterServiceError {
    #[error("Metadata error: {0}")]
    MetadataError(#[from] metadata::MetadataError),

    #[error("Storage error: {0}")]
    StorageError(#[from] storage::StorageError),

    #[error("Run error: {0}")]
    RunError(#[from] crate::runs::RunError),

    #[error("Internal error: {0}")]
    Internal(String),
}

struct WriteReq {
    ops: Vec<crate::runs::WriteOperation>,
    tx: tokio::sync::oneshot::Sender<Result<metadata::SeqNo, Status>>,
}

pub struct MyWriter {
    tx: tokio::sync::mpsc::Sender<WriteReq>,
    table_cache: metadata::TableCache,
}

impl MyWriter {
    #[must_use]
    pub fn new(metadata: metadata::MetadataStore, storage: storage::ObjectStore) -> Self {
        let table_cache = metadata::TableCache::new(metadata.clone());
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            Self::process_batch_queue(metadata, storage, rx).await;
        });

        Self { tx, table_cache }
    }

    async fn process_batch_queue(
        metadata: metadata::MetadataStore,
        storage: storage::ObjectStore,
        mut rx: tokio::sync::mpsc::Receiver<WriteReq>,
    ) {
        while let Some(item) = rx.recv().await {
            let mut ops_count = item.ops.len();
            let mut ops = vec![item.ops];
            let mut tx = vec![item.tx];

            let timeout = tokio::time::sleep(std::time::Duration::from_millis(250));
            tokio::pin!(timeout);

            loop {
                tokio::select! {
                    biased;

                    maybe_item = rx.recv() => {
                        match maybe_item {
                            Some(item) => {
                                ops_count += item.ops.len();
                                ops.push(item.ops);
                                tx.push(item.tx);

                                // If we've collected enough items, process the batch immediately
                                if ops_count >= 50_000 {
                                    break;
                                }
                            },
                            None => {
                                break;
                            }
                        }
                    },

                    () = &mut timeout => {
                        break;
                    }
                }
            }

            match Self::process_batch(&metadata, &storage, ops).await {
                Ok(seq_no) => {
                    for item in tx.drain(..) {
                        let _ = item.send(Ok(seq_no));
                    }
                },
                Err(e) => {
                    let status = Status::internal(e.to_string());
                    for item in tx.drain(..) {
                        let _ = item.send(Err(status.clone()));
                    }
                },
            }
        }

        tracing::info!("Batch queue closed");
    }

    async fn process_batch(
        metadata: &metadata::MetadataStore,
        storage: &storage::ObjectStore,
        batch: Vec<Vec<crate::runs::WriteOperation>>,
    ) -> Result<metadata::SeqNo, WriterServiceError> {
        let sorted_ops = batch
            .into_iter()
            .flat_map(|req| req.into_iter())
            .map(|op| (op.key().to_string(), op))
            .collect::<BTreeMap<_, _>>();

        let ops_stream = futures::stream::iter(sorted_ops.into_values().map(Ok));
        let wal_runs: Vec<_> = crate::runs::build_runs(ops_stream).try_collect().await?;

        let mut wal_run_ids = Vec::new();
        for (run_data, stats) in wal_runs {
            let run_id = crate::runs::RunId(ulid::Ulid::new().to_string());
            storage.put_run(run_id.clone(), run_data).await?;
            wal_run_ids.push((run_id, stats));
        }

        let seq_no = metadata.append_wal(wal_run_ids).await?;
        Ok(seq_no)
    }
}

#[tonic::async_trait]
impl WriterService for MyWriter {
    async fn write_batch(
        &self,
        req: Request<proto::WriteBatchRequest>,
    ) -> Result<Response<proto::WriteBatchResponse>, Status> {
        if req.get_ref().tables.is_empty() {
            return Err(Status::invalid_argument("No writes provided"));
        }

        let mut ops: Vec<crate::runs::WriteOperation> = Vec::new();
        for mut table in req.into_inner().tables.drain(..) {
            if table.table_name.is_empty() || table.table_name.contains(".") {
                return Err(Status::invalid_argument(
                    "Table name cannot be empty or contain a dot",
                ));
            }

            for item in table.items.drain(..) {
                let table_id = self
                    .table_cache
                    .get_table_id(TableName::from(table.table_name.clone()))
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                let key = format!("{}.{}", table_id, item.key);
                match item.operation {
                    Some(proto::write_batch_item::Operation::Value(value)) => {
                        ops.push(crate::runs::WriteOperation::Put(key, value));
                    },
                    None => {
                        ops.push(crate::runs::WriteOperation::Delete(key));
                    },
                }
            }
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(WriteReq { ops, tx })
            .await
            .map_err(|e| Status::internal(format!("Failed to send batch: {e}")))?;

        let seq_no = rx
            .await
            .map_err(|e| Status::internal(format!("Failed to receive batch result: {e}")))??;

        Ok(Response::new(proto::WriteBatchResponse {
            seq_no: seq_no.into(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::TableConfig;
    use crate::test_utils::{docker_is_available, setup_test_db, setup_test_object_store};

    #[tokio::test]
    async fn test_writer_service() {
        if !docker_is_available() {
            eprintln!("Docker not running - skipping test");
            return;
        }
        let (metadata, _postgres) = setup_test_db().await.unwrap();
        let (storage, _minio) = setup_test_object_store().await.unwrap();

        // Create a table
        let table_name = "test_table";
        metadata
            .create_table(
                TableName::from(table_name.to_string()),
                TableConfig::default(),
            )
            .await
            .unwrap();

        let writer = MyWriter::new(metadata, storage);

        let response = writer
            .write_batch(Request::new(proto::WriteBatchRequest {
                tables: vec![proto::TableWriteBatchRequest {
                    table_name: table_name.to_string(),
                    items: vec![proto::WriteBatchItem {
                        key: "test".to_string(),
                        operation: Some(proto::write_batch_item::Operation::Value(vec![1, 2, 3])),
                    }],
                }],
            }))
            .await
            .unwrap();

        let seq_no = response.into_inner().seq_no;
        assert_eq!(seq_no, 1);
    }
}
