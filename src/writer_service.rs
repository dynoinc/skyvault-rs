use std::collections::BTreeMap;

use futures::TryStreamExt;
use thiserror::Error;
use tonic::{
    Request,
    Response,
    Status,
};

use crate::{
    dynamic_config::SharedAppConfig,
    forest::{
        self,
        Forest,
        ForestImpl,
    },
    metadata,
    metadata::{
        ChangelogEntry,
        TableName,
    },
    proto::{
        self,
    },
    runs,
    runs::{
        RunError as RunsError,
        RunID,
        WriteOperation,
    },
    storage,
};

#[derive(Error, Debug)]
pub enum WriterServiceError {
    #[error("Metadata error: {0}")]
    MetadataError(#[from] metadata::MetadataError),

    #[error("Storage error: {0}")]
    StorageError(#[from] storage::StorageError),

    #[error("Run error: {0}")]
    RunError(#[from] RunsError),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Forest error: {0}")]
    ForestError(#[from] forest::ForestError),
}

struct WriteReq {
    ops: Vec<WriteOperation>,
    tx: tokio::sync::oneshot::Sender<Result<metadata::SeqNo, Status>>,
}

pub struct MyWriter {
    tx: tokio::sync::mpsc::Sender<WriteReq>,
    forest: Forest,
}

impl MyWriter {
    pub async fn new(
        metadata: metadata::MetadataStore,
        storage: storage::ObjectStore,
        dynamic_config: SharedAppConfig,
    ) -> Result<Self, WriterServiceError> {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let forest = ForestImpl::watch(metadata.clone(), storage.clone(), |stream| {
            stream.try_filter_map(|mut entry| async move {
                entry.retain(|e| matches!(e.changes, ChangelogEntry::TablesV1(_)));
                if !entry.is_empty() { Ok(Some(entry)) } else { Ok(None) }
            })
        })
        .await?;

        let task_config = dynamic_config.clone();
        tokio::spawn(async move {
            Self::process_batch_queue(metadata, storage, rx, task_config).await;
        });

        Ok(Self { tx, forest })
    }

    async fn process_batch_queue(
        metadata: metadata::MetadataStore,
        storage: storage::ObjectStore,
        mut rx: tokio::sync::mpsc::Receiver<WriteReq>,
        dynamic_config: SharedAppConfig,
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

            // Get a permit from the dynamic config's semaphore
            // Clone semaphore while holding read lock briefly, then release lock before
            // acquiring permit
            let semaphore = {
                let config = dynamic_config.read().await;
                config.writer_uploads_semaphore.clone()
            }; // read lock is released here

            // Now acquire the permit without holding any locks on the config
            let permit = semaphore.acquire_owned().await.unwrap();

            let metadata_clone = metadata.clone();
            let storage_clone = storage.clone();

            tokio::spawn(async move {
                let result = Self::process_batch(&metadata_clone, &storage_clone, ops).await;
                match result {
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
                // Drop the permit when done to release the semaphore slot
                drop(permit);
            });
        }

        tracing::info!("Batch queue closed");
    }

    async fn process_batch(
        metadata: &metadata::MetadataStore,
        storage: &storage::ObjectStore,
        batch: Vec<Vec<WriteOperation>>,
    ) -> Result<metadata::SeqNo, WriterServiceError> {
        let sorted_ops = batch
            .into_iter()
            .flat_map(|req| req.into_iter())
            .map(|op| (op.key().to_string(), op))
            .collect::<BTreeMap<_, _>>();

        let ops_stream = futures::stream::iter(sorted_ops.into_values().map(Ok));
        let wal_runs: Vec<_> = runs::build_runs(ops_stream, 4 * 1024 * 1024).try_collect().await?;

        let mut wal_run_ids = Vec::new();
        for (run_data, stats) in wal_runs {
            let run_id = RunID(ulid::Ulid::new().to_string());
            storage.put_run(run_id.clone(), run_data).await?;
            wal_run_ids.push((run_id, stats));
        }

        let seq_no = metadata.append_wal(wal_run_ids).await?;
        Ok(seq_no)
    }
}

#[tonic::async_trait]
impl proto::writer_service_server::WriterService for MyWriter {
    #[tracing::instrument(skip(self, req))]
    async fn write_batch(
        &self,
        req: Request<proto::WriteBatchRequest>,
    ) -> Result<Response<proto::WriteBatchResponse>, Status> {
        if req.get_ref().tables.is_empty() {
            return Err(Status::invalid_argument("No writes provided"));
        }

        let forest_state = self.forest.get_state();

        let mut ops: Vec<WriteOperation> = Vec::new();
        for mut table in req.into_inner().tables.drain(..) {
            let table_name = TableName::from(table.table_name.clone());
            let table_id = forest_state
                .tables
                .get(&table_name)
                .and_then(|t| t.table_id)
                .ok_or_else(|| Status::not_found(format!("Table not found: {table_name}")))?;
            for item in table.items.drain(..) {
                let key = format!("{}.{}", table_id, item.key);
                match item.operation {
                    Some(proto::write_batch_item::Operation::Value(value)) => {
                        ops.push(WriteOperation::Put(key, value));
                    },
                    None => {
                        ops.push(WriteOperation::Delete(key));
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

        Ok(Response::new(proto::WriteBatchResponse { seq_no: seq_no.into() }))
    }
}

#[cfg(test)]
mod tests {
    use tokio_retry::{
        RetryIf,
        strategy::ExponentialBackoff,
    };

    use super::*;
    use crate::{
        metadata::{
            TableConfig,
            TableID,
            TableName,
        },
        proto::writer_service_server::WriterService,
        requires_docker,
        test_utils::{
            setup_test_db,
            setup_test_object_store,
        },
    };

    #[tokio::test]
    async fn test_writer_service() {
        requires_docker!();
        let metadata = setup_test_db().await.unwrap();
        let storage = setup_test_object_store().await.unwrap();

        // Create a table
        let table_name = "test_table";
        metadata
            .create_table(TableConfig {
                table_id: Some(TableID::from(1)),
                table_name: TableName::from(table_name.to_string()),
            })
            .await
            .unwrap();

        let dynamic_config = std::sync::Arc::new(tokio::sync::RwLock::new(crate::dynamic_config::AppConfig::default()));

        let writer = MyWriter::new(metadata, storage, dynamic_config).await.unwrap();

        let retry_strategy = ExponentialBackoff::from_millis(1000).take(3);

        let response = RetryIf::spawn(
            retry_strategy,
            || async {
                writer
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
            },
            |e: &Status| e.code() == tonic::Code::NotFound,
        )
        .await
        .unwrap();

        let seq_no = response.into_inner().seq_no;
        assert_eq!(seq_no, 2); // 1 is used by create_table
    }
}
