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
    metadata::TableName,
    proto::{
        self,
    },
    runs,
    runs::{
        RunError as RunsError,
        RunId,
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
        let forest = ForestImpl::latest(metadata.clone(), storage.clone()).await?;

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
        tracing::info!("Writer service started with dynamic concurrent uploads limit");

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
                config.uploads_semaphore.clone()
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
        let wal_runs: Vec<_> = runs::build_runs(ops_stream).try_collect().await?;

        let mut wal_run_ids = Vec::new();
        for (run_data, stats) in wal_runs {
            let run_id = RunId(ulid::Ulid::new().to_string());
            storage.put_run(run_id.clone(), run_data).await?;
            wal_run_ids.push((run_id, stats));
        }

        let seq_no = metadata.append_wal(wal_run_ids).await?;
        Ok(seq_no)
    }
}

#[tonic::async_trait]
impl proto::writer_service_server::WriterService for MyWriter {
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
            if table.table_name.is_empty() {
                return Err(Status::invalid_argument("Table name cannot be empty"));
            }

            for item in table.items.drain(..) {
                let table_id = forest_state
                    .tables
                    .get(&TableName::from(table.table_name.clone()))
                    .and_then(|t| t.table_id)
                    .ok_or_else(|| Status::not_found("Table not found"))?;
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
    use std::{
        sync::{
            Arc,
            atomic::{
                AtomicUsize,
                Ordering,
            },
        },
        time::Duration,
    };

    use super::*;
    use crate::{
        metadata::{
            ChangelogEntry,
            ChangelogEntryWithID,
            MockMetadataStoreTrait,
            SeqNo,
            TableChangelogEntryV1,
            TableConfig,
            TableID,
            TableName,
        },
        proto::writer_service_server::WriterService,
        requires_docker,
        storage::MockObjectStoreTrait,
        test_utils::{
            setup_test_db,
            setup_test_object_store,
        },
    };

    #[tokio::test]
    async fn test_writer_service() {
        requires_docker!();
        let (metadata, _postgres) = setup_test_db().await.unwrap();
        let (storage, _minio) = setup_test_object_store().await.unwrap();

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
        assert_eq!(seq_no, 2); // 1 is used by create_table
    }

    #[tokio::test]
    async fn test_concurrent_batch_upload_limit() {
        let test_table_name = "test_table";
        let mut mock_meta = MockMetadataStoreTrait::new();
        mock_meta.expect_get_latest_snapshot().times(1).returning(|| {
            Box::pin(async {
                Ok((
                    None,
                    vec![ChangelogEntryWithID {
                        id: SeqNo::from(1),
                        changes: ChangelogEntry::TablesV1(TableChangelogEntryV1::TableCreated(TableID::from(1))),
                    }],
                ))
            })
        });
        mock_meta.expect_get_table_by_id().times(1).returning(|_id| {
            Box::pin(async {
                Ok(TableConfig {
                    table_id: Some(TableID::from(1)),
                    table_name: TableName::from(test_table_name.to_string()),
                })
            })
        });
        mock_meta
            .expect_append_wal()
            .times(8)
            .returning(|_runs| Box::pin(async { Ok(SeqNo::from(1)) }));

        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let delay = Duration::from_millis(1000);

        let mut mock_store = MockObjectStoreTrait::new();
        let active_clone = active.clone();
        let max_clone = max_active.clone();
        mock_store.expect_put_run().times(8).returning(move |_id, _data| {
            let active = active_clone.clone();
            let max = max_clone.clone();
            Box::pin(async move {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                loop {
                    let prev = max.load(Ordering::SeqCst);
                    if current > prev {
                        if max
                            .compare_exchange(prev, current, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                        {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                tokio::time::sleep(delay).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            })
        });

        let dynamic_config = std::sync::Arc::new(tokio::sync::RwLock::new(crate::dynamic_config::AppConfig::default()));

        let writer = MyWriter::new(Arc::new(mock_meta), Arc::new(mock_store), dynamic_config)
            .await
            .unwrap();

        let writer = Arc::new(writer);

        let mut handles = Vec::new();
        for i in 0..8 {
            let w = writer.clone();
            handles.push(tokio::spawn(async move {
                w.write_batch(Request::new(proto::WriteBatchRequest {
                    tables: vec![proto::TableWriteBatchRequest {
                        table_name: test_table_name.to_string(),
                        items: vec![proto::WriteBatchItem {
                            key: format!("key{i}"),
                            operation: Some(proto::write_batch_item::Operation::Value(vec![1])),
                        }],
                    }],
                }))
                .await
                .unwrap();
            }));
            tokio::time::sleep(Duration::from_millis(260)).await;
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(max_active.load(Ordering::SeqCst), 4);
    }
}
