use std::collections::BTreeMap;

use futures::TryStreamExt;
use thiserror::Error;
use tonic::{Request, Response, Status};

use crate::dynamic_config::SharedAppConfig;
use crate::metadata::TableName;
use crate::proto::{self};
use crate::runs::{RunError as RunsError, RunId, WriteOperation};
use crate::{metadata, runs, storage};

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
}

struct WriteReq {
    ops: Vec<WriteOperation>,
    tx: tokio::sync::oneshot::Sender<Result<metadata::SeqNo, Status>>,
}

pub struct MyWriter {
    tx: tokio::sync::mpsc::Sender<WriteReq>,
    table_cache: metadata::TableCache,
}

impl MyWriter {
    #[must_use]
    pub fn new(
        metadata: metadata::MetadataStore,
        storage: storage::ObjectStore,
        dynamic_config: SharedAppConfig,
    ) -> Self {
        let table_cache = metadata::TableCache::new(metadata.clone());
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        // Clone dynamic_config for the background task
        let task_config = dynamic_config.clone();

        tokio::spawn(async move {
            Self::process_batch_queue(metadata, storage, rx, task_config).await;
        });

        Self { tx, table_cache }
    }

    #[cfg(test)]
    pub fn new_for_test(
        metadata: metadata::MetadataStore,
        storage: storage::ObjectStore,
        table_cache: metadata::TableCache,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let dynamic_config = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::dynamic_config::AppConfig::default(),
        ));

        // Clone for the background task
        let task_config = dynamic_config.clone();

        tokio::spawn(async move {
            Self::process_batch_queue(metadata, storage, rx, task_config).await;
        });

        Self { tx, table_cache }
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
            // Clone semaphore while holding read lock briefly, then release lock before acquiring
            // permit
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

        let mut ops: Vec<WriteOperation> = Vec::new();
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

        Ok(Response::new(proto::WriteBatchResponse {
            seq_no: seq_no.into(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::metadata::{
        MockMetadataStoreTrait, SeqNo, TableCache, TableConfig, TableID, TableName,
    };
    use crate::proto::writer_service_server::WriterService;
    use crate::requires_docker;
    use crate::storage::MockObjectStoreTrait;
    use crate::test_utils::{setup_test_db, setup_test_object_store};

    #[tokio::test]
    async fn test_writer_service() {
        requires_docker!();
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

        // Create a dummy dynamic config for testing
        let dynamic_config = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::dynamic_config::AppConfig::default(),
        ));

        let writer = MyWriter::new(metadata, storage, dynamic_config);

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

    #[tokio::test]
    async fn test_concurrent_batch_upload_limit() {
        let mut mock_meta = MockMetadataStoreTrait::new();
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
        mock_store
            .expect_put_run()
            .times(8)
            .returning(move |_id, _data| {
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

        let table_cache = TableCache::new_for_test(HashMap::from([(
            TableName::from("tbl".to_string()),
            (
                Some((TableID::from(1_i64), TableConfig::default())),
                Instant::now(),
            ),
        )]));

        let writer = MyWriter::new_for_test(Arc::new(mock_meta), Arc::new(mock_store), table_cache);

        let writer = Arc::new(writer);

        let mut handles = Vec::new();
        for i in 0..8 {
            let w = writer.clone();
            handles.push(tokio::spawn(async move {
                w.write_batch(Request::new(proto::WriteBatchRequest {
                    tables: vec![proto::TableWriteBatchRequest {
                        table_name: "tbl".to_string(),
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
