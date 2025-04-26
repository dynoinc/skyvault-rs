use thiserror::Error;
use tonic::{Request, Response, Status};

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
    tx: tokio::sync::oneshot::Sender<Result<(), Status>>,
}

pub struct MyWriter {
    tx: tokio::sync::mpsc::Sender<WriteReq>,
}

impl MyWriter {
    #[must_use]
    pub fn new(metadata: metadata::MetadataStore, storage: storage::ObjectStore) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            Self::process_batch_queue(metadata, storage, rx).await;
        });

        Self { tx }
    }

    async fn process_batch_queue(
        metadata: metadata::MetadataStore,
        storage: storage::ObjectStore,
        mut rx: tokio::sync::mpsc::Receiver<WriteReq>,
    ) {
        while let Some(item) = rx.recv().await {
            let mut ops_count = item.ops.len();
            let mut buffer = vec![item];

            let timeout = tokio::time::sleep(std::time::Duration::from_millis(250));
            tokio::pin!(timeout);

            loop {
                tokio::select! {
                    biased;

                    maybe_item = rx.recv() => {
                        match maybe_item {
                            Some(item) => {
                                ops_count += item.ops.len();
                                buffer.push(item);

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

            let result = Self::process_batch(&metadata, &storage, &mut buffer).await;
            let result = result.map_err(|e| Status::internal(e.to_string()));
            for item in buffer.drain(..) {
                let _ = item.tx.send(result.clone());
            }
        }

        tracing::info!("Batch queue closed");
    }

    async fn process_batch(
        metadata: &metadata::MetadataStore,
        storage: &storage::ObjectStore,
        batch: &mut [WriteReq],
    ) -> Result<(), WriterServiceError> {
        let (run, stats) =
            crate::runs::create_run(batch.iter_mut().flat_map(|req| req.ops.drain(..)))?;

        let run_id = ulid::Ulid::new().to_string();
        storage.put_run(&run_id, run).await?;
        metadata.append_wal(run_id, stats).await?;
        Ok(())
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
            if table.table_name.is_empty() {
                return Err(Status::invalid_argument("Table name cannot be empty"));
            }

            for item in table.items.drain(..) {
                let key = format!("{}.{}", table.table_name, item.key);
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

        rx.await
            .map_err(|e| Status::internal(format!("Failed to receive batch result: {e}")))??;

        Ok(Response::new(proto::WriteBatchResponse {}))
    }
}
