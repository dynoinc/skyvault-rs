use std::sync::Arc;

use thiserror::Error;
use tonic::{Request, Response, Status};

use crate::{Batcher, metadata, storage, proto};

#[derive(Error, Debug)]
pub enum BatchError {
    #[error("Metadata error: {0}")]
    MetadataError(#[from] metadata::MetadataError),

    #[error("Storage error: {0}")]
    StorageError(#[from] storage::StorageError),

    #[error("Internal error: {0}")]
    Internal(String),
}

struct Item {
    req: proto::WriteBatchRequest,
    tx: tokio::sync::oneshot::Sender<Result<(), Status>>,
}

pub struct MyBatcher {
    tx: tokio::sync::mpsc::Sender<Item>,
}

impl MyBatcher {
    pub fn new(
        metadata: Arc<dyn metadata::MetadataStore>,
        storage: Arc<dyn storage::ObjectStore>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        tokio::spawn(async move {
            Self::process_batch_queue(metadata, storage, rx).await;
        });

        Self { tx }
    }

    async fn process_batch_queue(
        metadata: Arc<dyn metadata::MetadataStore>,
        storage: Arc<dyn storage::ObjectStore>,
        mut rx: tokio::sync::mpsc::Receiver<Item>,
    ) {
        while let Some(item) = rx.recv().await {
            let mut key_value_count = item.req.tables.iter().map(|t| t.items.len()).sum::<usize>();
            let mut buffer = vec![item];

            let timeout = tokio::time::sleep(std::time::Duration::from_millis(250));
            tokio::pin!(timeout);
            
            loop {
                tokio::select! {
                    biased;
                    
                    maybe_item = rx.recv() => {
                        match maybe_item {
                            Some(item) => {
                                key_value_count += item.req.tables.iter().map(|t| t.items.len()).sum::<usize>();
                                buffer.push(item);
                                
                                // If we've collected enough items, process the batch immediately
                                if key_value_count >= 50_000 {
                                    
                                    break;
                                }
                            },
                            None => {
                                break;
                            }
                        }
                    },
                    
                    _ = &mut timeout => {
                        break;
                    }
                }
            }
            
            let result = Self::process_batch(&metadata, &storage, &buffer).await;
            let result = result.map_err(|e| Status::internal(e.to_string()));
            for item in buffer.drain(..) {
                let _ = item.tx.send(result.clone());
            }
        }

        tracing::info!("Batch queue closed");
    }

    async fn process_batch(
        _metadata: &Arc<dyn metadata::MetadataStore>,
        _storage: &Arc<dyn storage::ObjectStore>,
        _batch: &[Item],
    ) -> Result<(), BatchError> {
        Ok(())
    }
}

#[tonic::async_trait]
impl Batcher for MyBatcher {
    async fn write_batch(
        &self,
        req: Request<proto::WriteBatchRequest>,
    ) -> Result<Response<proto::WriteBatchResponse>, Status> {
        if req.get_ref().tables.is_empty() {
            return Err(Status::invalid_argument("No writes provided"));
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        let item = Item {
            req: req.into_inner(),
            tx,
        };
        self.tx
            .send(item)
            .await
            .map_err(|e| Status::internal(format!("Failed to send batch: {}", e)))?;

        rx.await
            .map_err(|e| Status::internal(format!("Failed to receive batch result: {}", e)))?
            .map_err(|e| e)?;

        Ok(Response::new(proto::WriteBatchResponse {}))
    }
}
