use std::sync::{Arc, Mutex};

use futures::{pin_mut, StreamExt};
use thiserror::Error;
use tonic::{Request, Response, Status};
use tracing::error;

use crate::consistent_hashring::ConsistentHashRing;
use crate::forest::{Forest, ForestError};
use crate::metadata::MetadataStore;
use crate::pod_watcher::{self, PodChange, PodWatcherError};
use crate::proto::index_server::Index;
use crate::proto::{IndexRequest, IndexResponse};
use crate::storage::ObjectStore;

#[derive(Debug, Error)]
pub enum IndexServiceError {
    #[error("Pod watcher error: {0}")]
    PodWatcherError(#[from] PodWatcherError),

    #[error("Forest error: {0}")]
    ForestError(#[from] ForestError),
}


pub struct MyIndex {
    #[allow(dead_code)]
    storage: ObjectStore,
    forest: Forest,
    consistent_hashring: Arc<Mutex<ConsistentHashRing<String>>>,
}

impl MyIndex {
    pub async fn new(metadata: MetadataStore, storage: ObjectStore) -> Result<Self, IndexServiceError> {
        let forest = Forest::new(metadata).await?;
        
        let (pods, pods_stream) = pod_watcher::watch().await?;
        let consistent_hashring = ConsistentHashRing::with_nodes(4, pods);
        let consistent_hashring = Arc::new(Mutex::new(consistent_hashring));

        let ch = consistent_hashring.clone();
        tokio::spawn(async move {
            pin_mut!(pods_stream);
            while let Some(pod_change) = pods_stream.next().await {
                match pod_change {
                    Ok(PodChange::Added(pod)) => {
                        ch.lock().unwrap().add_node(pod);
                    },
                    Ok(PodChange::Removed(pod)) => {
                        ch.lock().unwrap().remove_node(&pod);
                    },
                    Err(e) => {
                        error!("Error watching pods: {e}");
                    }
                }
            }
        });

        Ok(Self { storage, forest, consistent_hashring })
    }
}

#[tonic::async_trait]
impl Index for MyIndex {
    async fn index_document(
        &self,
        _request: Request<IndexRequest>,
    ) -> Result<Response<IndexResponse>, Status> {
        Ok(Response::new(IndexResponse {}))
    }
}
