use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::{StreamExt, pin_mut};
use thiserror::Error;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use crate::consistent_hashring::ConsistentHashRing;
use crate::forest::{Forest, ForestError};
use crate::metadata::MetadataStore;
use crate::pod_watcher::{self, PodChange, PodWatcherError};
use crate::proto::reader_service_client::ReaderServiceClient;
use crate::proto::reader_service_server::ReaderService;
use crate::storage::{ObjectStore, StorageCache};
use crate::{proto, runs};

#[derive(Debug, Error)]
pub enum ReaderServiceError {
    #[error("Pod watcher error: {0}")]
    PodWatcherError(#[from] PodWatcherError),

    #[error("Forest error: {0}")]
    ForestError(#[from] ForestError),

    #[error("Connection error: {0}")]
    ConnectionError(#[from] tonic::transport::Error),
}

pub struct MyReader {
    // Metadata
    forest: Forest,

    // Cache
    storage_cache: StorageCache,
    consistent_hashring: Arc<Mutex<ConsistentHashRing<String>>>,

    // Connection
    port: u16,
    connections: Arc<Mutex<HashMap<String, ReaderServiceClient<Channel>>>>,
}

impl MyReader {
    pub async fn new(
        metadata: MetadataStore,
        storage: ObjectStore,
        port: u16,
    ) -> Result<Self, ReaderServiceError> {
        let forest = Forest::new(metadata).await?;

        let pods_stream = pod_watcher::watch().await?;
        let connections = Arc::new(Mutex::new(HashMap::new()));
        let consistent_hashring = Arc::new(Mutex::new(ConsistentHashRing::new(4)));

        let ch = consistent_hashring.clone();
        tokio::spawn(async move {
            pin_mut!(pods_stream);
            while let Some(pod_change) = pods_stream.next().await {
                match pod_change {
                    Ok(PodChange::Added(pod)) => {
                        info!("Adding pod {pod} to consistent hashring");
                        ch.lock().unwrap().add_node(pod);
                    },
                    Ok(PodChange::Removed(pod)) => {
                        info!("Removing pod {pod} from consistent hashring");
                        ch.lock().unwrap().remove_node(&pod);
                    },
                    Err(e) => {
                        error!("Error watching pods: {e}");
                    },
                }
            }
        });

        let storage_cache = StorageCache::new(storage);
        Ok(Self {
            storage_cache,
            forest,
            consistent_hashring,
            connections,
            port,
        })
    }

    async fn get_connection(
        &self,
        pod: &str,
    ) -> Result<ReaderServiceClient<Channel>, ReaderServiceError> {
        // First check if connection exists
        {
            let connections = self.connections.lock().unwrap();
            if let Some(conn) = connections.get(pod).cloned() {
                return Ok(conn);
            }
        }

        // Connection not found, create a new one outside the lock
        let addr = format!("http://{}:{}", pod, self.port);
        let conn = match ReaderServiceClient::connect(addr).await {
            Ok(conn) => conn,
            Err(e) => return Err(ReaderServiceError::ConnectionError(e)),
        };

        // Now acquire the lock again to insert the new connection
        let conn_clone = conn.clone();
        self.connections
            .lock()
            .unwrap()
            .insert(pod.to_string(), conn);
        Ok(conn_clone)
    }
}

#[tonic::async_trait]
impl ReaderService for MyReader {
    async fn get_batch(
        &self,
        request: Request<proto::GetBatchRequest>,
    ) -> Result<Response<proto::GetBatchResponse>, Status> {
        // For WAL we send all requests to same pod.
        let wal_pod = {
            let consistent_hashring = self.consistent_hashring.lock().unwrap();
            consistent_hashring.get_node(&"wal")
        };

        let wal_pod = match wal_pod {
            Some(pod) => pod,
            None => return Err(Status::internal("No reader service pods available")),
        };

        let mut wal_conn = match self.get_connection(&wal_pod).await {
            Ok(conn) => conn,
            Err(e) => {
                return Err(Status::internal(format!(
                    "No connection to reader service pod for WAL: {}",
                    e
                )));
            },
        };

        let wal_request = proto::GetFromWalRequest {
            tables: request
                .into_inner()
                .tables
                .into_iter()
                .map(|table| proto::TableReadBatchRequest {
                    table_name: table.table_name.clone(),
                    keys: table
                        .keys
                        .into_iter()
                        .map(|key| format!("{}.{}", table.table_name, key))
                        .collect(),
                })
                .collect(),
        };

        let wal_response = wal_conn.get_from_wal(wal_request).await?;

        let mut response = proto::GetBatchResponse::default();
        for table_response in wal_response.into_inner().tables {
            let table_name = table_response.table_name;
            let items = table_response.items;
            let prefix_len = table_name.len() + 1;

            let mut table_response = proto::TableReadBatchResponse {
                table_name,
                items: vec![],
            };

            for item in items {
                match item.result {
                    Some(proto::get_from_wal_item::Result::Value(value)) => {
                        table_response.items.push(proto::GetBatchItem {
                            key: item.key[prefix_len..].to_string(),
                            value,
                        });
                    },
                    Some(proto::get_from_wal_item::Result::Deleted(_)) | None => {},
                }
            }

            response.tables.push(table_response);
        }

        Ok(Response::new(response))
    }

    async fn get_from_wal(
        &self,
        request: Request<proto::GetFromWalRequest>,
    ) -> Result<Response<proto::GetFromWalResponse>, Status> {
        let forest_state = self.forest.get_state();

        let mut response = proto::GetFromWalResponse::default();
        for table in request.into_inner().tables {
            let table_name = table.table_name;
            let mut remaining_keys = table.keys;

            let mut table_response = proto::TableGetFromWalResponse {
                table_name,
                items: vec![],
            };

            for wal in forest_state.wal.values().rev() {
                if remaining_keys.is_empty() {
                    break;
                }

                let run = match self.storage_cache.get_run(&wal.id).await {
                    Ok(run) => run,
                    Err(e) => {
                        return Err(Status::internal(format!(
                            "Error getting run {}: {}",
                            wal.id, e
                        )));
                    },
                };

                remaining_keys.retain(|key| match runs::search_run(run.as_ref(), key.as_str()) {
                    runs::SearchResult::Found(value) => {
                        table_response.items.push(proto::GetFromWalItem {
                            key: key.clone(),
                            result: Some(proto::get_from_wal_item::Result::Value(value)),
                        });

                        false
                    },
                    runs::SearchResult::Tombstone => {
                        table_response.items.push(proto::GetFromWalItem {
                            key: key.clone(),
                            result: Some(proto::get_from_wal_item::Result::Deleted(())),
                        });

                        false
                    },
                    runs::SearchResult::NotFound => true,
                });
            }

            for key in remaining_keys {
                table_response.items.push(proto::GetFromWalItem {
                    key: key.clone(),
                    result: None,
                });
            }

            response.tables.push(table_response);
        }

        Ok(Response::new(response))
    }
}
