use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::{FutureExt, StreamExt, pin_mut};
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

    async fn table_get_batch_run(
        &self,
        request: Request<proto::GetFromRunRequest>,
    ) -> Result<Response<proto::GetFromRunResponse>, Status> {
        let run_pod = {
            let consistent_hashring = self.consistent_hashring.lock().unwrap();
            consistent_hashring.get_node(&request.get_ref().run_id)
        };

        let run_pod = match run_pod {
            Some(pod) => pod,
            None => return Err(Status::internal("No reader service pods available")),
        };

        let mut run_conn = match self.get_connection(&run_pod).await {
            Ok(conn) => conn,
            Err(e) => {
                return Err(Status::internal(format!(
                    "No connection to reader service pod for run: {}",
                    e
                )));
            },
        };

        run_conn.get_from_run(request).await
    }

    async fn table_get_batch_wal(
        &self,
        request: Request<proto::GetFromWalRequest>,
    ) -> Result<Response<proto::GetFromWalResponse>, Status> {
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

        wal_conn.get_from_wal(request).await
    }

    async fn table_get_batch(
        &self,
        request: Request<proto::TableGetBatchRequest>,
    ) -> Result<Response<proto::TableGetBatchResponse>, Status> {
        let forest_state = self.forest.get_state();

        let request = request.into_inner();
        let table_name = request.table_name;
        let table_prefix_len = table_name.len() + 1;

        let mut responses = Vec::new();
        if !forest_state.wal.is_empty() {
            // Make WAL request
            let wal_request = proto::GetFromWalRequest {
                keys: request
                    .keys
                    .iter()
                    .map(|key| format!("{table_name}.{key}"))
                    .collect(),
            };

            let wal_response_fut = self
                .table_get_batch_wal(Request::new(wal_request))
                .map(|r| {
                    r.map(|r| {
                        r.into_inner()
                            .items
                            .into_iter()
                            .map(|mut item| proto::GetFromRunItem {
                                key: item.key.split_off(table_prefix_len),
                                result: item.result,
                            })
                            .collect()
                    })
                });

            responses.push(wal_response_fut.boxed());
        }

        if let Some(table) = forest_state.tables.get(&table_name) {
            for (_, run_metadata) in table.buffer.iter().rev() {
                let run_id = run_metadata.id.clone();

                let run_request = proto::GetFromRunRequest {
                    run_id: run_id.to_string(),
                    keys: request.keys.clone(),
                };

                let run_response_fut = self
                    .table_get_batch_run(Request::new(run_request))
                    .map(|r| r.map(|r| r.into_inner().items));

                responses.push(run_response_fut.boxed());
            }
        }

        let responses = futures::future::join_all(responses).await;
        let mut merged: HashMap<String, proto::get_from_run_item::Result> = HashMap::new();
        for response in responses {
            match response {
                Ok(items) => {
                    for item in items.into_iter() {
                        if let Some(result) = item.result {
                            merged.entry(item.key).or_insert(result);
                        }
                    }
                },
                Err(e) => {
                    return Err(Status::internal(e.to_string()));
                },
            }
        }

        Ok(Response::new(proto::TableGetBatchResponse {
            table_name: table_name.to_string(),
            items: merged
                .into_iter()
                .filter_map(|(key, result)| match result {
                    proto::get_from_run_item::Result::Value(value) => {
                        Some(proto::GetBatchItem { key, value })
                    },
                    proto::get_from_run_item::Result::Deleted(_) => None,
                })
                .collect(),
        }))
    }
}

#[tonic::async_trait]
impl ReaderService for MyReader {
    async fn get_batch(
        &self,
        request: Request<proto::GetBatchRequest>,
    ) -> Result<Response<proto::GetBatchResponse>, Status> {
        let tables = request.into_inner().tables;

        let responses = futures::future::join_all(
            tables
                .into_iter()
                .map(|table_request| self.table_get_batch(Request::new(table_request))),
        )
        .await;

        let mut response = proto::GetBatchResponse::default();

        for table_result in responses {
            match table_result {
                Ok(table_response) => {
                    response.tables.push(table_response.into_inner());
                },
                Err(status) => {
                    return Err(status);
                },
            }
        }

        Ok(Response::new(response))
    }

    async fn get_from_wal(
        &self,
        request: Request<proto::GetFromWalRequest>,
    ) -> Result<Response<proto::GetFromWalResponse>, Status> {
        let forest_state = self.forest.get_state();

        let mut response = proto::GetFromWalResponse::default();
        let mut remaining_keys = request.into_inner().keys;

        for wal in forest_state.wal.values().rev() {
            if remaining_keys.is_empty() {
                break;
            }

            let run = match self.storage_cache.get_run(wal.id.clone()).await {
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
                    response.items.push(proto::GetFromRunItem {
                        key: key.clone(),
                        result: Some(proto::get_from_run_item::Result::Value(value)),
                    });

                    false
                },
                runs::SearchResult::Tombstone => {
                    response.items.push(proto::GetFromRunItem {
                        key: key.clone(),
                        result: Some(proto::get_from_run_item::Result::Deleted(())),
                    });

                    false
                },
                runs::SearchResult::NotFound => true,
            });
        }

        Ok(Response::new(response))
    }

    async fn get_from_run(
        &self,
        request: Request<proto::GetFromRunRequest>,
    ) -> Result<Response<proto::GetFromRunResponse>, Status> {
        let run_id = crate::runs::RunId(request.get_ref().run_id.clone());
        let run = match self.storage_cache.get_run(run_id).await {
            Ok(run) => run,
            Err(e) => {
                return Err(Status::internal(format!(
                    "Error getting run {}: {}",
                    request.get_ref().run_id,
                    e
                )));
            },
        };

        let mut response = proto::GetFromRunResponse::default();
        for key in request.into_inner().keys {
            match runs::search_run(run.as_ref(), key.as_str()) {
                runs::SearchResult::Found(value) => {
                    response.items.push(proto::GetFromRunItem {
                        key,
                        result: Some(proto::get_from_run_item::Result::Value(value)),
                    });
                },
                runs::SearchResult::Tombstone => {
                    response.items.push(proto::GetFromRunItem {
                        key,
                        result: Some(proto::get_from_run_item::Result::Deleted(())),
                    });
                },
                runs::SearchResult::NotFound => {},
            }
        }

        Ok(Response::new(response))
    }
}
