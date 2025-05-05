use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::{FutureExt, StreamExt, TryStreamExt, pin_mut, stream};
use thiserror::Error;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use crate::consistent_hashring::ConsistentHashRing;
use crate::forest::{Forest, ForestError};
use crate::metadata::{self, MetadataStore, SeqNo};
use crate::pod_watcher::{self, PodChange, PodWatcherError};
use crate::proto;
use crate::proto::cache_service_client::CacheServiceClient;
use crate::proto::reader_service_server::ReaderService;
use crate::runs::{RunError, Stats, WriteOperation};

#[derive(Debug, Error)]
pub enum ReaderServiceError {
    #[error("Pod watcher error: {0}")]
    PodWatcherError(#[from] PodWatcherError),

    #[error("Forest error: {0}")]
    ForestError(#[from] ForestError),

    #[error("Connection error: {0}")]
    ConnectionError(#[from] tonic::transport::Error),
}

#[derive(Clone)]
struct ConnectionManager {
    port: u16,
    connections: Arc<Mutex<HashMap<String, CacheServiceClient<Channel>>>>,
    consistent_hashring: Arc<Mutex<ConsistentHashRing<String>>>,
}

impl ConnectionManager {
    async fn new(port: u16) -> Result<Self, ReaderServiceError> {
        let pods_stream = pod_watcher::watch().await?;

        // Create ConnectionManager instance
        let connection_manager = ConnectionManager {
            port,
            connections: Arc::new(Mutex::new(HashMap::new())),
            consistent_hashring: Arc::new(Mutex::new(ConsistentHashRing::new(4))),
        };

        // Use hashring from connection_manager for pod watcher
        let hashring_clone = connection_manager.consistent_hashring.clone();
        tokio::spawn(async move {
            pin_mut!(pods_stream);
            while let Some(pod_change) = pods_stream.next().await {
                match pod_change {
                    Ok(PodChange::Added(pod)) => {
                        info!("Adding pod {pod} to consistent hashring");
                        hashring_clone.lock().unwrap().add_node(pod);
                    },
                    Ok(PodChange::Removed(pod)) => {
                        info!("Removing pod {pod} from consistent hashring");
                        hashring_clone.lock().unwrap().remove_node(&pod);
                    },
                    Err(e) => {
                        error!("Error watching pods: {e}");
                    },
                }
            }
        });

        Ok(connection_manager)
    }

    async fn get_connection(
        &self,
        pod: &str,
    ) -> Result<CacheServiceClient<Channel>, ReaderServiceError> {
        // First check if connection exists
        {
            let connections = self.connections.lock().unwrap();
            if let Some(conn) = connections.get(pod).cloned() {
                return Ok(conn);
            }
        }

        // Connection not found, create a new one outside the lock
        let addr = format!("http://{}:{}", pod, self.port);
        let conn = match CacheServiceClient::connect(addr).await {
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
        routing_key: String,
        request: Request<proto::GetFromRunRequest>,
    ) -> Result<Response<proto::GetFromRunResponse>, Status> {
        let run_pod = {
            let consistent_hashring = self.consistent_hashring.lock().unwrap();
            consistent_hashring.get_node(&routing_key)
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

    async fn table_scan_run(
        &self,
        routing_key: String,
        request: Request<proto::ScanFromRunRequest>,
    ) -> Result<Response<proto::ScanFromRunResponse>, Status> {
        let scan_pod = {
            let consistent_hashring = self.consistent_hashring.lock().unwrap();
            consistent_hashring.get_node(&routing_key)
        };

        let scan_pod = match scan_pod {
            Some(pod) => pod,
            None => return Err(Status::internal("No reader service pods available")),
        };

        let mut scan_conn = match self.get_connection(&scan_pod).await {
            Ok(conn) => conn,
            Err(e) => {
                return Err(Status::internal(format!(
                    "No connection to reader service pod for scan: {}",
                    e
                )));
            },
        };

        scan_conn.scan_from_run(request).await
    }
}

#[derive(Clone)]
pub struct MyReader {
    forest: Forest,
    connection_manager: ConnectionManager,
}

impl MyReader {
    pub async fn new(metadata: MetadataStore, port: u16) -> Result<Self, ReaderServiceError> {
        let forest = Forest::new(metadata).await?;
        let connection_manager = ConnectionManager::new(port).await?;

        Ok(Self {
            forest,
            connection_manager,
        })
    }

    async fn table_get_batch(
        &self,
        forest_state: Arc<crate::forest::State>,
        request: Request<proto::TableGetBatchRequest>,
    ) -> Result<Response<proto::TableGetBatchResponse>, Status> {
        let request = request.into_inner();
        let table_name = metadata::TableName::from(request.table_name);
        let table_prefix_len = table_name.len() + 1;

        let mut responses = Vec::new();
        if !forest_state.wal.is_empty() {
            // Make WAL request
            let wal_request = proto::GetFromRunRequest {
                run_ids: forest_state
                    .wal
                    .values()
                    .rev()
                    .map(|r| r.id.to_string())
                    .collect(),
                keys: request
                    .keys
                    .iter()
                    .map(|key| format!("{table_name}.{key}"))
                    .collect(),
            };

            // Clone connection_manager for the future
            let conn_manager_clone = self.connection_manager.clone();
            let wal_response_fut = async move {
                conn_manager_clone
                    .table_get_batch_run("wal".to_string(), Request::new(wal_request))
                    .await
            }
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
                    run_ids: vec![run_id.to_string()],
                    keys: request.keys.clone(),
                };

                // Clone connection_manager for the future
                let conn_manager_clone = self.connection_manager.clone();
                let run_response_fut = async move {
                    conn_manager_clone
                        .table_get_batch_run(run_id.to_string(), Request::new(run_request))
                        .await
                }
                .map(|r| r.map(|r| r.into_inner().items));

                responses.push(run_response_fut.boxed());
            }

            for (_, run_metadatas) in table.tree.iter() {
                let mut keys_by_run = HashMap::new();

                for key in request.keys.iter() {
                    if let Some((_, run_metadata)) = run_metadatas
                        .range::<str, _>((
                            std::ops::Bound::Unbounded,
                            std::ops::Bound::Included(key.as_str()),
                        ))
                        .next_back()
                    {
                        keys_by_run
                            .entry(run_metadata.id.clone())
                            .or_insert_with(Vec::new)
                            .push(key.clone());
                    }
                }

                for (run_id, keys) in keys_by_run {
                    let run_request = proto::GetFromRunRequest {
                        run_ids: vec![run_id.to_string()],
                        keys,
                    };

                    // Clone connection_manager for the future
                    let conn_manager_clone = self.connection_manager.clone();
                    let run_response_fut = async move {
                        conn_manager_clone
                            .table_get_batch_run(run_id.to_string(), Request::new(run_request))
                            .await
                    }
                    .map(|r| r.map(|r| r.into_inner().items));

                    responses.push(run_response_fut.boxed());
                }
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

    async fn table_scan(
        &self,
        forest_state: Arc<crate::forest::State>,
        request: proto::ScanRequest,
    ) -> Result<proto::ScanResponse, Status> {
        let table_name = metadata::TableName::from(request.table_name.clone());
        let table_prefix_len = table_name.len() + 1;

        let mut streams_to_merge = Vec::new();
        let mut seq_counter = i64::MAX;

        if !forest_state.wal.is_empty() {
            let wal_request = proto::ScanFromRunRequest {
                run_ids: forest_state
                    .wal
                    .values()
                    .rev()
                    .map(|r| r.id.to_string())
                    .collect(),
                exclusive_start_key: format!("{table_name}.{}", &request.exclusive_start_key),
                max_results: request.max_results,
            };

            // Clone connection_manager for the future
            let conn_manager_clone = self.connection_manager.clone();
            let wal_scan_fut = async move {
                conn_manager_clone
                    .table_scan_run("wal".to_string(), Request::new(wal_request))
                    .await
            };

            let wal_stream = stream::once(wal_scan_fut)
                .map_ok(|resp| {
                    stream::iter(resp.into_inner().items.into_iter().map(Ok::<_, Status>))
                })
                .try_flatten()
                .map_ok(move |item: proto::GetFromRunItem| {
                    let mut item = item;
                    item.key = item.key.split_off(table_prefix_len);
                    WriteOperation::from(item)
                })
                .map_err(RunError::GrpcError)
                .boxed();

            let seq_no = SeqNo::from(seq_counter);
            seq_counter -= 1;
            streams_to_merge.push((seq_no, wal_stream));
        }

        if let Some(table) = forest_state.tables.get(&table_name) {
            for (_, run_metadata) in table.buffer.iter().rev() {
                let run_id = run_metadata.id.clone();
                let buffer_request = proto::ScanFromRunRequest {
                    run_ids: vec![run_id.to_string()],
                    exclusive_start_key: request.exclusive_start_key.clone(),
                    max_results: request.max_results,
                };

                // Clone connection_manager for the future
                let conn_manager_clone = self.connection_manager.clone();
                let buffer_scan_fut = async move {
                    conn_manager_clone
                        .table_scan_run(run_id.to_string(), Request::new(buffer_request))
                        .await
                };

                let buffer_stream = stream::once(buffer_scan_fut)
                    .map_ok(|resp| stream::iter(resp.into_inner().items.into_iter().map(Ok)))
                    .try_flatten()
                    .map_ok(WriteOperation::from)
                    .map_err(RunError::GrpcError)
                    .boxed();

                let seq_no = SeqNo::from(seq_counter);
                seq_counter -= 1;
                streams_to_merge.push((seq_no, buffer_stream));
            }

            for run_metadatas in table.tree.values() {
                // Clone variables needed within the stream processing outside the iterator chain
                let conn_manager_level_clone = self.connection_manager.clone();
                let start_key_level_clone = request.exclusive_start_key.clone();
                let max_results_level = request.max_results;

                // Create an iterator over eligible runs in reverse order, without collecting
                let eligible_runs_iter = run_metadatas
                    .values()
                    .filter(move |run_metadata| match &run_metadata.stats {
                        Stats::StatsV1(stats) => stats.max_key > start_key_level_clone,
                    })
                    .cloned()
                    .collect::<Vec<_>>(); // TODO: This is highly inefficient, need to avoid this.

                // Create a stream directly from the iterator
                let start_key_level_clone = request.exclusive_start_key.clone();
                let level_stream = stream::iter(eligible_runs_iter)
                    .then(move |run_metadata| {
                        // Clone necessary variables for each async operation
                        let run_id = run_metadata.id.clone();
                        let conn_manager_run_clone = conn_manager_level_clone.clone();
                        let start_key_run_clone = start_key_level_clone.clone();

                        async move {
                            // Prepare the request for scanning this specific run
                            let run_request_proto = proto::ScanFromRunRequest {
                                run_ids: vec![run_id.to_string()],
                                exclusive_start_key: start_key_run_clone,
                                max_results: max_results_level,
                            };
                            // Perform the scan RPC call
                            conn_manager_run_clone
                                .table_scan_run(run_id.to_string(), Request::new(run_request_proto))
                                .await
                        }
                    })
                    .map_ok(|resp| {
                        // Input: Ok(Response<ScanFromRunResponse>)
                        // Convert successful response into a stream of items
                        stream::iter(resp.into_inner().items.into_iter().map(Ok))
                        // Output: Stream<Item = Result<GetFromRunItem, Status>>
                    })
                    .try_flatten() // Flatten the stream of streams into a single stream of items
                    .map_ok(WriteOperation::from) // Convert proto item to internal WriteOperation
                    .map_err(RunError::GrpcError) // Convert gRPC status error to RunError
                    .boxed(); // Box the stream for type erasure

                // Assign a sequence number and add the stream to the merge list,
                // even if the iterator was empty (resulting in an empty stream).
                let seq_no = SeqNo::from(seq_counter);
                seq_counter -= 1;
                streams_to_merge.push((seq_no, level_stream));
            }
        }

        let merged_stream = crate::k_way::merge(streams_to_merge);

        let merged_items = merged_stream
            .try_filter_map(|write_op| async {
                match write_op {
                    WriteOperation::Put(key, value) => Ok(Some(proto::ScanItem { key, value })),
                    WriteOperation::Delete(_) => Ok(None),
                }
            })
            .take(request.max_results as usize)
            .map_err(|run_error: RunError| {
                error!("Error during k-way merge or run scan: {}", run_error);
                Status::internal(format!("Scan failed: {}", run_error))
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(proto::ScanResponse {
            items: merged_items,
        })
    }
}

#[tonic::async_trait]
impl ReaderService for MyReader {
    async fn get_batch(
        &self,
        request: Request<proto::GetBatchRequest>,
    ) -> Result<Response<proto::GetBatchResponse>, Status> {
        let forest_state = self.forest.get_state();
        let (seq_no, tables) = {
            let request = request.into_inner();
            (SeqNo::from(request.seq_no), request.tables)
        };

        if seq_no > forest_state.seq_no {
            return Err(Status::failed_precondition(format!(
                "Requested seq_no {} is greater than the current seq_no {}",
                seq_no, forest_state.seq_no
            )));
        }

        let responses = futures::future::join_all(tables.into_iter().map(|table_request| {
            self.table_get_batch(forest_state.clone(), Request::new(table_request))
        }))
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

    async fn scan(
        &self,
        request: Request<proto::ScanRequest>,
    ) -> Result<Response<proto::ScanResponse>, Status> {
        let max_results_val = request.get_ref().max_results;
        if max_results_val <= 0 || max_results_val > 10_000 {
            return Err(Status::invalid_argument(
                "max_results must be between 1 and 10000",
            ));
        }

        let forest_state = self.forest.get_state();
        let request_inner = request.into_inner();

        let response = self.table_scan(forest_state, request_inner).await?;
        Ok(Response::new(response))
    }
}
