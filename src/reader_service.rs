use std::{
    collections::HashMap,
    sync::{
        Arc,
        Mutex,
    },
};

use clap::{
    Parser,
    arg,
};
use futures::{
    FutureExt,
    StreamExt,
    TryStreamExt,
    pin_mut,
    stream,
};
use thiserror::Error;
use tonic::{
    Request,
    Response,
    Status,
    transport::Channel,
};
use tracing::{
    error,
    info,
};

use crate::{
    consistent_hashring::ConsistentHashRing,
    forest::{
        Forest,
        ForestError,
        ForestImpl,
        Snapshot as ForestState,
    },
    k_way,
    metadata::{
        self,
        MetadataStore,
        SeqNo,
    },
    pod_watcher::{
        self,
        PodChange,
        PodWatcherError,
    },
    proto,
    runs::{
        RunError,
        Stats,
        WriteOperation,
    },
    storage::ObjectStore,
};

#[derive(Debug, Parser, Clone)]

pub struct ReaderConfig {
    #[arg(
        long,
        env = "SKYVAULT_READER_CACHE_LABEL_SELECTOR",
        default_value = "app.kubernetes.io/component=skyvault-cache",
        required_if_eq("service", "reader")
    )]
    pub cache_label_selector: String,

    #[arg(
        long,
        env = "SKYVAULT_READER_CACHE_PORT",
        default_value = "50051",
        required_if_eq("service", "reader")
    )]
    pub cache_port: u16,
}

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
struct ConsistentHashCM {
    port: u16,
    connections: Arc<Mutex<HashMap<String, proto::cache_service_client::CacheServiceClient<Channel>>>>,
    consistent_hashring: Arc<Mutex<ConsistentHashRing<String>>>,
}

impl ConsistentHashCM {
    async fn new(
        reader_config: ReaderConfig,
        k8s_client: kube::Client,
        namespace: String,
    ) -> Result<Self, ReaderServiceError> {
        let pods_stream = pod_watcher::watch(k8s_client, namespace, reader_config.cache_label_selector).await?;

        // Create ConnectionManager instance
        let connection_manager = ConsistentHashCM {
            port: reader_config.cache_port,
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
                        error!(error = %e, "Error watching pods");
                    },
                }
            }
        });

        Ok(connection_manager)
    }

    async fn get_connection(
        &self,
        pod: &str,
    ) -> Result<proto::cache_service_client::CacheServiceClient<Channel>, ReaderServiceError> {
        // First check if connection exists
        {
            let connections = self.connections.lock().unwrap();
            if let Some(conn) = connections.get(pod).cloned() {
                return Ok(conn);
            }
        }

        // Connection not found, create a new one outside the lock
        let addr = format!("http://{}:{}", pod, self.port);
        let conn = match proto::cache_service_client::CacheServiceClient::connect(addr).await {
            Ok(conn) => conn,
            Err(e) => return Err(ReaderServiceError::ConnectionError(e)),
        };

        // Now acquire the lock again to insert the new connection
        let conn_clone = conn.clone();
        self.connections.lock().unwrap().insert(pod.to_string(), conn);
        Ok(conn_clone)
    }
}

#[tonic::async_trait]
impl ConnectionManager for ConsistentHashCM {
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
                    "No connection to reader service pod for run: {e}"
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
                    "No connection to reader service pod for scan: {e}"
                )));
            },
        };

        scan_conn.scan_from_run(request).await
    }
}

#[tonic::async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait ConnectionManager {
    async fn table_get_batch_run(
        &self,
        routing_key: String,
        request: Request<proto::GetFromRunRequest>,
    ) -> Result<Response<proto::GetFromRunResponse>, Status>;

    async fn table_scan_run(
        &self,
        routing_key: String,
        request: Request<proto::ScanFromRunRequest>,
    ) -> Result<Response<proto::ScanFromRunResponse>, Status>;
}

#[derive(Clone)]
pub struct MyReader {
    forest: Forest,
    connection_manager: Arc<dyn ConnectionManager + Send + Sync>,
}

impl MyReader {
    pub async fn new(
        metadata: MetadataStore,
        object_store: ObjectStore,
        reader_config: ReaderConfig,
        k8s_client: kube::Client,
        namespace: String,
    ) -> Result<Self, ReaderServiceError> {
        let forest = ForestImpl::watch(metadata, object_store).await?;
        let connection_manager = ConsistentHashCM::new(reader_config, k8s_client, namespace).await?;

        Ok(Self {
            forest,
            connection_manager: Arc::new(connection_manager),
        })
    }

    #[cfg(test)]
    pub fn new_for_test(forest: Forest, connection_manager: Arc<dyn ConnectionManager + Send + Sync>) -> Self {
        Self {
            forest,
            connection_manager,
        }
    }

    async fn table_get_batch(
        &self,
        forest_state: Arc<ForestState>,
        request: Request<proto::TableGetBatchRequest>,
    ) -> Result<Response<proto::TableGetBatchResponse>, Status> {
        let request = request.into_inner();
        let table_name = metadata::TableName::from(request.table_name.clone());
        let table_id = forest_state
            .tables
            .get(&table_name)
            .and_then(|t| t.table_id)
            .ok_or_else(|| Status::not_found(format!("Table not found: {table_name}")))?;
        let table_prefix_len = format!("{table_id}.").len();

        let mut responses = Vec::new();
        if !forest_state.wal.is_empty() {
            // Make WAL request
            let wal_request = proto::GetFromRunRequest {
                run_ids: forest_state.wal.values().rev().map(|r| r.id.to_string()).collect(),
                keys: request.keys.iter().map(|key| format!("{table_id}.{key}")).collect(),
            };

            // Clone the Arc containing the trait object
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

        if let Some(table) = forest_state.trees.get(&table_id) {
            for (_, run_metadata) in table.buffer.iter().rev() {
                let run_id = run_metadata.id.clone();

                let run_request = proto::GetFromRunRequest {
                    run_ids: vec![run_id.to_string()],
                    keys: request.keys.clone(),
                };

                // Clone the Arc containing the trait object
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
                        .range::<str, _>((std::ops::Bound::Unbounded, std::ops::Bound::Included(key.as_str())))
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

                    // Clone the Arc containing the trait object
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
                    proto::get_from_run_item::Result::Value(value) => Some(proto::GetBatchItem { key, value }),
                    proto::get_from_run_item::Result::Deleted(_) => None,
                })
                .collect(),
        }))
    }

    async fn table_scan(
        &self,
        forest_state: Arc<ForestState>,
        request: proto::ScanRequest,
    ) -> Result<proto::ScanResponse, Status> {
        let table_name = metadata::TableName::from(request.table_name.clone());
        let table_id = forest_state
            .tables
            .get(&table_name)
            .and_then(|t| t.table_id)
            .ok_or_else(|| Status::not_found(format!("Table not found: {table_name}")))?;
        let table_prefix_len = format!("{table_id}.").len();

        let mut streams_to_merge = Vec::new();
        let mut seq_counter = i64::MAX;

        if !forest_state.wal.is_empty() {
            let wal_request = proto::ScanFromRunRequest {
                run_ids: forest_state.wal.values().rev().map(|r| r.id.to_string()).collect(),
                exclusive_start_key: format!("{table_id}.{}", &request.exclusive_start_key),
                max_results: request.max_results,
            };

            // Clone the Arc containing the trait object
            let conn_manager_clone = self.connection_manager.clone();
            let wal_scan_fut = async move {
                conn_manager_clone
                    .table_scan_run("wal".to_string(), Request::new(wal_request))
                    .await
            };

            let wal_stream = stream::once(wal_scan_fut)
                .map_ok(|resp| stream::iter(resp.into_inner().items.into_iter().map(Ok::<_, Status>)))
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

        if let Some(table) = forest_state.trees.get(&table_id) {
            for (_, run_metadata) in table.buffer.iter().rev() {
                let run_id = run_metadata.id.clone();
                let buffer_request = proto::ScanFromRunRequest {
                    run_ids: vec![run_id.to_string()],
                    exclusive_start_key: request.exclusive_start_key.clone(),
                    max_results: request.max_results,
                };

                // Clone the Arc containing the trait object
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
                // Clone the Arc containing the trait object outside the iterator chain
                let conn_manager_level_clone = self.connection_manager.clone();
                let start_key_level_clone = request.exclusive_start_key.clone();
                let max_results_level_clone = request.max_results;

                let eligible_runs_iter = {
                    let mut collected_runs = Vec::new();
                    let mut current_put_count = 0u64;
                    for run_metadata in run_metadatas.values() {
                        if match &run_metadata.stats {
                            Stats::StatsV1(stats) => stats.max_key > start_key_level_clone,
                        } {
                            collected_runs.push(run_metadata.clone());
                            match &run_metadata.stats {
                                Stats::StatsV1(stats) => {
                                    current_put_count += stats.put_count;
                                    if current_put_count > max_results_level_clone {
                                        break;
                                    }
                                },
                            }
                        }
                    }

                    collected_runs
                };

                // Create a stream directly from the iterator
                let start_key_level_clone = request.exclusive_start_key.clone();
                let level_stream = stream::iter(eligible_runs_iter)
                    .then(move |run_metadata| {
                        // Clone the Arc for each async operation
                        let run_id = run_metadata.id.clone();
                        let conn_manager_run_clone = conn_manager_level_clone.clone();
                        let start_key_run_clone = start_key_level_clone.clone();

                        async move {
                            // Prepare the request for scanning this specific run
                            let run_request_proto = proto::ScanFromRunRequest {
                                run_ids: vec![run_id.to_string()],
                                exclusive_start_key: start_key_run_clone,
                                max_results: max_results_level_clone,
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

        let merged_stream = k_way::merge(streams_to_merge);

        let merged_items = merged_stream
            .try_filter_map(|write_op| async {
                match write_op {
                    WriteOperation::Put(key, value) => Ok(Some(proto::ScanItem { key, value })),
                    WriteOperation::Delete(_) => Ok(None),
                }
            })
            .take(request.max_results as usize)
            .map_err(|run_error: RunError| {
                error!(error = %run_error, "Error during k-way merge or run scan");
                Status::internal(format!("Scan failed: {run_error}"))
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(proto::ScanResponse { items: merged_items })
    }
}

#[tonic::async_trait]
impl proto::reader_service_server::ReaderService for MyReader {
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
            let self_clone = self.clone();
            let forest_state_clone = forest_state.clone();
            async move {
                self_clone
                    .table_get_batch(forest_state_clone, Request::new(table_request))
                    .await
            }
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

    async fn scan(&self, request: Request<proto::ScanRequest>) -> Result<Response<proto::ScanResponse>, Status> {
        let max_results_val = request.get_ref().max_results;
        if !(1..=10_000).contains(&max_results_val) {
            return Err(Status::invalid_argument("max_results must be between 1 and 10000"));
        }

        let forest_state = self.forest.get_state();
        let request_inner = request.into_inner();
        let response = self.table_scan(forest_state, request_inner).await?;
        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use mockall::predicate::*;
    use tokio; // For async test runtime
    use tonic::{
        Request,
        Response,
        Status,
    };

    use super::*;
    use crate::{
        forest::{
            Forest,
            MockForestTrait,
        },
        metadata::{
            BelongsTo,
            RunMetadata,
            SeqNo,
            TableConfig,
            TableID,
            TableName,
        },
        proto,
        proto::reader_service_server::ReaderService,
        runs::{
            RunId,
            Stats,
            StatsV1,
        },
    };

    async fn create_test_forest(seq_no: SeqNo, tables: Vec<TableConfig>, runs: Vec<RunMetadata>) -> Forest {
        let tables = tables.into_iter().map(|t| (t.table_name.clone(), t)).collect();
        let state = ForestState::from_parts(seq_no, tables, runs).await;

        let mut mock_forest = MockForestTrait::new();
        mock_forest.expect_get_state().times(1).return_const(state.clone());

        Arc::new(mock_forest)
    }

    fn create_run_response(
        items: Vec<proto::GetFromRunItem>,
    ) -> Pin<Box<dyn Future<Output = Result<Response<proto::GetFromRunResponse>, Status>> + Send>> {
        Box::pin(async move { Ok(Response::new(proto::GetFromRunResponse { items })) })
    }

    fn create_scan_response(
        items: Vec<proto::GetFromRunItem>,
    ) -> Pin<Box<dyn Future<Output = Result<Response<proto::ScanFromRunResponse>, Status>> + Send>> {
        Box::pin(async move { Ok(Response::new(proto::ScanFromRunResponse { items })) })
    }

    fn create_run_item(key: &str, value: Option<&str>) -> proto::GetFromRunItem {
        proto::GetFromRunItem {
            key: key.to_string(),
            result: value
                .map(|v| proto::get_from_run_item::Result::Value(v.as_bytes().to_vec()))
                .or(Some(proto::get_from_run_item::Result::Deleted(()))),
        }
    }

    #[tokio::test]
    async fn test_get_batch() {
        let table_id = TableID::from(1);
        let table_name = "test_table";
        let wal_run_id = RunId::from("wal_run_1");
        let buf_run_id = RunId::from("buf_run_1");
        let key = "the_key";

        let forest = create_test_forest(
            SeqNo::from(1),
            vec![TableConfig {
                table_id: Some(table_id),
                table_name: TableName::from(table_name.to_string()),
            }],
            vec![
                RunMetadata {
                    id: wal_run_id.clone(),
                    belongs_to: BelongsTo::WalSeqNo(SeqNo::from(1)),
                    stats: Stats::StatsV1(StatsV1 {
                        min_key: "".to_string(),
                        max_key: "".to_string(),
                        put_count: 0,
                        delete_count: 0,
                        size_bytes: 0,
                    }),
                },
                RunMetadata {
                    id: buf_run_id.clone(),
                    belongs_to: BelongsTo::TableBuffer(table_id, SeqNo::from(1)),
                    stats: Stats::StatsV1(StatsV1 {
                        min_key: "".to_string(),
                        max_key: "".to_string(),
                        put_count: 0,
                        delete_count: 0,
                        size_bytes: 0,
                    }),
                },
            ],
        )
        .await;

        let mut mock_conn = MockConnectionManager::new();
        mock_conn
            .expect_table_get_batch_run()
            .with(
                eq("wal".to_string()),
                // Check that the request contains the correct run_id and key
                function(move |req: &Request<proto::GetFromRunRequest>| {
                    let inner = req.get_ref();
                    inner.run_ids == vec![wal_run_id.to_string()] && inner.keys == vec![format!("{}.{}", table_id, key)]
                }),
            )
            .times(1)
            .returning(move |_routing_key, _req| {
                create_run_response(vec![create_run_item(
                    &format!("{table_id}.{key}"),
                    Some("wal_value"),
                )])
            });
        mock_conn
            .expect_table_get_batch_run()
            .with(
                eq(buf_run_id.to_string()),
                function(move |req: &Request<proto::GetFromRunRequest>| {
                    let inner = req.get_ref();
                    inner.run_ids == vec![buf_run_id.to_string()] && inner.keys == vec![key.to_string()]
                }),
            )
            .times(1)
            .returning(move |_routing_key, _req| create_run_response(vec![create_run_item(key, Some("buf_value"))]));

        let reader = MyReader::new_for_test(forest, Arc::new(mock_conn));

        let request = proto::GetBatchRequest {
            seq_no: 0,
            tables: vec![proto::TableGetBatchRequest {
                table_name: table_name.to_string(),
                keys: vec![key.to_string()],
            }],
        };

        let response = reader.get_batch(Request::new(request)).await;
        assert!(response.is_ok());
        let batch_response = response.unwrap().into_inner();
        assert_eq!(batch_response.tables.len(), 1);
        let table_response = &batch_response.tables[0];
        assert_eq!(table_response.table_name, table_name);
        assert_eq!(table_response.items.len(), 1);
        assert_eq!(table_response.items[0].key, key);
        assert_eq!(table_response.items[0].value, b"wal_value");
    }

    #[tokio::test]
    async fn test_scan() {
        let table_id = TableID::from(1);
        let table_name = "test_table";
        let wal_run_id = RunId::from("wal_run_1");
        let buf_run_id = RunId::from("buf_run_1");
        let exclusive_start_key = "key1";

        let forest = create_test_forest(
            SeqNo::from(1),
            vec![TableConfig {
                table_id: Some(table_id),
                table_name: TableName::from(table_name.to_string()),
            }],
            vec![
                RunMetadata {
                    id: wal_run_id.clone(),
                    belongs_to: BelongsTo::WalSeqNo(SeqNo::from(1)),
                    stats: Stats::StatsV1(StatsV1 {
                        min_key: "".to_string(),
                        max_key: "".to_string(),
                        put_count: 0,
                        delete_count: 0,
                        size_bytes: 0,
                    }),
                },
                RunMetadata {
                    id: buf_run_id.clone(),
                    belongs_to: BelongsTo::TableBuffer(table_id, SeqNo::from(1)),
                    stats: Stats::StatsV1(StatsV1 {
                        min_key: "".to_string(),
                        max_key: "".to_string(),
                        put_count: 0,
                        delete_count: 0,
                        size_bytes: 0,
                    }),
                },
            ],
        )
        .await;

        let mut mock_conn = MockConnectionManager::new();
        mock_conn
            .expect_table_scan_run()
            .with(
                eq("wal".to_string()),
                // Check that the request contains the correct run_id and key
                function(move |req: &Request<proto::ScanFromRunRequest>| {
                    let inner = req.get_ref();
                    inner.run_ids == vec![wal_run_id.to_string()]
                        && inner.exclusive_start_key == format!("{table_id}.{exclusive_start_key}")
                }),
            )
            .times(1)
            .returning(move |_routing_key, _req| {
                create_scan_response(vec![create_run_item(&format!("{table_id}.key2"), Some("wal_value"))])
            });
        mock_conn
            .expect_table_scan_run()
            .with(
                eq(buf_run_id.to_string()),
                function(move |req: &Request<proto::ScanFromRunRequest>| {
                    let inner = req.get_ref();
                    inner.run_ids == vec![buf_run_id.to_string()]
                        && inner.exclusive_start_key == exclusive_start_key
                }),
            )
            .times(1)
            .returning(move |_routing_key, _req| {
                create_scan_response(vec![create_run_item("key3", Some("buf_value"))])
            });

        let reader = MyReader::new_for_test(forest, Arc::new(mock_conn));

        let request = proto::ScanRequest {
            table_name: table_name.to_string(),
            exclusive_start_key: exclusive_start_key.to_string(),
            max_results: 2,
        };

        let response = reader.scan(Request::new(request)).await;
        assert!(response.is_ok());
        let scan_response = response.unwrap().into_inner();
        assert_eq!(scan_response.items.len(), 2);
        assert_eq!(scan_response.items[0].key, "key2");
        assert_eq!(scan_response.items[1].key, "key3");
        assert_eq!(scan_response.items[0].value, b"wal_value");
        assert_eq!(scan_response.items[1].value, b"buf_value");
    }
}
