use std::pin::Pin;

use futures::{Stream, StreamExt, TryStreamExt, pin_mut};
use thiserror::Error;
use tonic::{Request, Response, Status};

use crate::runs::{RunError, RunId, SearchResult, WriteOperation, read_run_stream, search_run};
use crate::storage::{self, StorageCache};
use crate::{k_way, metadata, proto};

#[derive(Debug, Error)]
pub enum CacheServiceError {
    #[error("Storage error: {0}")]
    StorageError(#[from] storage::StorageError),
}

pub struct MyCache {
    storage_cache: StorageCache,
}

impl MyCache {
    pub async fn new(storage: storage::ObjectStore) -> Result<Self, CacheServiceError> {
        let storage_cache = StorageCache::new(storage);
        Ok(Self { storage_cache })
    }
}

#[tonic::async_trait]
impl proto::cache_service_server::CacheService for MyCache {
    async fn get_from_run(
        &self,
        request: Request<proto::GetFromRunRequest>,
    ) -> Result<Response<proto::GetFromRunResponse>, Status> {
        let mut response = proto::GetFromRunResponse::default();
        let request = request.into_inner();
        let mut remaining_keys = request.keys;

        for run_id in request.run_ids {
            if remaining_keys.is_empty() {
                break;
            }

            let run = match self.storage_cache.get_run(RunId(run_id.clone())).await {
                Ok(run) => run,
                Err(e) => {
                    return Err(Status::internal(format!(
                        "Error getting run {}: {}",
                        run_id, e
                    )));
                },
            };

            remaining_keys.retain(|key| match search_run(run.as_ref(), key.as_str()) {
                SearchResult::Found(value) => {
                    response.items.push(proto::GetFromRunItem {
                        key: key.clone(),
                        result: Some(proto::get_from_run_item::Result::Value(value)),
                    });

                    false
                },
                SearchResult::Tombstone => {
                    response.items.push(proto::GetFromRunItem {
                        key: key.clone(),
                        result: Some(proto::get_from_run_item::Result::Deleted(())),
                    });

                    false
                },
                SearchResult::NotFound => true,
            });
        }

        Ok(Response::new(response))
    }

    async fn scan_from_run(
        &self,
        request: Request<proto::ScanFromRunRequest>,
    ) -> Result<Response<proto::ScanFromRunResponse>, Status> {
        if request.get_ref().max_results == 0 || request.get_ref().max_results > 10_000 {
            return Err(Status::invalid_argument(
                "max_results must be between 1 and 10000",
            ));
        }

        let request = request.into_inner();
        let max_results = request.max_results;
        let exclusive_start_key = request.exclusive_start_key;

        type BoxedRunStream = Pin<Box<dyn Stream<Item = Result<WriteOperation, RunError>> + Send>>;
        let mut streams_to_merge: Vec<(metadata::SeqNo, BoxedRunStream)> = Vec::new();

        for (index, run_id_str) in request.run_ids.into_iter().enumerate() {
            let run_id = RunId(run_id_str.clone());
            let seq_no = metadata::SeqNo::from(i64::MAX - index as i64);
            let run_data = match self.storage_cache.get_run(run_id).await {
                Ok(run_data) => run_data,
                Err(e) => {
                    return Err(Status::internal(format!(
                        "Error getting run {}: {}",
                        run_id_str, e
                    )));
                },
            };
            let stream = read_run_stream(futures::stream::once(futures::future::ok(run_data)));

            let filtered_stream = {
                let exclusive_start_key = exclusive_start_key.clone();
                stream
                    .try_filter_map(move |op| {
                        let exclusive_start_key = exclusive_start_key.clone();
                        async move {
                            if op.key() > exclusive_start_key.as_str() {
                                Ok(Some(op))
                            } else {
                                Ok(None)
                            }
                        }
                    })
                    .map_err(RunError::from)
                    .boxed()
            };

            streams_to_merge.push((seq_no, filtered_stream));
        }

        let merged_stream = k_way::merge(streams_to_merge);
        pin_mut!(merged_stream);

        let mut response = proto::ScanFromRunResponse::default();
        let mut count = 0;

        while let Some(result) = merged_stream.next().await {
            let write_op =
                result.map_err(|e| Status::internal(format!("Merge stream error: {}", e)))?;

            count += if matches!(write_op, WriteOperation::Put(_, _)) {
                1
            } else {
                0
            };
            response.items.push(write_op.into());

            if count >= max_results {
                break;
            }
        }

        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use futures::{TryStreamExt, stream};

    use super::*;
    use crate::requires_docker;
    use crate::proto;
    use crate::proto::cache_service_server::CacheService;
    use crate::runs::{RunError, RunId, Stats, WriteOperation, build_runs};
    use crate::storage::ObjectStore;
    use crate::test_utils::setup_test_object_store;

    async fn create_and_store_run(
        object_store: &ObjectStore,
        run_id: &RunId,
        operations: Vec<WriteOperation>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ops_stream = stream::iter(operations.into_iter().map(Ok::<_, RunError>));

        let mut run_items: Vec<(Bytes, Stats)> = build_runs(ops_stream).try_collect().await?;

        if run_items.len() != 1 {
            return Err(format!(
                "Expected 1 run, got {}. Ensure operations are sorted.",
                run_items.len()
            )
            .into());
        }

        let (data_bytes, _stats) = run_items.pop().unwrap();

        object_store.put_run(run_id.clone(), data_bytes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_my_cache_new() {
        requires_docker!();
        let (object_store, _container) = setup_test_object_store().await.unwrap();
        let cache = MyCache::new(object_store).await;
        assert!(cache.is_ok());
    }

    #[tokio::test]
    async fn test_get_from_run_simple() {
        requires_docker!();
        let (object_store, _container) = setup_test_object_store().await.unwrap();
        let cache_service = MyCache::new(object_store.clone()).await.unwrap();

        let run_id_str = "test_get_run_1".to_string();
        let run_id = RunId(run_id_str.clone());

        let get_test_ops = vec![
            WriteOperation::Put("key1".to_string(), Bytes::from("value1").to_vec()),
            WriteOperation::Put("key2".to_string(), Bytes::from("value2").to_vec()),
            WriteOperation::Delete("key3".to_string()),
        ];

        create_and_store_run(&object_store, &run_id, get_test_ops)
            .await
            .unwrap();

        let request = Request::new(proto::GetFromRunRequest {
            run_ids: vec![run_id_str],
            keys: vec!["key1".to_string(), "key3".to_string(), "key4".to_string()],
        });

        let response = cache_service.get_from_run(request).await.unwrap();
        let response_inner = response.into_inner();

        assert_eq!(response_inner.items.len(), 2);

        let mut results_map = HashMap::new();
        for item in response_inner.items {
            results_map.insert(item.key, item.result);
        }

        match results_map.get("key1") {
            Some(Some(proto::get_from_run_item::Result::Value(val))) => {
                assert_eq!(val, b"value1");
            },
            _ => panic!(
                "key1 not found or incorrect result type. Actual: {:?}",
                results_map.get("key1")
            ),
        }

        match results_map.get("key3") {
            Some(Some(proto::get_from_run_item::Result::Deleted(_))) => {},
            _ => panic!(
                "key3 not found or incorrect result type for deleted. Actual: {:?}",
                results_map.get("key3")
            ),
        }

        assert!(!results_map.contains_key("key4"));
    }

    #[tokio::test]
    async fn test_scan_from_run_simple() {
        requires_docker!();
        let (object_store, _container) = setup_test_object_store().await.unwrap();
        let cache_service = MyCache::new(object_store.clone()).await.unwrap();

        let run_id_str = "test_scan_run_1".to_string();
        let run_id = RunId(run_id_str.clone());

        let ops = vec![
            WriteOperation::Put("a_key1".to_string(), Bytes::from("value1").to_vec()),
            WriteOperation::Delete("b_key2".to_string()),
            WriteOperation::Put("c_key3".to_string(), Bytes::from("value3").to_vec()),
            WriteOperation::Put("d_key4".to_string(), Bytes::from("value4").to_vec()),
        ];
        create_and_store_run(&object_store, &run_id, ops.clone())
            .await
            .unwrap();

        let request_all = Request::new(proto::ScanFromRunRequest {
            run_ids: vec![run_id_str.clone()],
            exclusive_start_key: "".to_string(),
            max_results: 10,
        });

        let response_all = cache_service.scan_from_run(request_all).await.unwrap();
        let inner_all = response_all.into_inner();

        assert_eq!(inner_all.items.len(), 4);
        let expected_items: Vec<proto::GetFromRunItem> =
            ops.iter().cloned().map(Into::into).collect();
        assert_eq!(inner_all.items[0], expected_items[0]);
        assert_eq!(inner_all.items[1], expected_items[1]);
        assert_eq!(inner_all.items[2], expected_items[2]);
        assert_eq!(inner_all.items[3], expected_items[3]);

        let request_max_puts = Request::new(proto::ScanFromRunRequest {
            run_ids: vec![run_id_str.clone()],
            exclusive_start_key: "".to_string(),
            max_results: 2,
        });

        let response_max_puts = cache_service.scan_from_run(request_max_puts).await.unwrap();
        let inner_max_puts = response_max_puts.into_inner();
        assert_eq!(inner_max_puts.items.len(), 3);
        assert_eq!(inner_max_puts.items[0], expected_items[0]);
        assert_eq!(inner_max_puts.items[1], expected_items[1]);
        assert_eq!(inner_max_puts.items[2], expected_items[2]);

        let request_start_key = Request::new(proto::ScanFromRunRequest {
            run_ids: vec![run_id_str.clone()],
            exclusive_start_key: "b_key2".to_string(),
            max_results: 10,
        });

        let response_start_key = cache_service
            .scan_from_run(request_start_key)
            .await
            .unwrap();
        let inner_start_key = response_start_key.into_inner();
        assert_eq!(inner_start_key.items.len(), 2);
        assert_eq!(inner_start_key.items[0], expected_items[2]);
        assert_eq!(inner_start_key.items[1], expected_items[3]);

        let request_invalid_max_zero = Request::new(proto::ScanFromRunRequest {
            run_ids: vec![run_id_str.clone()],
            exclusive_start_key: "".to_string(),
            max_results: 0,
        });
        assert!(
            cache_service
                .scan_from_run(request_invalid_max_zero)
                .await
                .is_err()
        );

        let request_invalid_max_large = Request::new(proto::ScanFromRunRequest {
            run_ids: vec![run_id_str.clone()],
            exclusive_start_key: "".to_string(),
            max_results: 10001,
        });
        assert!(
            cache_service
                .scan_from_run(request_invalid_max_large)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_scan_from_run_multiple_runs() {
        requires_docker!();
        let (object_store, _container) = setup_test_object_store().await.unwrap();
        let cache_service = MyCache::new(object_store.clone()).await.unwrap();

        let run_id_1_str = "scan_multi_run_1".to_string();
        let run_id_1 = RunId(run_id_1_str.clone());
        let ops1 = vec![
            WriteOperation::Put("apple".to_string(), Bytes::from("red_from_run1").to_vec()),
            WriteOperation::Put(
                "banana".to_string(),
                Bytes::from("yellow_from_run1").to_vec(),
            ),
        ];
        create_and_store_run(&object_store, &run_id_1, ops1.clone())
            .await
            .unwrap();
        let proto_items1: Vec<proto::GetFromRunItem> =
            ops1.iter().cloned().map(Into::into).collect();

        let run_id_2_str = "scan_multi_run_2".to_string();
        let run_id_2 = RunId(run_id_2_str.clone());
        let ops2 = vec![
            WriteOperation::Put(
                "banana".to_string(),
                Bytes::from("green_from_run2").to_vec(),
            ),
            WriteOperation::Delete("cherry".to_string()),
        ];
        create_and_store_run(&object_store, &run_id_2, ops2.clone())
            .await
            .unwrap();
        let proto_items2: Vec<proto::GetFromRunItem> =
            ops2.iter().cloned().map(Into::into).collect();

        let request = Request::new(proto::ScanFromRunRequest {
            run_ids: vec![run_id_2_str.clone(), run_id_1_str.clone()],
            exclusive_start_key: "".to_string(),
            max_results: 10,
        });

        let response = cache_service.scan_from_run(request).await.unwrap();
        let inner = response.into_inner();

        assert_eq!(inner.items.len(), 3);
        assert_eq!(inner.items[0], proto_items1[0]);
        assert_eq!(inner.items[1], proto_items2[0]);
        assert_eq!(inner.items[2], proto_items2[1]);
    }
}
