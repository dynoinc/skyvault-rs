use std::pin::Pin;

use futures::{Stream, StreamExt, TryStreamExt, pin_mut};
use thiserror::Error;
use tonic::{Request, Response, Status};

use crate::proto::cache_service_server::CacheService;
use crate::runs::{RunError, RunId, WriteOperation};
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
impl CacheService for MyCache {
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

            remaining_keys.retain(|key| {
                match crate::runs::search_run(run.as_ref(), key.as_str()) {
                    crate::runs::SearchResult::Found(value) => {
                        response.items.push(proto::GetFromRunItem {
                            key: key.clone(),
                            result: Some(proto::get_from_run_item::Result::Value(value)),
                        });

                        false
                    },
                    crate::runs::SearchResult::Tombstone => {
                        response.items.push(proto::GetFromRunItem {
                            key: key.clone(),
                            result: Some(proto::get_from_run_item::Result::Deleted(())),
                        });

                        false
                    },
                    crate::runs::SearchResult::NotFound => true,
                }
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
            let stream =
                crate::runs::read_run_stream(futures::stream::once(futures::future::ok(run_data)));

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
