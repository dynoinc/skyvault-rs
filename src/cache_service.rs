use thiserror::Error;
use tonic::{Request, Response, Status};

use crate::proto::cache_service_server::CacheService;
use crate::proto;
use crate::storage::{self, StorageCache};

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

            let run = match self
                .storage_cache
                .get_run(crate::runs::RunId(run_id.clone()))
                .await
            {
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
        _request: Request<proto::ScanFromRunRequest>,
    ) -> Result<Response<proto::ScanFromRunResponse>, Status> {
        todo!()
    }
}
