use std::sync::Arc;

use slog::{debug, info};
use slog_scope;
use tonic::{Request, Response, Status};

use crate::{metadata, skyvault, storage, Batcher};

pub struct MyBatcher {
    _metadata: Arc<dyn metadata::MetadataStore>,
    _storage: Arc<dyn storage::ObjectStore>,
}

impl MyBatcher {
    pub fn new(metadata: Arc<dyn metadata::MetadataStore>, storage: Arc<dyn storage::ObjectStore>) -> Self {
        Self { _metadata: metadata, _storage: storage }
    }
}

#[tonic::async_trait]
impl Batcher for MyBatcher {
    async fn batch_process(
        &self,
        req: Request<skyvault::BatchRequest>,
    ) -> Result<Response<skyvault::BatchResponse>, Status> {
        let log = slog_scope::logger();
        let request_id = req
            .metadata()
            .get("x-request-id")
            .map(|v| v.to_str().unwrap_or("unknown"))
            .unwrap_or("unknown");

        info!(log, "Received batch request"; "request_id" => request_id);
        debug!(log, "Processing batch request"; "request" => ?req.get_ref());

        // Process the batch request here

        let response = skyvault::BatchResponse {};
        info!(log, "Batch request processed successfully"; "request_id" => request_id);

        Ok(Response::new(response))
    }
}
