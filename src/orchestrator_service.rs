use std::sync::Arc;

use slog::{debug, info};
use slog_scope;
use tonic::{Request, Response, Status};

use crate::{Orchestrator, skyvault, metadata};

pub struct MyOrchestrator {
    _metadata: Arc<dyn metadata::MetadataStore>,
}

impl MyOrchestrator {
    pub fn new(metadata: Arc<dyn metadata::MetadataStore>) -> Self {
        Self { _metadata: metadata }
    }
}

#[tonic::async_trait]
impl Orchestrator for MyOrchestrator {
    async fn orchestrate(
        &self,
        req: Request<skyvault::OrchestrateRequest>,
    ) -> Result<Response<skyvault::OrchestrateResponse>, Status> {
        let log = slog_scope::logger();
        let request_id = req
            .metadata()
            .get("x-request-id")
            .map(|v| v.to_str().unwrap_or("unknown"))
            .unwrap_or("unknown");

        info!(log, "Received orchestrate request"; "request_id" => request_id);
        debug!(log, "Processing orchestrate request"; "request" => ?req.get_ref());

        // Process the orchestrate request here

        let response = skyvault::OrchestrateResponse {};
        info!(log, "Orchestrate request processed successfully"; "request_id" => request_id);

        Ok(Response::new(response))
    }
}
