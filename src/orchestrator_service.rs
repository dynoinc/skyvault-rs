use crate::metadata::MetadataStore;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::proto::orchestrator_server::Orchestrator;
use crate::proto::{OrchestrateRequest, OrchestrateResponse};

pub struct MyOrchestrator {
    #[allow(dead_code)]
    metadata: Arc<dyn MetadataStore>,
}

impl MyOrchestrator {
    pub fn new(metadata: Arc<dyn MetadataStore>) -> Self {
        Self { metadata }
    }
}

#[tonic::async_trait]
impl Orchestrator for MyOrchestrator {
    async fn orchestrate(
        &self,
        _request: Request<OrchestrateRequest>,
    ) -> Result<Response<OrchestrateResponse>, Status> {
        Ok(Response::new(OrchestrateResponse {}))
    }
}
