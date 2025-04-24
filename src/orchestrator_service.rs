use tonic::{Request, Response, Status};

use crate::metadata::MetadataStore;
use crate::proto::orchestrator_server::Orchestrator;
use crate::proto::{OrchestrateRequest, OrchestrateResponse};

pub struct MyOrchestrator {
    #[allow(dead_code)]
    metadata: MetadataStore,
}

impl MyOrchestrator {
    pub fn new(metadata: MetadataStore) -> Self {
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
