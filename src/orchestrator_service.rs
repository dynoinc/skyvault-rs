use tonic::{Request, Response, Status};

use crate::forest::{Forest, ForestError};
use crate::metadata::MetadataStore;
use crate::proto::orchestrator_server::Orchestrator;
use crate::proto::{ListRunsRequest, ListRunsResponse};

pub struct MyOrchestrator {
    #[allow(dead_code)]
    metadata: MetadataStore,
    forest: Forest,
}

impl MyOrchestrator {
    #[must_use]
    pub async fn new(metadata: MetadataStore) -> Result<Self, ForestError> {
        let forest = Forest::new(metadata.clone()).await?;
        Ok(Self { metadata, forest })
    }
}

#[tonic::async_trait]
impl Orchestrator for MyOrchestrator {
    async fn list_runs(
        &self,
        _request: Request<ListRunsRequest>,
    ) -> Result<Response<ListRunsResponse>, Status> {
        let runs = self.forest.get_live_runs();
        Ok(Response::new(ListRunsResponse { runs: runs.values().cloned().collect() }))
    }
}
