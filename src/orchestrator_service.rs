use tonic::{Request, Response, Status};

use crate::forest::{Forest, ForestError};
use crate::metadata::MetadataStore;
use crate::proto::orchestrator_server::Orchestrator;
use crate::proto::{
    self, DumpChangelogRequest, DumpChangelogResponse, ListRunsRequest, ListRunsResponse,
};

pub struct MyOrchestrator {
    #[allow(dead_code)]
    metadata: MetadataStore,
    forest: Forest,
}

impl MyOrchestrator {
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
        let state = self.forest.get_state();
        let runs: Vec<proto::RunMetadata> = state.wal.iter().cloned().map(|r| r.into()).collect();
        Ok(Response::new(ListRunsResponse { runs }))
    }

    async fn dump_changelog(
        &self,
        _request: Request<DumpChangelogRequest>,
    ) -> Result<Response<DumpChangelogResponse>, Status> {
        let (snapshot, _) = match self.metadata.get_changelog_snapshot().await {
            Ok((snapshot, last_seqno)) => (snapshot, last_seqno),
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        let next_id = match self.metadata.get_next_id().await {
            Ok(next_id) => next_id,
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        Ok(Response::new(DumpChangelogResponse {
            entries: snapshot.into_iter().map(|e| e.into()).collect(),
            next_seqno: next_id.next_seq_no,
            next_changelog_id: next_id.next_changelog_id,
        }))
    }
}
