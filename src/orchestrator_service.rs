use tonic::{Request, Response, Status};


use crate::forest::{Forest, ForestError};
use crate::metadata::MetadataStore;
use crate::proto::orchestrator_service_server::OrchestratorService;
use crate::proto::{
    self, DumpChangelogRequest, DumpChangelogResponse, ListRunsRequest, ListRunsResponse,
};

#[derive(Clone)]
pub struct MyOrchestrator {
    #[allow(dead_code)]
    metadata: MetadataStore,
    forest: Forest,
}

impl MyOrchestrator {
    pub async fn new(metadata: MetadataStore) -> Result<Self, ForestError> {
        let forest = Forest::new(metadata.clone()).await?;
        let orchestrator = Self { metadata, forest };
        
        // Spawn a background task to periodically refresh the forest
        let orchestrator_clone = orchestrator.clone();
        tokio::spawn(async move {
            orchestrator_clone.schedule_compaction().await;
        });
        
        Ok(orchestrator)
    }
}

impl MyOrchestrator {
    pub async fn schedule_compaction(&self) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

        loop {
            interval.tick().await;

            let state = self.forest.get_state();

            // If total size of WALs exceeds 100MB, schedule a job to compact the WALs
            let total_size = state.wal.iter().map(|r| 
                match &r.stats {
                    crate::runs::Stats::StatsV1(stats) => stats.size_bytes,
                }).sum::<u64>();

            if total_size > 100_000_000 {
                tracing::info!("Total size of WALs {} exceeds 100MB, scheduling compaction", total_size);

                if let Err(e) = self.metadata.schedule_wal_compaction().await {
                    tracing::error!("Failed to schedule WAL compaction: {}", e);
                }
            }

            // Check if there are pending jobs to kickoff
            let _pending_jobs = match self.metadata.get_pending_jobs().await {
                Ok(jobs) => jobs,
                Err(e) => {
                    tracing::error!("Failed to get pending jobs: {}", e);
                    continue;
                }
            };
            
        }
    }
}

#[tonic::async_trait]
impl OrchestratorService for MyOrchestrator {
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
        let snapshot = match self.metadata.get_changelog_snapshot().await {
            Ok((snapshot, _)) => snapshot,
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        Ok(Response::new(DumpChangelogResponse {
            entries: snapshot.into_iter().map(|e| e.into()).collect(),
        }))
    }
}
