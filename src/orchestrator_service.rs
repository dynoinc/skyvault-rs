use kube::Error as KubeError;
use thiserror::Error;
use tonic::{Request, Response, Status};

use crate::forest::{Forest, ForestError};
use crate::metadata::{JobParams, MetadataStore, TableName};
use crate::proto::orchestrator_service_server::OrchestratorService;
use crate::proto::{
    self, DumpChangelogRequest, DumpChangelogResponse, DumpForestRequest, DumpForestResponse,
    GetJobStatusRequest, GetJobStatusResponse, KickOffJobRequest, KickOffJobResponse,
};
use crate::{Config, metadata};

#[derive(Clone)]
pub struct MyOrchestrator {
    config: Config,
    metadata: MetadataStore,
    forest: Forest,

    k8s_client: kube::Client,
}

#[derive(Debug, Error)]
pub enum OrchestratorError {
    #[error("Kube client error: {0}")]
    K8sClientError(#[from] kube::Error),

    #[error("Forest error: {0}")]
    ForestError(#[from] ForestError),

    #[error("Failed to get hostname: {0}")]
    HostnameError(#[from] std::io::Error),
}

impl MyOrchestrator {
    pub async fn new(metadata: MetadataStore, config: Config) -> Result<Self, OrchestratorError> {
        let forest = Forest::new(metadata.clone()).await?;
        let k8s_client = kube::Client::try_default().await?;
        let orchestrator = Self {
            metadata,
            forest,
            k8s_client,
            config,
        };

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
            let total_wal_size = state
                .wal
                .values()
                .map(|r| match &r.stats {
                    crate::runs::Stats::StatsV1(stats) => stats.size_bytes,
                })
                .sum::<u64>();

            if total_wal_size > 100_000_000 {
                tracing::info!(
                    "Total size of WALs {total_wal_size} exceeds 100MB, scheduling compaction",
                );

                if let Err(e) = self.metadata.schedule_wal_compaction().await {
                    tracing::error!("Failed to schedule WAL compaction: {}", e);
                }
            }

            for (table_name, table) in state.tables.iter() {
                let total_buffer_size = table
                    .buffer
                    .values()
                    .map(|r| match &r.stats {
                        crate::runs::Stats::StatsV1(stats) => stats.size_bytes,
                    })
                    .sum::<u64>();

                if total_buffer_size > 100_000_000 {
                    tracing::info!(
                        "{table_name} table total size of buffer {total_buffer_size} exceeds \
                         100MB, scheduling compaction"
                    );

                    if let Err(e) = self
                        .metadata
                        .schedule_table_buffer_compaction(table_name.clone())
                        .await
                    {
                        tracing::error!("Failed to schedule table buffer compaction: {}", e);
                    }
                }
            }

            // Check if there are pending jobs to kickoff
            let pending_jobs = match self.metadata.get_pending_jobs().await {
                Ok(jobs) => jobs,
                Err(e) => {
                    tracing::error!("Failed to get pending jobs: {}", e);
                    continue;
                },
            };

            for job in pending_jobs {
                match job {
                    (id, JobParams::WALCompaction) => {
                        tracing::info!("Kicking off WAL compaction job with ID: {}", id);
                        if let Err(e) = self.create_k8s_job(id.to_string(), "wal-compaction").await
                        {
                            tracing::error!("Failed to create k8s job for WAL compaction: {}", e);
                        }
                    },
                    (id, JobParams::TableBufferCompaction(_)) => {
                        tracing::info!("Kicking off table buffer compaction job with ID: {}", id);
                        if let Err(e) = self
                            .create_k8s_job(id.to_string(), "table-buffer-compaction")
                            .await
                        {
                            tracing::error!(
                                "Failed to create k8s job for table buffer compaction: {}",
                                e
                            );
                        }
                    },
                }
            }
        }
    }

    async fn create_k8s_job(&self, job_id: String, job_type: &str) -> Result<(), kube::Error> {
        use k8s_openapi::api::batch::v1::Job;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        use kube::api::{Api, PostParams};

        let jobs: Api<Job> = Api::default_namespaced(self.k8s_client.clone());

        // Create a Kubernetes job specification
        let job_name = format!("{}-{}", job_type, job_id);
        let job = Job {
            metadata: ObjectMeta {
                name: Some(job_name.clone()),
                labels: Some(std::collections::BTreeMap::from([
                    ("app".to_string(), "orchestrator".to_string()),
                    ("job.kubernetes.io/type".to_string(), job_type.to_string()),
                ])),
                ..ObjectMeta::default()
            },
            spec: Some(k8s_openapi::api::batch::v1::JobSpec {
                template: k8s_openapi::api::core::v1::PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(std::collections::BTreeMap::from([
                            ("app".to_string(), "orchestrator".to_string()),
                            ("job.kubernetes.io/type".to_string(), job_type.to_string()),
                        ])),
                        ..ObjectMeta::default()
                    }),
                    spec: Some(k8s_openapi::api::core::v1::PodSpec {
                        containers: vec![k8s_openapi::api::core::v1::Container {
                            name: "worker".to_string(),
                            command: Some(vec![
                                "/app/worker".to_string(),
                                "--job-id".to_string(),
                                job_id.clone(),
                            ]),
                            image: Some(self.config.image_id.clone()),
                            env: Some(vec![
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "SKYVAULT_METADATA_URL".to_string(),
                                    value: Some(self.config.metadata_url.clone()),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "SKYVAULT_BUCKET_NAME".to_string(),
                                    value: Some(self.config.bucket_name.clone()),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "AWS_REGION".to_string(),
                                    value: std::env::var("AWS_REGION").ok(),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "AWS_ACCESS_KEY_ID".to_string(),
                                    value: std::env::var("AWS_ACCESS_KEY_ID").ok(),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "AWS_SECRET_ACCESS_KEY".to_string(),
                                    value: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "AWS_ENDPOINT_URL_S3".to_string(),
                                    value: std::env::var("AWS_ENDPOINT_URL_S3").ok(),
                                    value_from: None,
                                },
                            ]),
                            ..k8s_openapi::api::core::v1::Container::default()
                        }],
                        restart_policy: Some("Never".to_string()),
                        ..k8s_openapi::api::core::v1::PodSpec::default()
                    }),
                },
                backoff_limit: Some(3),
                ttl_seconds_after_finished: Some(3600), // Clean up after 1 hour
                ..k8s_openapi::api::batch::v1::JobSpec::default()
            }),
            ..Job::default()
        };

        // Try to create the job in Kubernetes, ignoring "already exists" errors
        match jobs.create(&PostParams::default(), &job).await {
            Ok(_) => {
                tracing::info!("Created k8s job for job ID: {}", job_id);
                Ok(())
            },
            Err(KubeError::Api(ae)) if ae.code == 409 && ae.reason == "AlreadyExists" => {
                tracing::info!("Job '{}' already exists", job.metadata.name.unwrap());
                Ok(())
            },
            Err(e) => Err(e),
        }
    }
}

#[tonic::async_trait]
impl OrchestratorService for MyOrchestrator {
    async fn dump_forest(
        &self,
        _request: Request<DumpForestRequest>,
    ) -> Result<Response<DumpForestResponse>, Status> {
        let state = self.forest.get_state();

        let mut response = DumpForestResponse {
            wal: state.wal.values().cloned().map(|r| r.into()).collect(),
            tables: vec![],
        };

        for (table_name, table) in state.tables.iter() {
            let mut table_response = proto::Table {
                name: table_name.to_string(),
                buffer: table.buffer.values().cloned().map(|r| r.into()).collect(),
                levels: vec![],
            };

            for (level, level_data) in table.tree.iter() {
                let level_response = proto::TableLevel {
                    level: (*level).into(),
                    tree: level_data.values().cloned().map(|r| r.into()).collect(),
                };

                table_response.levels.push(level_response);
            }

            response.tables.push(table_response);
        }
        Ok(Response::new(response))
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

    async fn kick_off_job(
        &self,
        request: Request<KickOffJobRequest>,
    ) -> Result<Response<KickOffJobResponse>, Status> {
        let r = match request.into_inner().job {
            Some(proto::kick_off_job_request::Job::WalCompaction(true)) => {
                self.metadata.schedule_wal_compaction().await
            },
            Some(proto::kick_off_job_request::Job::TableBufferCompaction(table_name)) => {
                self.metadata
                    .schedule_table_buffer_compaction(TableName::from(table_name))
                    .await
            },
            _ => return Err(Status::invalid_argument("Invalid job type")),
        };

        let job_id = match r {
            Ok(job_id) => job_id,
            Err(e) => return Err(Status::internal(format!("Failed to schedule job: {}", e))),
        };

        Ok(Response::new(KickOffJobResponse {
            job_id: job_id.into(),
        }))
    }

    async fn get_job_status(
        &self,
        request: Request<GetJobStatusRequest>,
    ) -> Result<Response<GetJobStatusResponse>, Status> {
        let job_id = metadata::JobId::from(request.into_inner().job_id);
        let status = match self.metadata.get_job_status(job_id).await {
            Ok(status) => status,
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        Ok(Response::new(GetJobStatusResponse {
            status: Some(status.into()),
        }))
    }
}
