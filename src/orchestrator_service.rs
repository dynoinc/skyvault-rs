use std::sync::Arc;

use kube::Error as KubeError;
use prost::Message;
use thiserror::Error;
use tonic::{Request, Response, Status};

use crate::forest::{Forest, ForestError, ForestImpl, Snapshot};
use crate::metadata::{
    JobParams, Level, MetadataError, MetadataStore, SeqNo, SnapshotID, TableConfig, TableID,
    TableName,
};
use crate::runs::Stats;
use crate::storage::{self, ObjectStore};
use crate::{Config, k8s, metadata, proto};

#[derive(Clone)]
pub struct MyOrchestrator {
    config: Config,
    metadata: MetadataStore,
    storage: ObjectStore,
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

    #[error("Metadata error: {0}")]
    MetadataError(#[from] metadata::MetadataError),

    #[error("Storage error: {0}")]
    StorageError(#[from] storage::StorageError),
}

impl MyOrchestrator {
    pub async fn new(
        metadata: MetadataStore,
        storage: ObjectStore,
        config: Config,
    ) -> Result<Self, OrchestratorError> {
        let forest = ForestImpl::watch(metadata.clone(), storage.clone()).await?;
        let k8s_client = k8s::create_k8s_client().await?;
        let orchestrator = Self {
            metadata,
            storage,
            forest,
            k8s_client,
            config,
        };

        let orchestrator_clone = orchestrator.clone();
        tokio::spawn(async move {
            orchestrator_clone.run().await;
        });

        Ok(orchestrator)
    }
}

impl MyOrchestrator {
    pub async fn run(&self) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));

        loop {
            interval.tick().await;

            let state = self.forest.get_state();

            // If we have more than 5000 entries in the changelog, persist a new snapshot
            if match self.metadata.get_latest_snapshot_id().await {
                Ok(Some((_, seq_no))) => i64::from(state.seq_no) - i64::from(seq_no) > 5000,
                Ok(None) => i64::from(state.seq_no) > 5000,
                Err(e) => {
                    tracing::error!("Failed to get latest snapshot ID: {}", e);
                    false
                },
            } {
                match self.persist_snapshot(state.clone()).await {
                    Ok((snapshot_id, seq_no)) => {
                        tracing::info!(
                            "Successfully persisted snapshot {} at seq_no {}",
                            snapshot_id,
                            seq_no
                        );
                    },
                    Err(e) => {
                        tracing::error!("Failed to persist snapshot: {}", e);
                    },
                }
            }

            // If total size of WALs exceeds 100MB, schedule a job to compact the WALs
            let total_wal_size = state
                .wal
                .values()
                .map(|r| match &r.stats {
                    Stats::StatsV1(stats) => stats.size_bytes,
                })
                .sum::<u64>();

            if total_wal_size > 100_000_000 {
                tracing::info!(
                    "Total size of WALs {total_wal_size} exceeds 100MB, scheduling compaction",
                );

                if let Err(e) = self.metadata.schedule_job(JobParams::WALCompaction).await {
                    tracing::error!("Failed to schedule WAL compaction: {}", e);
                }
            }

            for (table_id, table) in state.tables.iter() {
                let total_buffer_size = table
                    .buffer
                    .values()
                    .map(|r| match &r.stats {
                        Stats::StatsV1(stats) => stats.size_bytes,
                    })
                    .sum::<u64>();

                if total_buffer_size > 100_000_000 {
                    tracing::info!(
                        "{table_id} table total size of buffer {total_buffer_size} exceeds 100MB, \
                         scheduling compaction"
                    );

                    if let Err(e) = self
                        .metadata
                        .schedule_job(JobParams::TableBufferCompaction(*table_id))
                        .await
                    {
                        tracing::error!("Failed to schedule table buffer compaction: {}", e);
                    }
                }

                let mut max_level_size = 1_000_000_000;
                for (level, runs) in table.tree.iter() {
                    let total_level_size = runs
                        .values()
                        .map(|r| match &r.stats {
                            Stats::StatsV1(stats) => stats.size_bytes,
                        })
                        .sum::<u64>();

                    if total_level_size > max_level_size {
                        tracing::info!(
                            "Level {level} size {total_level_size} exceeds max level size \
                             {max_level_size}, scheduling compaction"
                        );

                        if let Err(e) = self
                            .metadata
                            .schedule_job(JobParams::TableTreeCompaction(*table_id, *level))
                            .await
                        {
                            tracing::error!("Failed to schedule table tree compaction: {}", e);
                        }
                    }

                    max_level_size *= 10;
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
                    (id, JobParams::TableTreeCompaction(_, _)) => {
                        tracing::info!("Kicking off table tree compaction job with ID: {}", id);
                        if let Err(e) = self
                            .create_k8s_job(id.to_string(), "table-tree-compaction")
                            .await
                        {
                            tracing::error!(
                                "Failed to create k8s job for table tree compaction: {}",
                                e
                            );
                        }
                    },
                }
            }
        }
    }

    async fn persist_snapshot(
        &self,
        state: Arc<Snapshot>,
    ) -> Result<(SnapshotID, SeqNo), OrchestratorError> {
        let snapshot = proto::Snapshot::encode_to_vec(&Arc::unwrap_or_clone(state.clone()).into());
        let snapshot_id = metadata::SnapshotID::from(ulid::Ulid::new().to_string());
        self.storage
            .put_snapshot(snapshot_id.clone(), bytes::Bytes::from(snapshot))
            .await?;

        self.metadata
            .persist_snapshot(snapshot_id.clone(), state.seq_no)
            .await?;

        Ok((snapshot_id, state.seq_no))
    }

    async fn create_k8s_job(&self, job_id: String, job_type: &str) -> Result<(), kube::Error> {
        use k8s_openapi::api::batch::v1::Job;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        use kube::api::{Api, PostParams};

        let jobs: Api<Job> = Api::default_namespaced(self.k8s_client.clone());

        // Create a Kubernetes job specification
        let job_name = format!("{job_type}-{job_id}");
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
                        service_account_name: Some(self.config.worker_service_account_name.clone()),
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
                                    name: "POSTGRES_USER".to_string(),
                                    value: Some(self.config.postgres.user.clone()),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRES_HOST".to_string(),
                                    value: Some(self.config.postgres.host.clone()),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRES_PORT".to_string(),
                                    value: Some(self.config.postgres.port.to_string()),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRES_DB".to_string(),
                                    value: Some(self.config.postgres.db_name.clone()),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "POSTGRES_SSLMODE".to_string(),
                                    value: Some(self.config.postgres.sslmode.clone()),
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
                                    name: "AWS_ENDPOINT_URL_S3".to_string(),
                                    value: std::env::var("AWS_ENDPOINT_URL_S3").ok(),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "RUST_LOG".to_string(),
                                    value: std::env::var("RUST_LOG").ok(),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "AWS_ACCESS_KEY_ID".to_string(),
                                    value: None,
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(
                                            k8s_openapi::api::core::v1::SecretKeySelector {
                                                name: k8s::AWS_SECRET_NAME.to_string(),
                                                key: k8s::AWS_ACCESS_KEY_ID_KEY.to_string(),
                                                ..Default::default()
                                            },
                                        ),
                                        ..Default::default()
                                    }),
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "AWS_SECRET_ACCESS_KEY".to_string(),
                                    value: None,
                                    value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                        secret_key_ref: Some(
                                            k8s_openapi::api::core::v1::SecretKeySelector {
                                                name: k8s::AWS_SECRET_NAME.to_string(),
                                                key: k8s::AWS_SECRET_ACCESS_KEY_KEY.to_string(),
                                                ..Default::default()
                                            },
                                        ),
                                        ..Default::default()
                                    }),
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
impl proto::orchestrator_service_server::OrchestratorService for MyOrchestrator {
    async fn dump_snapshot(
        &self,
        _request: Request<proto::DumpSnapshotRequest>,
    ) -> Result<Response<proto::DumpSnapshotResponse>, Status> {
        let state = self.forest.get_state();

        Ok(Response::new(proto::DumpSnapshotResponse {
            snapshot: Some(Arc::unwrap_or_clone(state).into()),
        }))
    }

    async fn dump_changelog(
        &self,
        request: Request<proto::DumpChangelogRequest>,
    ) -> Result<Response<proto::DumpChangelogResponse>, Status> {
        let from_seq_no = SeqNo::from(request.into_inner().from_seq_no);
        let entries = match self.metadata.get_changelog(from_seq_no).await {
            Ok(entries) => entries,
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        Ok(Response::new(proto::DumpChangelogResponse {
            entries: entries.into_iter().map(|e| e.into()).collect(),
        }))
    }

    async fn kick_off_job(
        &self,
        request: Request<proto::KickOffJobRequest>,
    ) -> Result<Response<proto::KickOffJobResponse>, Status> {
        let r = match request.into_inner().job {
            Some(proto::kick_off_job_request::Job::WalCompaction(true)) => {
                self.metadata.schedule_job(JobParams::WALCompaction).await
            },
            Some(proto::kick_off_job_request::Job::TableBufferCompaction(table_id)) => {
                self.metadata
                    .schedule_job(JobParams::TableBufferCompaction(TableID::from(table_id)))
                    .await
            },
            Some(proto::kick_off_job_request::Job::TableTreeCompaction(table_tree_compaction)) => {
                self.metadata
                    .schedule_job(JobParams::TableTreeCompaction(
                        TableID::from(table_tree_compaction.table_id),
                        Level::from(table_tree_compaction.level),
                    ))
                    .await
            },
            _ => return Err(Status::invalid_argument("Invalid job type")),
        };

        let job_id = match r {
            Ok(job_id) => job_id,
            Err(e) => return Err(Status::internal(format!("Failed to schedule job: {e}"))),
        };

        Ok(Response::new(proto::KickOffJobResponse {
            job_id: job_id.into(),
        }))
    }

    async fn get_job_status(
        &self,
        request: Request<proto::GetJobStatusRequest>,
    ) -> Result<Response<proto::GetJobStatusResponse>, Status> {
        let job_id = metadata::JobId::from(request.into_inner().job_id);
        let status = match self.metadata.get_job_status(job_id).await {
            Ok(status) => status,
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        Ok(Response::new(proto::GetJobStatusResponse {
            status: Some(status.into()),
        }))
    }

    async fn persist_snapshot(
        &self,
        _request: Request<proto::PersistSnapshotRequest>,
    ) -> Result<Response<proto::PersistSnapshotResponse>, Status> {
        let state = self.forest.get_state();
        let (snapshot_id, seq_no) = match self.persist_snapshot(state).await {
            Ok((snapshot_id, seq_no)) => (snapshot_id, seq_no),
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        Ok(Response::new(proto::PersistSnapshotResponse {
            snapshot_id: snapshot_id.into(),
            seq_no: seq_no.into(),
        }))
    }

    async fn create_table(
        &self,
        request: Request<proto::CreateTableRequest>,
    ) -> Result<Response<proto::CreateTableResponse>, Status> {
        let request = request.into_inner();
        let table_name = TableName::from(request.table_name);
        let config = request.config.map(TableConfig::from).unwrap_or_default();

        let table_id = match self.metadata.create_table(table_name, config).await {
            Ok(table_id) => table_id,
            Err(MetadataError::TableAlreadyExists(_)) => {
                return Err(Status::already_exists("Table already exists"));
            },
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        Ok(Response::new(proto::CreateTableResponse {
            table_id: table_id.into(),
        }))
    }

    async fn drop_table(
        &self,
        request: Request<proto::DropTableRequest>,
    ) -> Result<Response<proto::DropTableResponse>, Status> {
        let request = request.into_inner();
        let table_name = TableName::from(request.table_name);

        match self.metadata.drop_table(table_name).await {
            Ok(_) => Ok(Response::new(proto::DropTableResponse {})),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn get_table(
        &self,
        request: Request<proto::GetTableRequest>,
    ) -> Result<Response<proto::GetTableResponse>, Status> {
        let request = request.into_inner();
        let table_name = TableName::from(request.table_name);

        let (table_id, config) = match self.metadata.get_table(table_name).await {
            Ok((table_id, config)) => (table_id, config),
            Err(MetadataError::TableNotFound(_)) => {
                return Err(Status::not_found("Table not found"));
            },
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        Ok(Response::new(proto::GetTableResponse {
            table_id: table_id.into(),
            config: Some(config.into()),
        }))
    }

    async fn list_tables(
        &self,
        _request: Request<proto::ListTablesRequest>,
    ) -> Result<Response<proto::ListTablesResponse>, Status> {
        let tables = match self.metadata.list_tables().await {
            Ok(tables) => tables,
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        Ok(Response::new(proto::ListTablesResponse {
            table_names: tables.into_iter().map(|t| t.to_string()).collect(),
        }))
    }
}
