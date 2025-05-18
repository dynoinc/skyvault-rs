use std::sync::Arc;

use futures::StreamExt;
use kube::Error as KubeError;
use prost::Message;
use thiserror::Error;
use tonic::{Request, Response, Status};
use futures::pin_mut;

use crate::forest::{Forest, ForestError, ForestImpl, Snapshot};
use crate::job_watcher::{self, JobChange, JobWatcherError};
use crate::metadata::{
    JobID, JobParams, Level, MetadataError, MetadataStore, SeqNo, SnapshotID, TableConfig, TableID,
    TableName,
};
use crate::runs::Stats;
use crate::storage::{self, ObjectStore};
use crate::{dynamic_config, metadata, proto};

#[derive(Clone)]
pub struct MyOrchestrator {
    metadata: MetadataStore,
    storage: ObjectStore,
    forest: Forest,
    k8s_client: kube::Client,
    dynamic_config: dynamic_config::SharedAppConfig,
    // Track Kubernetes jobs to prevent duplicate creation
    known_jobs: Arc<tokio::sync::RwLock<std::collections::HashMap<JobID, String>>>,
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
    
    #[error("Job watcher error: {0}")]
    JobWatcherError(#[from] JobWatcherError),
}

impl MyOrchestrator {
    pub async fn new(
        metadata: MetadataStore,
        storage: ObjectStore,
        k8s_client: kube::Client,
        namespace: String,
        dynamic_config: dynamic_config::SharedAppConfig,
    ) -> Result<Self, OrchestratorError> {
        let forest = ForestImpl::watch(metadata.clone(), storage.clone()).await?;
        
        // Create a jobs map for tracking
        let known_jobs = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
        
        let orchestrator = Self {
            metadata,
            storage,
            forest,
            k8s_client,
            dynamic_config,
            known_jobs,
        };

        // Start main loop in the background
        let orchestrator_clone = orchestrator.clone();
        tokio::spawn(async move {
            orchestrator_clone.run().await;
        });

        // Start job watcher in the background
        let orchestrator_clone = orchestrator.clone();
        tokio::spawn(async move {
            orchestrator_clone.watch_jobs(namespace).await;
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
                    tracing::error!(error = %e, "Failed to get latest snapshot ID");
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
                        tracing::error!(error = %e, "Failed to persist snapshot");
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

            if total_wal_size > 100_000_000 || state.wal.len() > 25 {
                tracing::info!(
                    "Total size of WALs {total_wal_size} exceeds 100MB or number of WALs {} \
                     exceeds 25, scheduling compaction",
                    state.wal.len()
                );

                if let Err(e) = self.metadata.schedule_job(JobParams::WALCompaction).await {
                    tracing::error!(error = %e, "Failed to schedule WAL compaction");
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

                if total_buffer_size > 100_000_000 || table.buffer.len() > 25 {
                    tracing::info!(
                        "{table_id} table total size of buffer {total_buffer_size} exceeds 100MB \
                         or number of buffer runs {} exceeds 25, scheduling compaction",
                        table.buffer.len()
                    );

                    if let Err(e) = self
                        .metadata
                        .schedule_job(JobParams::TableBufferCompaction(*table_id))
                        .await
                    {
                        tracing::error!(error = %e, "Failed to schedule table buffer compaction");
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
                            tracing::error!(error = %e, "Failed to schedule table tree compaction");
                        }
                    }

                    max_level_size *= 10;
                }
            }

            // Check if there are pending jobs to kickoff
            let pending_jobs = match self.metadata.get_pending_jobs().await {
                Ok(jobs) => jobs,
                Err(e) => {
                    tracing::error!(error = %e, "Failed to get pending jobs");
                    continue;
                },
            };

            // Get a read lock on the job map to check which ones are already running
            let known_jobs = self.known_jobs.read().await;

            for job in pending_jobs {
                let current_job_id = job.0; // This is JobID
                
                // Skip if this job is already known to be running in Kubernetes
                if known_jobs.contains_key(&current_job_id) {
                    tracing::debug!("Skipping job ID: {} - already tracked in Kubernetes", current_job_id);
                    continue;
                }
                
                match job {
                    (id, JobParams::WALCompaction) => {
                        tracing::info!("Kicking off WAL compaction job with ID: {}", id);
                        if let Err(e) = self.create_k8s_job(id.to_string(), "wal-compaction").await
                        {
                            tracing::error!(error = %e, "Failed to create k8s job for WAL compaction");
                        }
                    },
                    (id, JobParams::TableBufferCompaction(_)) => {
                        tracing::info!("Kicking off table buffer compaction job with ID: {}", id);
                        if let Err(e) = self
                            .create_k8s_job(id.to_string(), "table-buffer-compaction")
                            .await
                        {
                            tracing::error!(error = %e, "Failed to create k8s job for table buffer compaction");
                        }
                    },
                    (id, JobParams::TableTreeCompaction(_, _)) => {
                        tracing::info!("Kicking off table tree compaction job with ID: {}", id);
                        if let Err(e) = self
                            .create_k8s_job(id.to_string(), "table-tree-compaction")
                            .await
                        {
                            tracing::error!(error = %e, "Failed to create k8s job for table tree compaction");
                        }
                    },
                }
            }
        }
    }

    async fn watch_jobs(&self, namespace: String) {
        let label_selector = "app=orchestrator";
        
        // Create a job watcher that will reconnect on failures
        loop {
            match job_watcher::watch(
                self.k8s_client.clone(), 
                namespace.clone(), 
                label_selector, 
            ).await {
                Ok(job_stream_unpinned) => {
                    // Process job events
                    let job_stream = job_stream_unpinned;
                    pin_mut!(job_stream);
                    while let Some(job_event) = job_stream.next().await {
                        match job_event {
                            Ok(job_change) => {
                                match job_change {
                                    JobChange::Added(job_id, job_name) => {
                                        // Add job to known jobs
                                        let mut jobs_guard = self.known_jobs.write().await;
                                        tracing::info!("Job {} (ID: {}) added to tracking.", job_name, job_id);
                                        jobs_guard.insert(job_id, job_name);
                                    },
                                    JobChange::Completed(job_id, job_name) => {
                                        // Remove from tracking
                                        let mut jobs_guard = self.known_jobs.write().await;
                                        if jobs_guard.remove(&job_id).is_some() {
                                            tracing::info!("Job {} (ID: {}) completed successfully and removed from tracking.", job_name, job_id);
                                        } else {
                                            tracing::warn!("Job {} (ID: {}) completed but was not in tracking map.", job_name, job_id);
                                        }
                                        // Worker process updates the DB after success, so no DB update here for success.
                                    },
                                    JobChange::Failed(job_id, job_name) => {
                                        // Remove from tracking and mark as failed
                                        let mut jobs_guard = self.known_jobs.write().await;
                                        if jobs_guard.remove(&job_id).is_some() {
                                            tracing::warn!("Job {} (ID: {}) failed (or was deleted) and removed from tracking.", job_name, job_id);
                                        } else {
                                            tracing::warn!("Job {} (ID: {}) failed (or was deleted) but was not in tracking map.", job_name, job_id);
                                        }
                                            
                                        // Mark job as failed in database
                                        if let Err(e) = self.metadata.mark_job_failed(job_id).await {
                                            tracing::error!(error = %e, job_id = %job_id, "Failed to update job status in DB to failed");
                                        }
                                    }
                                }
                            },
                            Err(e) => {
                                tracing::error!(error = %e, "Error in job watcher stream");
                                break; // Exit this stream and reconnect
                            }
                        }
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, "Failed to create job watcher");
                }
            }
            
            // If we get here, the stream has ended or failed - wait and reconnect
            tracing::info!("Job watcher terminated, restarting in 10 seconds");
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
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

        // Get job retry limit from dynamic config
        let retry_limit = {
            let config = self.dynamic_config.read().await;
            config.job_retry_limit
        };
        
        tracing::info!("Creating K8s job with retry limit of {}", retry_limit);

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
                        service_account_name: Some("skyvault-serviceaccount".to_string()),
                        containers: vec![k8s_openapi::api::core::v1::Container {
                            name: "worker".to_string(),
                            command: Some(vec![
                                "/app/worker".to_string(),
                                "--job-id".to_string(),
                                job_id.clone(),
                            ]),
                            image: std::env::var("SKYVAULT_IMAGE_ID").ok(),
                            env: Some(vec![
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "SKYVAULT_POSTGRES_USER".to_string(),
                                    value: std::env::var("SKYVAULT_POSTGRES_USER").ok(),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "SKYVAULT_POSTGRES_HOST".to_string(),
                                    value: std::env::var("SKYVAULT_POSTGRES_HOST").ok(),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "SKYVAULT_POSTGRES_PORT".to_string(),
                                    value: std::env::var("SKYVAULT_POSTGRES_PORT").ok(),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "SKYVAULT_POSTGRES_DB".to_string(),
                                    value: std::env::var("SKYVAULT_POSTGRES_DB").ok(),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "SKYVAULT_POSTGRES_SSLMODE".to_string(),
                                    value: std::env::var("SKYVAULT_POSTGRES_SSLMODE").ok(),
                                    value_from: None,
                                },
                                k8s_openapi::api::core::v1::EnvVar {
                                    name: "SKYVAULT_S3_BUCKET".to_string(),
                                    value: std::env::var("SKYVAULT_S3_BUCKET").ok(),
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
                            ]),
                            ..k8s_openapi::api::core::v1::Container::default()
                        }],
                        restart_policy: Some("Never".to_string()),
                        ..k8s_openapi::api::core::v1::PodSpec::default()
                    }),
                },
                backoff_limit: Some(retry_limit),
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
        let job_id = metadata::JobID::from(request.into_inner().job_id);
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
