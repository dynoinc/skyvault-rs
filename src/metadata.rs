use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use futures::Stream;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::JsonValue;
use thiserror::Error;

use crate::proto;

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("Database connection error: {0}")]
    DatabaseConnectionError(#[from] sqlx::Error),

    #[error("Database migration error: {0}")]
    DatabaseMigrationError(#[from] sqlx::migrate::MigrateError),

    #[error("JSON serde error: {0}")]
    JsonSerdeError(#[from] serde_json::Error),

    #[error("Some runs were already marked as deleted")]
    AlreadyDeleted(String),

    #[error("Job is not in pending state")]
    InvalidJobState(String),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum BelongsTo {
    WalSeqNo(i64),
    TableBuffer(String, i64),
    TableTree(String, u64),
}

impl From<BelongsTo> for proto::run_metadata::BelongsTo {
    fn from(belongs_to: BelongsTo) -> Self {
        match belongs_to {
            BelongsTo::WalSeqNo(seq_no) => proto::run_metadata::BelongsTo::WalSeqNo(seq_no),
            BelongsTo::TableBuffer(table_name, seq_no) => {
                proto::run_metadata::BelongsTo::TableBuffer(proto::TableBuffer {
                    table_name,
                    seq_no,
                })
            },
            BelongsTo::TableTree(table_name, level) => {
                proto::run_metadata::BelongsTo::TableTree(proto::TableTree { table_name, level })
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct RunMetadata {
    pub id: crate::runs::RunId,
    pub belongs_to: BelongsTo,
    pub stats: crate::runs::Stats,
}

impl From<RunMetadata> for proto::RunMetadata {
    fn from(metadata: RunMetadata) -> Self {
        proto::RunMetadata {
            id: metadata.id.to_string(),
            belongs_to: Some(metadata.belongs_to.into()),
            stats: Some(metadata.stats.into()),
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ChangelogEntryV1 {
    pub runs_added: Vec<crate::runs::RunId>,
    pub runs_removed: Vec<crate::runs::RunId>,
}

impl From<ChangelogEntryV1> for proto::ChangelogEntryV1 {
    fn from(entry: ChangelogEntryV1) -> Self {
        proto::ChangelogEntryV1 {
            runs_added: entry.runs_added.iter().map(|id| id.to_string()).collect(),
            runs_removed: entry.runs_removed.iter().map(|id| id.to_string()).collect(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum ChangelogEntry {
    V1(ChangelogEntryV1),
}

impl From<JsonValue> for ChangelogEntry {
    fn from(val: JsonValue) -> Self {
        // panics on malformed JSON; switch to `from_value(val).unwrap()` or handle the Result
        serde_json::from_value(val).expect("invalid ChangelogEntry JSON")
    }
}

impl From<ChangelogEntry> for proto::ChangelogEntry {
    fn from(entry: ChangelogEntry) -> Self {
        match entry {
            ChangelogEntry::V1(v1) => proto::ChangelogEntry {
                entry: Some(proto::changelog_entry::Entry::V1(v1.into())),
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ChangelogEntryWithID {
    id: i64,
    changes: ChangelogEntry,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum JobParams {
    WALCompaction,
    TableBufferCompaction(String),
}

#[async_trait::async_trait]
pub trait MetadataStoreTrait: Send + Sync + 'static {
    async fn get_changelog_snapshot(&self) -> Result<(Vec<ChangelogEntry>, i64), MetadataError>;

    type ChangelogStream: Stream<Item = Result<ChangelogEntry, MetadataError>> + Send + 'static;

    async fn get_changelog(
        &self,
    ) -> Result<(Vec<ChangelogEntry>, Self::ChangelogStream), MetadataError>;

    async fn append_wal(
        &self,
        run_ids: Vec<(crate::runs::RunId, crate::runs::Stats)>,
    ) -> Result<(), MetadataError>;

    async fn append_wal_compaction(
        &self,
        job_id: i64,
        compacted: Vec<crate::runs::RunId>,
        new_table_runs: Vec<(crate::runs::RunId, String, crate::runs::Stats)>,
    ) -> Result<(), MetadataError>;

    async fn append_table_buffer_compaction(
        &self,
        job_id: i64,
        compacted: Vec<crate::runs::RunId>,
        new_runs: Vec<RunMetadata>,
    ) -> Result<(), MetadataError>;

    async fn get_run_metadata_batch(
        &self,
        run_ids: Vec<crate::runs::RunId>,
    ) -> Result<HashMap<crate::runs::RunId, RunMetadata>, MetadataError>;

    async fn schedule_wal_compaction(&self) -> Result<i64, MetadataError>;

    async fn schedule_table_buffer_compaction(
        &self,
        table_name: String,
    ) -> Result<i64, MetadataError>;

    async fn get_job_status(&self, job_id: i64) -> Result<String, MetadataError>;

    async fn get_pending_jobs(&self) -> Result<Vec<(i64, JobParams)>, MetadataError>;

    async fn get_job(&self, job_id: i64) -> Result<JobParams, MetadataError>;
}

pub type MetadataStore = Arc<
    dyn MetadataStoreTrait<
        ChangelogStream = Pin<
            Box<dyn Stream<Item = Result<ChangelogEntry, MetadataError>> + Send + 'static>,
        >,
    >,
>;

#[derive(Clone)]
pub struct PostgresMetadataStore {
    pg_pool: PgPool,
}

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

impl PostgresMetadataStore {
    pub async fn new(metadata_url: String) -> Result<Self, MetadataError> {
        let pg_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&metadata_url)
            .await?;

        MIGRATOR.run(&pg_pool).await?;

        Ok(Self { pg_pool })
    }

    /// Attempts to perform the append_wal operation within a single transaction.
    /// Returns Ok(()) on success, or sqlx::Error on failure.
    /// The caller is responsible for retry logic based on the error type.
    async fn _append_wal_attempt(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        run_ids_with_stats: &[(crate::runs::RunId, crate::runs::Stats)],
    ) -> Result<(), sqlx::Error> {
        let n = run_ids_with_stats.len();
        if n == 0 {
            return Ok(()); // Nothing to do
        }

        // --- Reserve Sequence Block ---
        // Assume sequence name is 'changelog_id_seq'. Verify this!
        let first_seq_no: i64 = sqlx::query_scalar("SELECT nextval('changelog_id_seq')")
            .fetch_one(&mut **transaction) // Deref the mutable reference
            .await?;

        if n > 1 {
            let last_seq_no = first_seq_no + (n as i64) - 1;
            sqlx::query("SELECT setval('changelog_id_seq', $1, true)")
                .bind(last_seq_no)
                .execute(&mut **transaction) // Deref the mutable reference
                .await?;
        }

        // --- Insert Changelog Entry ---
        let changelog_changes = serde_json::to_value(ChangelogEntry::V1(ChangelogEntryV1 {
            runs_added: run_ids_with_stats
                .iter()
                .map(|(id, _)| id.clone())
                .collect(),
            runs_removed: vec![],
        }))
        .map_err(|e| sqlx::Error::Configuration(e.into()))?; // Convert serde error appropriately if needed

        sqlx::query!(
            r#"
            INSERT INTO changelog (id, changes)
            VALUES ($1, $2)
            "#,
            first_seq_no,
            changelog_changes
        )
        .execute(&mut **transaction) // Deref the mutable reference
        .await?;

        // --- Prepare Runs Data ---
        let run_ids: Vec<String> = run_ids_with_stats
            .iter()
            .map(|(id, _)| id.to_string())
            .collect();

        let stats_values: Vec<serde_json::Value> = run_ids_with_stats
            .iter()
            .map(|(_, stats)| serde_json::to_value(stats))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| sqlx::Error::Configuration(e.into()))?; // Convert serde error

        let belongs_to_values: Vec<serde_json::Value> = (0..n)
            .map(|i| serde_json::to_value(BelongsTo::WalSeqNo(first_seq_no + i as i64)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| sqlx::Error::Configuration(e.into()))?; // Convert serde error

        // --- Insert Runs ---
        sqlx::query!(
            r#"
            INSERT INTO runs (id, belongs_to, stats)
            SELECT * FROM UNNEST($1::text[], $2::jsonb[], $3::jsonb[])
            "#,
            &run_ids,
            &belongs_to_values,
            &stats_values
        )
        .execute(&mut **transaction) // Deref the mutable reference
        .await?;

        Ok(())
    }

    /// Attempts to perform the append_wal_compaction operation within a single transaction.
    /// Returns Ok(()) on success, or sqlx::Error on failure.
    /// The caller is responsible for retry logic based on the error type.
    async fn _append_wal_compaction_attempt(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        job_id: i64,
        compacted: &[crate::runs::RunId],
        new_table_runs: &[(crate::runs::RunId, String, crate::runs::Stats)],
    ) -> Result<(), sqlx::Error> {
        // --- Mark compacted runs as deleted ---
        let compacted_strings: Vec<String> = compacted.iter().map(|id| id.to_string()).collect();
        let deleted_count_result = sqlx::query!(
            r#"
            WITH updated_runs AS (
                UPDATE runs
                SET deleted_at = NOW()
                WHERE id = ANY($1)
                AND deleted_at IS NULL
                RETURNING id
            )
            SELECT COUNT(*) FROM updated_runs
            "#,
            &compacted_strings
        )
        .fetch_one(&mut **transaction) // Deref
        .await?;

        let deleted_count = deleted_count_result.count.unwrap_or(0);
        if deleted_count as usize != compacted.len() {
            // Use a generic DB error here; mapping happens in the public fn
            return Err(sqlx::Error::Protocol(
                "Mismatch in number of deleted runs.".into(),
            ));
        }

        // --- Mark the job as completed ---
        let job_update_result = sqlx::query!(
            r#"
            WITH updated_jobs AS (
                UPDATE jobs
                SET status = 'completed'
                WHERE id = $1 AND status = 'pending'
                RETURNING id
            )
            SELECT COUNT(*) FROM updated_jobs
            "#,
            job_id
        )
        .fetch_one(&mut **transaction) // Deref
        .await?;

        if job_update_result.count.unwrap_or(0) != 1 {
            // Use a generic DB error here; mapping happens in the public fn
            return Err(sqlx::Error::Protocol(format!(
                "Job {} update failed or not in pending state.",
                job_id
            )));
        }

        // --- Reserve Sequence Block for Changelog and potential new runs ---
        let n_new_runs = new_table_runs.len();
        // Reserve N IDs if creating runs, otherwise reserve 1 ID for the changelog entry itself.
        let num_ids_to_reserve = std::cmp::max(1, n_new_runs);

        let first_seq_no: i64 = sqlx::query_scalar("SELECT nextval('changelog_id_seq')")
            .fetch_one(&mut **transaction) // Deref
            .await?;

        if num_ids_to_reserve > 1 {
            let last_seq_no = first_seq_no + (num_ids_to_reserve as i64) - 1;
            sqlx::query("SELECT setval('changelog_id_seq', $1, true)")
                .bind(last_seq_no)
                .execute(&mut **transaction) // Deref
                .await?;
        }
        // If num_ids_to_reserve == 1, nextval already advanced it correctly.

        // --- Insert Changelog Entry ---
        let changelog_changes = serde_json::to_value(ChangelogEntry::V1(ChangelogEntryV1 {
            runs_added: new_table_runs.iter().map(|(id, _, _)| id.clone()).collect(),
            runs_removed: compacted.to_vec(), // Clone if needed or take ownership
        }))
        .map_err(|e| sqlx::Error::Configuration(e.into()))?;

        sqlx::query!(
            r#"
            INSERT INTO changelog (id, changes)
            VALUES ($1, $2)
            "#,
            first_seq_no, // Use the first reserved ID for the changelog entry
            changelog_changes
        )
        .execute(&mut **transaction) // Deref
        .await?;

        // --- Insert New Runs (if any) ---
        if n_new_runs > 0 {
            let ids: Vec<String> = new_table_runs
                .iter()
                .map(|(id, _, _)| id.to_string())
                .collect();

            // Create unique BelongsTo::TableBuffer values using the reserved sequence block
            let belongs_to_values: Vec<serde_json::Value> = new_table_runs
                .iter()
                .enumerate()
                .map(|(i, (_, table_name, _))| {
                    // Assign unique sequence number: first_seq_no + i
                    serde_json::to_value(BelongsTo::TableBuffer(
                        table_name.clone(),
                        first_seq_no + i as i64, // Use unique ID here
                    ))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| sqlx::Error::Configuration(e.into()))?;

            let stats_values: Vec<serde_json::Value> = new_table_runs
                .iter()
                .map(|(_, _, stats)| serde_json::to_value(stats))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| sqlx::Error::Configuration(e.into()))?;

            // Use UNNEST to insert multiple runs correctly
            sqlx::query!(
                r#"
                INSERT INTO runs (id, belongs_to, stats)
                SELECT * FROM UNNEST($1::text[], $2::jsonb[], $3::jsonb[])
                "#,
                &ids,
                &belongs_to_values,
                &stats_values
            )
            .execute(&mut **transaction) // Deref
            .await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl MetadataStoreTrait for PostgresMetadataStore {
    /// Fetches all existing changelog entries and returns them as a vector.
    async fn get_changelog_snapshot(&self) -> Result<(Vec<ChangelogEntry>, i64), MetadataError> {
        let entries = sqlx::query_as!(
            ChangelogEntryWithID,
            "SELECT * FROM changelog ORDER BY id ASC"
        )
        .fetch_all(&self.pg_pool)
        .await?;

        let last_id = entries.last().map(|e| e.id).unwrap_or(0);
        Ok((entries.into_iter().map(|e| e.changes).collect(), last_id))
    }

    type ChangelogStream =
        Pin<Box<dyn Stream<Item = Result<ChangelogEntry, MetadataError>> + Send + 'static>>;

    /// Returns a snapshot of existing changelog entries and a stream of new entries.
    /// The stream continuously polls for new entries after reaching the end of the snapshot.
    async fn get_changelog(
        &self,
    ) -> Result<(Vec<ChangelogEntry>, Self::ChangelogStream), MetadataError> {
        let (snapshot, mut last_id) = self.get_changelog_snapshot().await?;

        // Clone pg_pool to avoid lifetime issues
        let pg_pool = self.pg_pool.clone();

        let stream = stream! {
            loop {
                let new_entries = sqlx::query_as!(
                    ChangelogEntryWithID,
                    "SELECT * FROM changelog WHERE id > $1 ORDER BY id ASC",
                    last_id
                )
                .fetch_all(&pg_pool)
                .await?;

                for entry in new_entries {
                    last_id = entry.id;
                    yield Ok(entry.changes);
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };

        Ok((snapshot, Box::pin(stream)))
    }

    async fn append_wal(
        &self,
        run_ids_with_stats: Vec<(crate::runs::RunId, crate::runs::Stats)>,
    ) -> Result<(), MetadataError> {
        loop {
            // Retry loop for transaction serialization failures
            let mut transaction = self.pg_pool.begin().await?;

            match self
                ._append_wal_attempt(&mut transaction, &run_ids_with_stats)
                .await
            {
                Ok(_) => {
                    // Attempt commit
                    match transaction.commit().await {
                        Ok(_) => return Ok(()), // Success! Exit function.
                        Err(commit_err) => {
                            // Check if commit error is retryable
                            if let sqlx::Error::Database(db_err) = &commit_err {
                                if db_err.code().is_some_and(|code| code == "40001") {
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                    continue; // Retry the whole transaction
                                }
                            }
                            // Non-retryable commit error
                            return Err(MetadataError::DatabaseConnectionError(commit_err));
                        },
                    }
                },
                Err(attempt_err) => {
                    // Rollback is implicitly handled by dropping the transaction on error return.
                    // Check if the attempt error is retryable
                    if let sqlx::Error::Database(db_err) = &attempt_err {
                        if db_err.code().is_some_and(|code| code == "40001") {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue; // Retry the whole transaction
                        }
                    }
                    // Non-retryable attempt error
                    return Err(MetadataError::DatabaseConnectionError(attempt_err));
                },
            }
        } // End of loop
    }

    async fn append_wal_compaction(
        &self,
        job_id: i64,
        compacted: Vec<crate::runs::RunId>,
        new_table_runs: Vec<(crate::runs::RunId, String, crate::runs::Stats)>,
    ) -> Result<(), MetadataError> {
        loop {
            // Retry loop
            let mut transaction = self.pg_pool.begin().await?;

            match self
                ._append_wal_compaction_attempt(
                    &mut transaction,
                    job_id,
                    &compacted,
                    &new_table_runs,
                )
                .await
            {
                Ok(_) => {
                    // Attempt commit
                    match transaction.commit().await {
                        Ok(_) => return Ok(()), // Success!
                        Err(commit_err) => {
                            if let sqlx::Error::Database(db_err) = &commit_err {
                                if db_err.code().is_some_and(|code| code == "40001") {
                                    tokio::time::sleep(Duration::from_millis(10)).await;
                                    continue; // Retry transaction
                                }
                            }
                            // Map non-retryable commit error
                            return Err(MetadataError::DatabaseConnectionError(commit_err));
                        },
                    }
                },
                Err(attempt_err) => {
                    // Rollback implicitly handled by drop
                    if let sqlx::Error::Database(db_err) = &attempt_err {
                        if db_err.code().is_some_and(|code| code == "40001") {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue; // Retry transaction
                        }
                    }
                    // Map non-retryable attempt error
                    // Need to check for the specific protocol errors we returned
                    if let sqlx::Error::Protocol(msg) = &attempt_err {
                        if msg.contains("Mismatch in number of deleted runs") {
                            return Err(MetadataError::AlreadyDeleted(msg.to_string()));
                        }
                        if msg.contains("Job") && msg.contains("update failed") {
                            return Err(MetadataError::InvalidJobState(msg.to_string()));
                        }
                    }
                    // Otherwise, assume it's a general DB connection error
                    return Err(MetadataError::DatabaseConnectionError(attempt_err));
                },
            }
        } // End loop
    }

    async fn append_table_buffer_compaction(
        &self,
        job_id: i64,
        compacted: Vec<crate::runs::RunId>,
        new_runs: Vec<RunMetadata>,
    ) -> Result<(), MetadataError> {
        let mut transaction = self.pg_pool.begin().await?;

        let compacted_strings: Vec<String> = compacted.iter().map(|id| id.to_string()).collect();
        let result = sqlx::query!(
            r#"
            WITH updated_runs AS (
                UPDATE runs
                SET deleted_at = NOW()
                WHERE id = ANY($1)
                AND deleted_at IS NULL
                RETURNING id
            )
            SELECT COUNT(*) FROM updated_runs
            "#,
            &compacted_strings
        )
        .fetch_one(&mut *transaction)
        .await?;

        // Check if we updated exactly the number of runs we expected to
        if result.count.unwrap_or(0) as usize != compacted.len() {
            return Err(MetadataError::AlreadyDeleted(
                "Some runs were already marked as deleted".to_string(),
            ));
        }

        // Mark the job as completed if it's pending
        let job_result = sqlx::query!(
            r#"
            WITH updated_jobs AS (
                UPDATE jobs
                SET status = 'completed'
                WHERE id = $1 AND status = 'pending'
                RETURNING id
            )
            SELECT COUNT(*) FROM updated_jobs
            "#,
            job_id
        )
        .fetch_one(&mut *transaction)
        .await?;

        // Check if we updated exactly one job
        if job_result.count.unwrap_or(0) != 1 {
            return Err(MetadataError::InvalidJobState(format!(
                "Job {} is not in pending state",
                job_id
            )));
        }

        sqlx::query_as!(
            ChangelogEntryWithID,
            r#"
            INSERT INTO changelog (changes)
            VALUES ($1)
            RETURNING *
            "#,
            serde_json::to_value(ChangelogEntry::V1(ChangelogEntryV1 {
                runs_added: new_runs.iter().map(|m| m.id.clone()).collect(),
                runs_removed: compacted,
            }))?
        )
        .fetch_one(&mut *transaction)
        .await?;

        if !new_runs.is_empty() {
            let ids: Vec<String> = new_runs.iter().map(|m| m.id.to_string()).collect();

            let belongs_to_values: Vec<serde_json::Value> = new_runs
                .iter()
                .map(|m| serde_json::to_value(&m.belongs_to)) // Use belongs_to directly from RunMetadata
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| sqlx::Error::Configuration(e.into()))?; // Propagate errors

            let stats_values: Vec<serde_json::Value> = new_runs
                .iter()
                .map(|m| serde_json::to_value(&m.stats)) // Use stats directly from RunMetadata
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| sqlx::Error::Configuration(e.into()))?; // Propagate errors

            sqlx::query!(
                r#"
                INSERT INTO runs (id, belongs_to, stats)
                SELECT * FROM UNNEST($1::text[], $2::jsonb[], $3::jsonb[])
                "#,
                &ids,
                &belongs_to_values,
                &stats_values
            )
            .execute(&mut *transaction)
            .await?;
        }

        transaction.commit().await?;

        Ok(())
    }

    async fn get_run_metadata_batch(
        &self,
        run_ids: Vec<crate::runs::RunId>,
    ) -> Result<HashMap<crate::runs::RunId, RunMetadata>, MetadataError> {
        let run_ids_strings: Vec<String> = run_ids.iter().map(|id| id.to_string()).collect();
        let rows = sqlx::query!(
            r#"
            SELECT id, belongs_to, stats
            FROM runs WHERE id = ANY($1)
            "#,
            &run_ids_strings
        )
        .fetch_all(&self.pg_pool)
        .await?;

        let run_metadatas = rows
            .into_iter()
            .map(|row| {
                let belongs_to: BelongsTo = serde_json::from_value(row.belongs_to)
                    .map_err(MetadataError::JsonSerdeError)?;
                let stats: crate::runs::Stats =
                    serde_json::from_value(row.stats).map_err(MetadataError::JsonSerdeError)?;

                Ok(RunMetadata {
                    id: crate::runs::RunId(row.id),
                    belongs_to,
                    stats,
                })
            })
            .collect::<Result<Vec<RunMetadata>, MetadataError>>()?;

        Ok(run_metadatas
            .into_iter()
            .map(|m| (m.id.clone(), m))
            .collect())
    }

    async fn schedule_wal_compaction(&self) -> Result<i64, MetadataError> {
        let mut transaction = self.pg_pool.begin().await?;

        // Check if there's already a wal_compaction job in pending or running state
        let existing_job = sqlx::query!(
            r#"
            SELECT id FROM jobs 
            WHERE typ = $1 AND status IN ('pending', 'running')
            LIMIT 1
            "#,
            "wal_compaction"
        )
        .fetch_optional(&mut *transaction)
        .await?;

        // Get job_id from existing job or create a new one
        let job_id = if let Some(job) = existing_job {
            job.id
        } else {
            let result = sqlx::query!(
                r#"
                INSERT INTO jobs (typ, job)
                VALUES ($1, $2)
                RETURNING id
                "#,
                "wal_compaction",
                serde_json::to_value(JobParams::WALCompaction)?
            )
            .fetch_one(&mut *transaction)
            .await?;

            result.id
        };

        transaction.commit().await?;

        Ok(job_id)
    }

    async fn schedule_table_buffer_compaction(
        &self,
        table_name: String,
    ) -> Result<i64, MetadataError> {
        let mut transaction = self.pg_pool.begin().await?;

        // Check if there's already a table_buffer_compaction job in pending or running state
        let existing_job = sqlx::query!(
            r#"
            SELECT id FROM jobs 
            WHERE typ = $1 AND status IN ('pending', 'running')
            LIMIT 1
            "#,
            "table_buffer_compaction"
        )
        .fetch_optional(&mut *transaction)
        .await?;

        // Get job_id from existing job or create a new one
        let job_id = if let Some(job) = existing_job {
            job.id
        } else {
            let result = sqlx::query!(
                r#"
                INSERT INTO jobs (typ, job)
                VALUES ($1, $2)
                RETURNING id
                "#,
                "table_buffer_compaction",
                serde_json::to_value(JobParams::TableBufferCompaction(table_name))?
            )
            .fetch_one(&mut *transaction)
            .await?;

            result.id
        };

        transaction.commit().await?;

        Ok(job_id)
    }

    async fn get_job_status(&self, job_id: i64) -> Result<String, MetadataError> {
        let row = sqlx::query!(
            r#"
            SELECT status FROM jobs WHERE id = $1
            "#,
            job_id
        )
        .fetch_one(&self.pg_pool)
        .await?;

        Ok(row.status)
    }

    async fn get_pending_jobs(&self) -> Result<Vec<(i64, JobParams)>, MetadataError> {
        let jobs = sqlx::query!(
            r#"
            SELECT id, job FROM jobs WHERE status = 'pending'
            "#
        )
        .fetch_all(&self.pg_pool)
        .await?
        .into_iter()
        .map(|row| {
            let job_params: JobParams =
                serde_json::from_value(row.job).map_err(MetadataError::JsonSerdeError)?;
            Ok((row.id, job_params))
        })
        .collect::<Result<Vec<(i64, JobParams)>, MetadataError>>()?;

        Ok(jobs)
    }

    async fn get_job(&self, job_id: i64) -> Result<JobParams, MetadataError> {
        let row = sqlx::query!(r#"SELECT id, job FROM jobs WHERE id = $1"#, job_id)
            .fetch_one(&self.pg_pool)
            .await?;

        let params: JobParams =
            serde_json::from_value(row.job).map_err(MetadataError::JsonSerdeError)?;

        Ok(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn test_schedule_and_get_wal_compaction_job() {
        // Setup test database
        let (metadata_store, _container) = setup_test_db().await.unwrap();

        // Schedule a WAL compaction job
        metadata_store.schedule_wal_compaction().await.unwrap();

        // Get pending jobs
        let pending_jobs = metadata_store.get_pending_jobs().await.unwrap();

        // Verify that we have exactly one job
        assert_eq!(pending_jobs.len(), 1);

        // Verify the job ID is positive
        let (job_id, job_params) = &pending_jobs[0];
        assert!(*job_id > 0);

        // Verify the job parameters match WALCompaction
        match job_params {
            JobParams::WALCompaction => {},
            JobParams::TableBufferCompaction(_) => {
                panic!("Expected WALCompaction job, got TableBufferCompaction");
            },
        }

        // Schedule another job - should not create a duplicate
        metadata_store.schedule_wal_compaction().await.unwrap();

        // Verify we still have only one job
        let pending_jobs_after_second_schedule = metadata_store.get_pending_jobs().await.unwrap();
        assert_eq!(pending_jobs_after_second_schedule.len(), 1);
    }
}
