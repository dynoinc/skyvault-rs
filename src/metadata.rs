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
    TableName(String),
}

impl From<BelongsTo> for proto::run_metadata::BelongsTo {
    fn from(belongs_to: BelongsTo) -> Self {
        match belongs_to {
            BelongsTo::WalSeqNo(seq_no) => proto::run_metadata::BelongsTo::WalSeqno(seq_no),
            BelongsTo::TableName(table_name) => {
                proto::run_metadata::BelongsTo::TableName(table_name)
            },
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct RunMetadata {
    pub id: String,
    pub belongs_to: BelongsTo,
    pub stats: crate::runs::Stats,
}

impl From<RunMetadata> for proto::RunMetadata {
    fn from(metadata: RunMetadata) -> Self {
        proto::RunMetadata {
            id: metadata.id,
            belongs_to: Some(metadata.belongs_to.into()),
            stats: Some(metadata.stats.into()),
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ChangelogEntryV1 {
    pub runs_added: Vec<String>,
    pub runs_removed: Vec<String>,
}

impl From<ChangelogEntryV1> for proto::ChangelogEntryV1 {
    fn from(entry: ChangelogEntryV1) -> Self {
        proto::ChangelogEntryV1 {
            runs_added: entry.runs_added,
            runs_removed: entry.runs_removed,
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
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Job {
    params: JobParams,
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
        run_id: String,
        stats: crate::runs::Stats,
    ) -> Result<(), MetadataError>;

    async fn append_compaction(
        &self,
        job_id: i64,
        compacted: Vec<String>,
        new_run_id: String,
        belongs_to: BelongsTo,
        new_stats: crate::runs::Stats,
    ) -> Result<(), MetadataError>;

    async fn get_run_metadata_batch(
        &self,
        run_ids: Vec<String>,
    ) -> Result<HashMap<String, RunMetadata>, MetadataError>;

    async fn schedule_wal_compaction(&self) -> Result<(), MetadataError>;

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

static MIGRATOR: Migrator = sqlx::migrate!("./src/migrations");

impl PostgresMetadataStore {
    pub async fn new(metadata_url: String) -> Result<Self, MetadataError> {
        let pg_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&metadata_url)
            .await?;

        MIGRATOR.run(&pg_pool).await?;

        Ok(Self { pg_pool })
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
        run_id: String,
        stats: crate::runs::Stats,
    ) -> Result<(), MetadataError> {
        loop {
            // Start a transaction to add changelog record and run metadata
            let mut transaction = self.pg_pool.begin().await?;

            // Insert changelog entry and get its ID
            let changelog_entry = sqlx::query_as!(
                ChangelogEntryWithID,
                r#"
                INSERT INTO changelog (changes)
                VALUES ($1)
                RETURNING *
                "#,
                serde_json::to_value(ChangelogEntry::V1(ChangelogEntryV1 {
                    runs_added: vec![run_id.clone()],
                    runs_removed: vec![],
                }))?
            )
            .fetch_one(&mut *transaction)
            .await?;

            // Create run metadata with the changelog ID as the WAL sequence number
            sqlx::query!(
                r#"
                INSERT INTO runs (id, belongs_to, stats)
                VALUES ($1, $2, $3)
                "#,
                run_id,
                serde_json::to_value(BelongsTo::WalSeqNo(changelog_entry.id))?,
                serde_json::to_value(stats.clone())?
            )
            .execute(&mut *transaction)
            .await?;

            // Commit the transaction, retry on conflict
            match transaction.commit().await {
                Ok(_) => break,
                Err(e) => {
                    if let sqlx::Error::Database(db_err) = &e {
                        if db_err.code().unwrap_or_default() == "40001" {
                            continue;
                        }
                    }
                    return Err(e.into());
                },
            }
        }

        Ok(())
    }

    async fn append_compaction(
        &self,
        job_id: i64,
        compacted: Vec<String>,
        new_run_id: String,
        belongs_to: BelongsTo,
        new_stats: crate::runs::Stats,
    ) -> Result<(), MetadataError> {
        let mut transaction = self.pg_pool.begin().await?;

        // Mark all compacted runs as deleted in a single query and get the count of updated rows
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
            &compacted as &[String]
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
                runs_added: vec![new_run_id.clone()],
                runs_removed: compacted,
            }))?
        )
        .fetch_one(&mut *transaction)
        .await?;

        sqlx::query!(
            r#"
            INSERT INTO runs (id, belongs_to, stats)
            VALUES ($1, $2, $3)
            "#,
            new_run_id,
            serde_json::to_value(belongs_to)?,
            serde_json::to_value(new_stats.clone())?
        )
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;

        Ok(())
    }

    async fn get_run_metadata_batch(
        &self,
        run_ids: Vec<String>,
    ) -> Result<HashMap<String, RunMetadata>, MetadataError> {
        let rows = sqlx::query!(
            r#"
            SELECT id, belongs_to, stats
            FROM runs WHERE id = ANY($1)
            "#,
            &run_ids as &[String]
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
                    id: row.id,
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

    async fn schedule_wal_compaction(&self) -> Result<(), MetadataError> {
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

        // Only insert if no existing job was found
        if existing_job.is_none() {
            sqlx::query!(
                r#"
                INSERT INTO jobs (typ, job)
                VALUES ($1, $2)
                "#,
                "wal_compaction",
                serde_json::to_value(JobParams::WALCompaction)?
            )
            .execute(&mut *transaction)
            .await?;
        }

        transaction.commit().await?;

        Ok(())
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
        }

        // Schedule another job - should not create a duplicate
        metadata_store.schedule_wal_compaction().await.unwrap();

        // Verify we still have only one job
        let pending_jobs_after_second_schedule = metadata_store.get_pending_jobs().await.unwrap();
        assert_eq!(pending_jobs_after_second_schedule.len(), 1);
    }
}
