use std::{
    collections::HashMap,
    fmt::{
        self,
        Display,
    },
    ops::Deref,
    pin::Pin,
    str::FromStr,
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};

use async_stream::stream;
use futures::Stream;
use metrics::{
    counter,
    histogram,
};
use sqlx::{
    PgPool,
    migrate::Migrator,
    postgres::PgPoolOptions,
    types::JsonValue,
};
use thiserror::Error;
use tracing::debug;

use crate::{
    proto,
    runs::{
        RunId,
        Stats,
    },
};

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("Database migration error: {0}")]
    DatabaseMigrationError(#[from] sqlx::migrate::MigrateError),

    #[error("JSON serde error: {0}")]
    JsonSerdeError(#[from] serde_json::Error),

    #[error("Some runs were already marked as deleted")]
    AlreadyDeleted(String),

    #[error("Job is not in pending state")]
    InvalidJobState(String),

    #[error("Job not found: {0}")]
    JobNotFound(JobID),

    #[error("Table already exists")]
    TableAlreadyExists(TableName),

    #[error("Table not found: {0}")]
    TableNotFound(TableName),

    #[error("Table ID not found: {0}")]
    TableIDNotFound(TableID),
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Eq, PartialEq, Clone, Copy, sqlx::Type, Hash)]
#[sqlx(transparent)]
pub struct JobID(i64);

impl From<i64> for JobID {
    fn from(value: i64) -> Self {
        JobID(value)
    }
}

impl From<JobID> for i64 {
    fn from(value: JobID) -> Self {
        value.0
    }
}

impl FromStr for JobID {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(JobID(s.parse()?))
    }
}

impl Display for JobID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Clone, Copy, Hash, sqlx::Type,
)]
#[sqlx(transparent)]
pub struct SeqNo(i64);

impl SeqNo {
    pub fn zero() -> Self {
        SeqNo(0)
    }
}

impl From<i64> for SeqNo {
    fn from(value: i64) -> Self {
        SeqNo(value)
    }
}

impl From<SeqNo> for i64 {
    fn from(value: SeqNo) -> Self {
        value.0
    }
}

impl Display for SeqNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Level(u64);

impl Level {
    pub fn max() -> Self {
        // 0 - 100MB
        // 1 - 1GB
        // 2 - 10GB
        // 3 - 100GB
        // 4 - 1TB
        // 5 - 10TB
        // 6 - infinite (100TB or more)
        Level(6)
    }

    pub fn zero() -> Self {
        Level(0)
    }

    pub fn next(&self) -> Self {
        Level(self.0 + 1)
    }
}

impl From<u64> for Level {
    fn from(value: u64) -> Self {
        Level(value)
    }
}

impl From<Level> for u64 {
    fn from(value: Level) -> Self {
        value.0
    }
}

impl Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
pub struct SnapshotID(String);

impl From<String> for SnapshotID {
    fn from(value: String) -> Self {
        SnapshotID(value)
    }
}

impl From<SnapshotID> for String {
    fn from(value: SnapshotID) -> Self {
        value.0
    }
}

impl Display for SnapshotID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
pub struct TableName(String);

impl<T: Into<String>> From<T> for TableName {
    fn from(value: T) -> Self {
        TableName(value.into())
    }
}

impl Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Deref for TableName {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub enum BelongsTo {
    WalSeqNo(SeqNo),
    TableBuffer(TableID, SeqNo),
    TableTreeLevel(TableID, Level),
}

impl From<BelongsTo> for proto::run_metadata::BelongsTo {
    fn from(belongs_to: BelongsTo) -> Self {
        match belongs_to {
            BelongsTo::WalSeqNo(seq_no) => proto::run_metadata::BelongsTo::WalSeqNo(seq_no.0),
            BelongsTo::TableBuffer(table_id, seq_no) => {
                proto::run_metadata::BelongsTo::TableBuffer(proto::TableBuffer {
                    table_id: table_id.0,
                    seq_no: seq_no.0,
                })
            },
            BelongsTo::TableTreeLevel(table_id, level) => {
                proto::run_metadata::BelongsTo::TableTree(proto::TableTreeLevel {
                    table_id: table_id.0,
                    level: level.0,
                })
            },
        }
    }
}

impl From<proto::run_metadata::BelongsTo> for BelongsTo {
    fn from(belongs_to: proto::run_metadata::BelongsTo) -> Self {
        match belongs_to {
            proto::run_metadata::BelongsTo::WalSeqNo(seq_no) => BelongsTo::WalSeqNo(SeqNo(seq_no)),
            proto::run_metadata::BelongsTo::TableBuffer(table_buffer) => {
                BelongsTo::TableBuffer(TableID(table_buffer.table_id), SeqNo(table_buffer.seq_no))
            },
            proto::run_metadata::BelongsTo::TableTree(table_tree) => {
                BelongsTo::TableTreeLevel(TableID(table_tree.table_id), Level(table_tree.level))
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct RunMetadata {
    pub id: RunId,
    pub belongs_to: BelongsTo,
    pub stats: Stats,
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

impl From<proto::RunMetadata> for RunMetadata {
    fn from(metadata: proto::RunMetadata) -> Self {
        RunMetadata {
            id: metadata.id.into(),
            belongs_to: match metadata.belongs_to {
                Some(belongs_to) => belongs_to.into(),
                None => panic!("belongs_to is None"),
            },
            stats: match metadata.stats {
                Some(stats) => stats.into(),
                None => panic!("stats is None"),
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RunsChangelogEntryV1 {
    pub runs_added: Vec<RunId>,
    pub runs_removed: Vec<RunId>,
}

impl From<RunsChangelogEntryV1> for proto::RunsChangelogEntryV1 {
    fn from(entry: RunsChangelogEntryV1) -> Self {
        proto::RunsChangelogEntryV1 {
            runs_added: entry.runs_added.iter().map(|id| id.to_string()).collect(),
            runs_removed: entry.runs_removed.iter().map(|id| id.to_string()).collect(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum TableChangelogEntryV1 {
    TableCreated(TableID),
    TableDropped(TableID),
}

impl From<TableChangelogEntryV1> for proto::TableChangelogEntryV1 {
    fn from(entry: TableChangelogEntryV1) -> Self {
        proto::TableChangelogEntryV1 {
            entry: match entry {
                TableChangelogEntryV1::TableCreated(table_id) => Some(
                    proto::table_changelog_entry_v1::Entry::TableCreated(proto::TableCreated { table_id: table_id.0 }),
                ),
                TableChangelogEntryV1::TableDropped(table_id) => Some(
                    proto::table_changelog_entry_v1::Entry::TableDropped(proto::TableDropped { table_id: table_id.0 }),
                ),
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum ChangelogEntry {
    RunsV1(RunsChangelogEntryV1),
    TablesV1(TableChangelogEntryV1),
}

impl From<JsonValue> for ChangelogEntry {
    fn from(val: JsonValue) -> Self {
        // panics on malformed JSON; switch to `from_value(val).unwrap()` or handle the
        // Result
        serde_json::from_value(val).expect("invalid ChangelogEntry JSON")
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ChangelogEntryWithID {
    pub id: SeqNo,
    pub changes: ChangelogEntry,
}

impl From<ChangelogEntryWithID> for proto::ChangelogEntryWithId {
    fn from(entry_with_id: ChangelogEntryWithID) -> Self {
        proto::ChangelogEntryWithId {
            id: entry_with_id.id.0,
            entry: match entry_with_id.changes {
                ChangelogEntry::RunsV1(v1) => {
                    Some(proto::changelog_entry_with_id::Entry::RunsChangelogEntryV1(v1.into()))
                },
                ChangelogEntry::TablesV1(v1) => {
                    Some(proto::changelog_entry_with_id::Entry::TableChangelogEntryV1(v1.into()))
                },
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum JobParams {
    WALCompaction,
    TableBufferCompaction(TableID),
    TableTreeCompaction(TableID, Level),
}

impl From<JobParams> for proto::JobParams {
    fn from(params: JobParams) -> Self {
        match params {
            JobParams::WALCompaction => proto::JobParams {
                params: Some(proto::job_params::Params::WalCompaction(())),
            },
            JobParams::TableBufferCompaction(table_id) => proto::JobParams {
                params: Some(proto::job_params::Params::TableBufferCompaction(table_id.0)),
            },
            JobParams::TableTreeCompaction(table_id, level) => proto::JobParams {
                params: Some(proto::job_params::Params::TableTreeCompaction(proto::TableTreeCompaction {
                    table_id: table_id.0,
                    level: level.0,
                })),
            },
        }
    }
}

impl From<proto::JobParams> for JobParams {
    fn from(params: proto::JobParams) -> Self {
        match params.params {
            Some(proto::job_params::Params::WalCompaction(_)) => JobParams::WALCompaction,
            Some(proto::job_params::Params::TableBufferCompaction(table_id)) => JobParams::TableBufferCompaction(TableID(table_id)),
            Some(proto::job_params::Params::TableTreeCompaction(table_tree)) => JobParams::TableTreeCompaction(TableID(table_tree.table_id), Level(table_tree.level)),
            None => panic!("params is None"),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Completed(SeqNo),
    Failed,
}

impl From<JobStatus> for proto::JobStatus {
    fn from(status: JobStatus) -> Self {
        match status {
            JobStatus::Pending => proto::JobStatus {
                status: Some(proto::job_status::Status::Pending(true)),
            },
            JobStatus::Completed(seq_no) => proto::JobStatus {
                status: Some(proto::job_status::Status::SeqNo(seq_no.0)),
            },
            JobStatus::Failed => proto::JobStatus {
                status: Some(proto::job_status::Status::Failed(true)),
            },
        }
    }
}

pub struct Job {
    pub id: JobID,
    pub params: JobParams,
    pub status: JobStatus,
}

impl From<Job> for proto::Job {
    fn from(job: Job) -> Self {
        proto::Job {
            id: job.id.0,
            params: Some(job.params.into()),
            status: Some(job.status.into()),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Hash, Copy)]
pub struct TableID(i64);

impl From<i64> for TableID {
    fn from(value: i64) -> Self {
        TableID(value)
    }
}

impl From<TableID> for i64 {
    fn from(value: TableID) -> Self {
        value.0
    }
}

impl Display for TableID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct TableConfig {
    #[serde(skip)]
    pub table_id: Option<TableID>,
    pub table_name: TableName,
}

impl From<proto::TableConfig> for TableConfig {
    fn from(config: proto::TableConfig) -> Self {
        TableConfig {
            table_id: if config.table_id != 0 {
                Some(TableID(config.table_id))
            } else {
                None
            },
            table_name: TableName::from(config.table_name),
        }
    }
}

impl From<&TableConfig> for proto::TableConfig {
    fn from(config: &TableConfig) -> Self {
        proto::TableConfig {
            table_id: config.table_id.map(|id| id.0).unwrap_or(0),
            table_name: config.table_name.to_string(),
        }
    }
}

type ChangelogStream = Pin<Box<dyn Stream<Item = Result<ChangelogEntryWithID, MetadataError>> + Send + 'static>>;

#[async_trait::async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait MetadataStoreTrait: Send + Sync + 'static {
    // SNAPSHOTS
    async fn get_latest_snapshot_id(&self) -> Result<Option<(SnapshotID, SeqNo)>, MetadataError>;
    async fn get_latest_snapshot(&self) -> Result<(Option<SnapshotID>, Vec<ChangelogEntryWithID>), MetadataError>;
    async fn persist_snapshot(&self, snapshot_id: SnapshotID, seq_no: SeqNo) -> Result<(), MetadataError>;

    // CHANGELOG
    async fn stream_changelog(&self) -> Result<(Option<SnapshotID>, ChangelogStream), MetadataError>;
    async fn get_changelog(&self, from_seq_no: SeqNo) -> Result<Vec<ChangelogEntryWithID>, MetadataError>;

    // WAL & COMPACTIONS
    async fn append_wal(&self, run_ids: Vec<(RunId, Stats)>) -> Result<SeqNo, MetadataError>;
    async fn append_wal_compaction(
        &self,
        job_id: JobID,
        compacted: Vec<RunId>,
        new_table_runs: Vec<(RunId, TableID, Stats)>,
    ) -> Result<SeqNo, MetadataError>;
    async fn append_table_compaction(
        &self,
        job_id: JobID,
        compacted: Vec<RunId>,
        new_runs: Vec<RunMetadata>,
    ) -> Result<SeqNo, MetadataError>;
    async fn get_run_metadata_batch(&self, run_ids: Vec<RunId>) -> Result<HashMap<RunId, RunMetadata>, MetadataError>;

    // JOBS
    async fn schedule_job(&self, job_params: JobParams) -> Result<JobID, MetadataError>;
    async fn get_pending_jobs(&self) -> Result<Vec<(JobID, JobParams)>, MetadataError>;
    async fn get_job(&self, job_id: JobID) -> Result<Job, MetadataError>;
    async fn list_jobs(&self, limit: i64) -> Result<Vec<Job>, MetadataError>;
    async fn mark_job_failed(&self, job_id: JobID) -> Result<(), MetadataError>;

    // TABLES
    async fn create_table(&self, config: TableConfig) -> Result<SeqNo, MetadataError>;
    async fn drop_table(&self, table_name: TableName) -> Result<SeqNo, MetadataError>;
    async fn list_tables(&self) -> Result<Vec<TableConfig>, MetadataError>;
    async fn get_table(&self, table_name: TableName) -> Result<TableConfig, MetadataError>;
    async fn get_table_by_id(&self, table_id: TableID) -> Result<TableConfig, MetadataError>;
}

pub type MetadataStore = Arc<dyn MetadataStoreTrait>;

#[derive(Clone)]
pub struct PostgresMetadataStore {
    pg_pool: PgPool,
}

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

impl PostgresMetadataStore {
    pub async fn from_url(metadata_url: String) -> Result<MetadataStore, MetadataError> {
        let pg_pool = PgPoolOptions::new().max_connections(5).connect(&metadata_url).await?;

        MIGRATOR.run(&pg_pool).await?;

        Ok(Arc::new(MetricsMetadataStore::new(Self { pg_pool })))
    }

    /// Attempts to perform the append_wal operation within a single
    /// transaction.
    async fn append_wal_attempt(&self, run_ids_with_stats: &[(RunId, Stats)]) -> Result<SeqNo, MetadataError> {
        let mut transaction = self.pg_pool.begin().await?;

        let first_seq_no: i64 = sqlx::query_scalar("SELECT nextval('changelog_id_seq')")
            .fetch_one(&mut *transaction) // Deref the mutable reference
            .await?;

        let n = run_ids_with_stats.len();
        if n > 1 {
            let last_seq_no = first_seq_no + (n as i64) - 1;
            sqlx::query("SELECT setval('changelog_id_seq', $1, true)")
                .bind(last_seq_no)
                .execute(&mut *transaction) // Deref the mutable reference
                .await?;
        }

        let changelog_changes = serde_json::to_value(ChangelogEntry::RunsV1(RunsChangelogEntryV1 {
            runs_added: run_ids_with_stats.iter().map(|(id, _)| id.clone()).collect(),
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
        .execute(&mut *transaction) // Deref the mutable reference
        .await?;

        let run_ids: Vec<String> = run_ids_with_stats.iter().map(|(id, _)| id.to_string()).collect();

        let stats_values: Vec<serde_json::Value> = run_ids_with_stats
            .iter()
            .map(|(_, stats)| serde_json::to_value(stats))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| sqlx::Error::Configuration(e.into()))?; // Convert serde error

        let belongs_to_values: Vec<serde_json::Value> = (0..n as i64)
            .map(|i| serde_json::to_value(BelongsTo::WalSeqNo(SeqNo(first_seq_no + i))))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| sqlx::Error::Configuration(e.into()))?; // Convert serde error

        sqlx::query!(
            r#"
            INSERT INTO runs (id, belongs_to, stats)
            SELECT * FROM UNNEST($1::text[], $2::jsonb[], $3::jsonb[])
            "#,
            &run_ids,
            &belongs_to_values,
            &stats_values
        )
        .execute(&mut *transaction) // Deref the mutable reference
        .await?;

        transaction.commit().await?;
        Ok(SeqNo::from(first_seq_no + n as i64 - 1))
    }

    /// Attempts to mark a job as completed within a transaction.
    /// Returns Ok(()) if the job was found in pending state and updated,
    /// otherwise returns an error.
    async fn mark_job_completed(
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        job_id: JobID,
        output: JsonValue,
    ) -> Result<(), sqlx::Error> {
        let job_update_result = sqlx::query!(
            r#"
            WITH updated_jobs AS (
                UPDATE jobs
                SET status = 'completed', output = $2
                WHERE id = $1 AND status = 'pending'
                RETURNING id
            )
            SELECT COUNT(*) FROM updated_jobs
            "#,
            job_id.0,
            output
        )
        .fetch_one(&mut **transaction) // Deref
        .await?;

        if job_update_result.count.unwrap_or(0) != 1 {
            Err(sqlx::Error::RowNotFound)
        } else {
            Ok(())
        }
    }

    /// Attempts to perform the append_wal_compaction operation within a single
    /// transaction.
    async fn append_wal_compaction_attempt(
        &self,
        job_id: JobID,
        compacted: &[RunId],
        new_table_runs: &[(RunId, TableID, Stats)],
    ) -> Result<SeqNo, MetadataError> {
        let mut transaction = self.pg_pool.begin().await?;

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
        .fetch_one(&mut *transaction) // Deref
        .await?;

        let deleted_count = deleted_count_result.count.unwrap_or(0) as usize;
        if deleted_count != compacted.len() {
            return Err(MetadataError::AlreadyDeleted(
                "Mismatch in number of deleted runs.".into(),
            ));
        }

        // --- Reserve Sequence Block for Changelog and potential new runs ---
        let n_new_runs = new_table_runs.len();
        // Reserve N IDs if creating runs, otherwise reserve 1 ID for the changelog
        // entry itself.
        let num_ids_to_reserve = std::cmp::max(1, n_new_runs);

        let first_seq_no: i64 = sqlx::query_scalar("SELECT nextval('changelog_id_seq')")
            .fetch_one(&mut *transaction)
            .await?;

        if num_ids_to_reserve > 1 {
            let last_seq_no = first_seq_no + (num_ids_to_reserve as i64) - 1;
            sqlx::query("SELECT setval('changelog_id_seq', $1, true)")
                .bind(last_seq_no)
                .execute(&mut *transaction) // Deref
                .await?;
        }
        // If num_ids_to_reserve == 1, nextval already advanced it correctly.

        // --- Insert Changelog Entry ---
        let changelog_changes = serde_json::to_value(ChangelogEntry::RunsV1(RunsChangelogEntryV1 {
            runs_added: new_table_runs.iter().map(|(id, ..)| id.clone()).collect(),
            runs_removed: compacted.to_vec(), // Clone if needed or take ownership
        }))
        .map_err(MetadataError::JsonSerdeError)?; // Map serde error

        sqlx::query!(
            r#"
            INSERT INTO changelog (id, changes)
            VALUES ($1, $2)
            "#,
            first_seq_no, // Use the first reserved ID for the changelog entry
            changelog_changes
        )
        .execute(&mut *transaction) // Deref
        .await?;

        PostgresMetadataStore::mark_job_completed(
            &mut transaction,
            job_id,
            serde_json::to_value(first_seq_no).map_err(MetadataError::JsonSerdeError)?,
        )
        .await
        .map_err(|e| {
            if matches!(e, sqlx::Error::RowNotFound) {
                // Directly return the specific MetadataError
                MetadataError::InvalidJobState(format!("Job {job_id} update failed or not in pending state."))
            } else {
                MetadataError::DatabaseError(e) // Propagate other DB errors
            }
        })?;

        // --- Insert New Runs (if any) ---
        if n_new_runs > 0 {
            let ids: Vec<String> = new_table_runs.iter().map(|(id, ..)| id.to_string()).collect();

            // Create unique BelongsTo::TableBuffer values using the reserved sequence block
            let belongs_to_values: Vec<serde_json::Value> = new_table_runs
                .iter()
                .enumerate()
                .map(|(i, (_, table_id, _))| {
                    // Assign unique sequence number: first_seq_no + i
                    serde_json::to_value(BelongsTo::TableBuffer(
                        *table_id,
                        SeqNo::from(first_seq_no + i as i64), // Use unique ID here
                    ))
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(MetadataError::JsonSerdeError)?; // Map serde error

            let stats_values: Vec<serde_json::Value> = new_table_runs
                .iter()
                .map(|(_, _, stats)| serde_json::to_value(stats))
                .collect::<Result<Vec<_>, _>>()
                .map_err(MetadataError::JsonSerdeError)?; // Map serde error

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
            .execute(&mut *transaction) // Deref
            .await?;
        }

        transaction.commit().await?;
        Ok(SeqNo::from(first_seq_no + n_new_runs as i64 - 1))
    }

    async fn append_table_compaction_attempt(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        job_id: JobID,
        compacted: &[RunId],
        new_runs: &[RunMetadata],
    ) -> Result<SeqNo, MetadataError> {
        let compacted_strings: Vec<String> = compacted.iter().map(ToString::to_string).collect();
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
        .fetch_one(&mut **transaction)
        .await?;

        if result.count.unwrap_or(0) as usize != compacted.len() {
            return Err(MetadataError::AlreadyDeleted(
                "Some runs were already marked as deleted".to_string(),
            ));
        }

        let changelog_entry_payload = ChangelogEntry::RunsV1(RunsChangelogEntryV1 {
            runs_added: new_runs.iter().map(|m| m.id.clone()).collect(),
            runs_removed: compacted.to_vec(),
        });
        let changelog_changes_json = serde_json::to_value(changelog_entry_payload)?;

        let changelog_record = sqlx::query!(
            r#"
            INSERT INTO changelog (changes)
            VALUES ($1)
            RETURNING id
            "#,
            changelog_changes_json
        )
        .fetch_one(&mut **transaction)
        .await?;

        let changelog_seq_no = SeqNo::from(changelog_record.id);

        PostgresMetadataStore::mark_job_completed(transaction, job_id, serde_json::to_value(changelog_seq_no)?)
            .await
            .map_err(|e| {
                if matches!(e, sqlx::Error::RowNotFound) {
                    MetadataError::InvalidJobState(format!("Job {job_id} is not in pending state"))
                } else {
                    MetadataError::DatabaseError(e)
                }
            })?;

        if !new_runs.is_empty() {
            let ids: Vec<String> = new_runs.iter().map(|m| m.id.to_string()).collect();

            let belongs_to_values: Vec<serde_json::Value> = new_runs
                .iter()
                .map(|m| serde_json::to_value(&m.belongs_to))
                .collect::<Result<Vec<_>, _>>()?;

            let stats_values: Vec<serde_json::Value> = new_runs
                .iter()
                .map(|m| serde_json::to_value(&m.stats))
                .collect::<Result<Vec<_>, _>>()?;

            sqlx::query!(
                r#"
                INSERT INTO runs (id, belongs_to, stats)
                SELECT * FROM UNNEST($1::text[], $2::jsonb[], $3::jsonb[])
                "#,
                &ids,
                &belongs_to_values,
                &stats_values
            )
            .execute(&mut **transaction)
            .await?;
        }

        Ok(changelog_seq_no)
    }
}

pub struct MetricsMetadataStore {
    inner: PostgresMetadataStore,
}

impl MetricsMetadataStore {
    pub fn new(inner: PostgresMetadataStore) -> Self {
        Self { inner }
    }

    async fn record_metrics<T, F, Fut>(method: &'static str, operation: F) -> Result<T, MetadataError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, MetadataError>>,
    {
        let start = Instant::now();
        let result = operation().await;
        let duration = start.elapsed();

        let status = match result.as_ref() {
            Ok(_) => "ok".to_string(),
            Err(err) => match err {
                MetadataError::DatabaseError(_) => "database_error".to_string(),
                MetadataError::DatabaseMigrationError(_) => "migration_error".to_string(),
                MetadataError::JsonSerdeError(_) => "json_error".to_string(),
                MetadataError::AlreadyDeleted(_) => "already_deleted".to_string(),
                MetadataError::InvalidJobState(_) => "invalid_job_state".to_string(),
                MetadataError::JobNotFound(_) => "job_not_found".to_string(),
                MetadataError::TableAlreadyExists(_) => "table_exists".to_string(),
                MetadataError::TableNotFound(_) => "table_not_found".to_string(),
                MetadataError::TableIDNotFound(_) => "table_id_not_found".to_string(),
            },
        };

        let req_counter = counter!(
            "skyvault/postgres/requests_total",
            "method" => method,
            "status" => status.clone()
        );
        let duration_hist = histogram!(
            "skyvault/postgres/request_duration_seconds",
            "method" => method
        );

        req_counter.increment(1);
        duration_hist.record(duration.as_secs_f64());

        debug!(
            method = method,
            status = %status,
            duration_ms = duration.as_millis(),
            "Postgres operation completed"
        );

        result
    }
}

#[async_trait::async_trait]
impl MetadataStoreTrait for MetricsMetadataStore {
    async fn get_latest_snapshot_id(&self) -> Result<Option<(SnapshotID, SeqNo)>, MetadataError> {
        Self::record_metrics("get_latest_snapshot_id", || self.inner.get_latest_snapshot_id()).await
    }

    async fn get_latest_snapshot(&self) -> Result<(Option<SnapshotID>, Vec<ChangelogEntryWithID>), MetadataError> {
        Self::record_metrics("get_latest_snapshot", || self.inner.get_latest_snapshot()).await
    }

    async fn persist_snapshot(&self, snapshot_id: SnapshotID, seq_no: SeqNo) -> Result<(), MetadataError> {
        Self::record_metrics("persist_snapshot", || self.inner.persist_snapshot(snapshot_id, seq_no)).await
    }

    async fn stream_changelog(&self) -> Result<(Option<SnapshotID>, ChangelogStream), MetadataError> {
        Self::record_metrics("stream_changelog", || self.inner.stream_changelog()).await
    }

    async fn get_changelog(&self, from_seq_no: SeqNo) -> Result<Vec<ChangelogEntryWithID>, MetadataError> {
        Self::record_metrics("get_changelog", || self.inner.get_changelog(from_seq_no)).await
    }

    async fn append_wal(&self, run_ids: Vec<(RunId, Stats)>) -> Result<SeqNo, MetadataError> {
        Self::record_metrics("append_wal", || self.inner.append_wal(run_ids)).await
    }

    async fn append_wal_compaction(
        &self,
        job_id: JobID,
        compacted: Vec<RunId>,
        new_table_runs: Vec<(RunId, TableID, Stats)>,
    ) -> Result<SeqNo, MetadataError> {
        Self::record_metrics("append_wal_compaction", || {
            self.inner.append_wal_compaction(job_id, compacted, new_table_runs)
        })
        .await
    }

    async fn append_table_compaction(
        &self,
        job_id: JobID,
        compacted: Vec<RunId>,
        new_runs: Vec<RunMetadata>,
    ) -> Result<SeqNo, MetadataError> {
        Self::record_metrics("append_table_compaction", || {
            self.inner.append_table_compaction(job_id, compacted, new_runs)
        })
        .await
    }

    async fn get_run_metadata_batch(&self, run_ids: Vec<RunId>) -> Result<HashMap<RunId, RunMetadata>, MetadataError> {
        Self::record_metrics("get_run_metadata_batch", || self.inner.get_run_metadata_batch(run_ids)).await
    }

    async fn schedule_job(&self, job_params: JobParams) -> Result<JobID, MetadataError> {
        Self::record_metrics("schedule_job", || self.inner.schedule_job(job_params)).await
    }

    async fn get_pending_jobs(&self) -> Result<Vec<(JobID, JobParams)>, MetadataError> {
        Self::record_metrics("get_pending_jobs", || self.inner.get_pending_jobs()).await
    }

    async fn get_job(&self, job_id: JobID) -> Result<Job, MetadataError> {
        Self::record_metrics("get_job", || self.inner.get_job(job_id)).await
    }

    async fn list_jobs(&self, limit: i64) -> Result<Vec<Job>, MetadataError> {
        Self::record_metrics("list_jobs", || self.inner.list_jobs(limit)).await
    }

    async fn mark_job_failed(&self, job_id: JobID) -> Result<(), MetadataError> {
        Self::record_metrics("mark_job_failed", || self.inner.mark_job_failed(job_id)).await
    }

    async fn create_table(&self, config: TableConfig) -> Result<SeqNo, MetadataError> {
        Self::record_metrics("create_table", || self.inner.create_table(config)).await
    }

    async fn drop_table(&self, table_name: TableName) -> Result<SeqNo, MetadataError> {
        Self::record_metrics("drop_table", || self.inner.drop_table(table_name)).await
    }

    async fn list_tables(&self) -> Result<Vec<TableConfig>, MetadataError> {
        Self::record_metrics("list_tables", || self.inner.list_tables()).await
    }

    async fn get_table(&self, table_name: TableName) -> Result<TableConfig, MetadataError> {
        Self::record_metrics("get_table", || self.inner.get_table(table_name)).await
    }

    async fn get_table_by_id(&self, table_id: TableID) -> Result<TableConfig, MetadataError> {
        Self::record_metrics("get_table_by_id", || self.inner.get_table_by_id(table_id)).await
    }
}

/// Helper function to check if a database error is retryable.
/// Currently only considers serialization failures (error code 40001) as
/// retryable.
fn is_retryable_error(error: &MetadataError) -> bool {
    matches!(error, MetadataError::DatabaseError(sqlx::Error::Database(db_err)) if db_err.code().is_some_and(|code| code == "40001"))
}

#[async_trait::async_trait]
impl MetadataStoreTrait for PostgresMetadataStore {
    async fn get_latest_snapshot_id(&self) -> Result<Option<(SnapshotID, SeqNo)>, MetadataError> {
        let latest_persisted_snapshot = sqlx::query!("SELECT id, seq_no FROM snapshots ORDER BY seq_no DESC LIMIT 1")
            .fetch_optional(&self.pg_pool)
            .await?;

        Ok(latest_persisted_snapshot.map(|e| (SnapshotID::from(e.id), SeqNo::from(e.seq_no))))
    }

    /// Fetches all existing changelog entries and returns them as a vector.
    async fn get_latest_snapshot(&self) -> Result<(Option<SnapshotID>, Vec<ChangelogEntryWithID>), MetadataError> {
        let latest_persisted_snapshot = sqlx::query!("SELECT id, seq_no FROM snapshots ORDER BY seq_no DESC LIMIT 1")
            .fetch_optional(&self.pg_pool)
            .await?;

        let snapshot = latest_persisted_snapshot.map(|e| (SnapshotID::from(e.id), SeqNo::from(e.seq_no)));
        let last_seq_no = snapshot.clone().map(|(_, seq_no)| seq_no).unwrap_or(SeqNo::zero());
        let changelog = self.get_changelog(last_seq_no).await?;
        Ok((snapshot.map(|(id, _)| id), changelog))
    }

    async fn persist_snapshot(&self, snapshot_id: SnapshotID, seq_no: SeqNo) -> Result<(), MetadataError> {
        sqlx::query!(
            "INSERT INTO snapshots (id, seq_no) VALUES ($1, $2)",
            snapshot_id.0,
            seq_no.0
        )
        .execute(&self.pg_pool)
        .await?;
        Ok(())
    }

    /// Returns a snapshot of existing changelog entries and a stream of new
    /// entries. The stream continuously polls for new entries after
    /// reaching the end of the snapshot.
    async fn stream_changelog(&self) -> Result<(Option<SnapshotID>, ChangelogStream), MetadataError> {
        let latest_persisted_snapshot = sqlx::query!("SELECT id, seq_no FROM snapshots ORDER BY seq_no DESC LIMIT 1")
            .fetch_optional(&self.pg_pool)
            .await?;

        let snapshot = latest_persisted_snapshot.map(|e| (SnapshotID::from(e.id), SeqNo::from(e.seq_no)));

        let pg_pool = self.pg_pool.clone();
        let mut last_id = snapshot.clone().map(|(_, seq_no)| seq_no).unwrap_or(SeqNo::zero());
        let stream = stream! {
            loop {
                let new_entries = sqlx::query_as!(
                    ChangelogEntryWithID,
                    "SELECT * FROM changelog WHERE id > $1 ORDER BY id ASC",
                    last_id.0
                )
                .fetch_all(&pg_pool)
                .await?;

                for entry in new_entries {
                    last_id = entry.id;
                    yield Ok(entry);
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };

        Ok((snapshot.map(|(id, _)| id), Box::pin(stream)))
    }

    async fn get_changelog(&self, from_seq_no: SeqNo) -> Result<Vec<ChangelogEntryWithID>, MetadataError> {
        let entries = sqlx::query_as!(
            ChangelogEntryWithID,
            "SELECT * FROM changelog WHERE id > $1 ORDER BY id ASC",
            from_seq_no.0
        )
        .fetch_all(&self.pg_pool)
        .await?;

        Ok(entries)
    }

    async fn append_wal(&self, run_ids_with_stats: Vec<(RunId, Stats)>) -> Result<SeqNo, MetadataError> {
        loop {
            match self.append_wal_attempt(&run_ids_with_stats).await {
                Ok(seq_no) => return Ok(seq_no), // Success! Exit function.
                Err(err) if is_retryable_error(&err) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                },
                Err(err) => return Err(err),
            }
        }
    }

    async fn append_wal_compaction(
        &self,
        job_id: JobID,
        compacted: Vec<RunId>,
        new_table_runs: Vec<(RunId, TableID, Stats)>,
    ) -> Result<SeqNo, MetadataError> {
        loop {
            return match self
                .append_wal_compaction_attempt(job_id, &compacted, &new_table_runs)
                .await
            {
                Ok(seq_no) => return Ok(seq_no),
                Err(err) if is_retryable_error(&err) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                },
                Err(e) => Err(e),
            };
        }
    }

    async fn append_table_compaction(
        &self,
        job_id: JobID,
        compacted: Vec<RunId>,
        new_runs: Vec<RunMetadata>,
    ) -> Result<SeqNo, MetadataError> {
        loop {
            let mut transaction = self.pg_pool.begin().await?;

            match self
                .append_table_compaction_attempt(&mut transaction, job_id, &compacted, &new_runs)
                .await
            {
                Ok(seq_no) => match transaction.commit().await {
                    Ok(_) => return Ok(seq_no),
                    Err(commit_err) => {
                        if let sqlx::Error::Database(db_err) = &commit_err {
                            if db_err.code().is_some_and(|code| code == "40001") {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                continue;
                            }
                        }
                        return Err(MetadataError::DatabaseError(commit_err));
                    },
                },
                Err(attempt_meta_err) => match attempt_meta_err {
                    MetadataError::DatabaseError(sqlx_err) => {
                        if let sqlx::Error::Database(db_err) = &sqlx_err {
                            if db_err.code().is_some_and(|code| code == "40001") {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                continue;
                            }
                        }
                        return Err(MetadataError::DatabaseError(sqlx_err));
                    },
                    other_err => return Err(other_err),
                },
            }
        }
    }

    async fn get_run_metadata_batch(&self, run_ids: Vec<RunId>) -> Result<HashMap<RunId, RunMetadata>, MetadataError> {
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
                let belongs_to: BelongsTo =
                    serde_json::from_value(row.belongs_to).map_err(MetadataError::JsonSerdeError)?;
                let stats: Stats = serde_json::from_value(row.stats).map_err(MetadataError::JsonSerdeError)?;

                Ok(RunMetadata {
                    id: RunId(row.id),
                    belongs_to,
                    stats,
                })
            })
            .collect::<Result<Vec<RunMetadata>, MetadataError>>()?;

        Ok(run_metadatas.into_iter().map(|m| (m.id.clone(), m)).collect())
    }

    async fn schedule_job(&self, job_params: JobParams) -> Result<JobID, MetadataError> {
        let mut transaction = self.pg_pool.begin().await?;

        // Check if there's already a job in pending or running state
        let existing_job = sqlx::query!(
            r#"
            SELECT id FROM jobs 
            WHERE status = 'pending' AND job = $1
            LIMIT 1
            "#,
            serde_json::to_value(job_params.clone())?
        )
        .fetch_optional(&mut *transaction)
        .await?;

        // Get job_id from existing job or create a new one
        let job_id = if let Some(job) = existing_job {
            job.id
        } else {
            let result = sqlx::query!(
                r#"
                INSERT INTO jobs (job)
                VALUES ($1)
                RETURNING id
                "#,
                serde_json::to_value(job_params)?
            )
            .fetch_one(&mut *transaction)
            .await?;

            result.id
        };

        transaction.commit().await?;

        Ok(JobID::from(job_id))
    }

    async fn get_pending_jobs(&self) -> Result<Vec<(JobID, JobParams)>, MetadataError> {
        let jobs = sqlx::query!(
            r#"
            SELECT id, job FROM jobs WHERE status = 'pending'
            "#
        )
        .fetch_all(&self.pg_pool)
        .await?
        .into_iter()
        .map(|row| {
            let job_params: JobParams = serde_json::from_value(row.job).map_err(MetadataError::JsonSerdeError)?;
            Ok((JobID::from(row.id), job_params))
        })
        .collect::<Result<Vec<(JobID, JobParams)>, MetadataError>>()?;

        Ok(jobs)
    }

    async fn get_job(&self, job_id: JobID) -> Result<Job, MetadataError> {
        let row = sqlx::query!(r#"SELECT id, job, status FROM jobs WHERE id = $1"#, job_id.0)
            .fetch_optional(&self.pg_pool)
            .await?;

        let job = match row {
            Some(row) => Job {
                id: JobID::from(row.id),
                params: serde_json::from_value(row.job).map_err(MetadataError::JsonSerdeError)?,
                status: serde_json::from_str(&row.status).map_err(MetadataError::JsonSerdeError)?,
            },
            None => return Err(MetadataError::JobNotFound(job_id)),
        };

        Ok(job)
    }

    async fn list_jobs(&self, limit: i64) -> Result<Vec<Job>, MetadataError> {
        let jobs = sqlx::query!(
            r#"
            SELECT id, job, status FROM jobs ORDER BY id DESC LIMIT $1
            "#,
            limit
        )
        .fetch_all(&self.pg_pool)
        .await?
        .into_iter()
        .map(|row| {
            let job_params: JobParams = serde_json::from_value(row.job).map_err(MetadataError::JsonSerdeError)?;
            let job_status: JobStatus = serde_json::from_str(&row.status).map_err(MetadataError::JsonSerdeError)?;
            Ok(Job { id: JobID::from(row.id), params: job_params, status: job_status })
        })
        .collect::<Result<Vec<Job>, MetadataError>>()?;

        Ok(jobs)
    }

    async fn mark_job_failed(&self, job_id: JobID) -> Result<(), MetadataError> {
        sqlx::query!(
            "UPDATE jobs SET status = 'failed' WHERE id = $1 AND status = 'pending'",
            job_id.0,
        )
        .execute(&self.pg_pool)
        .await?;

        Ok(())
    }

    async fn create_table(&self, config: TableConfig) -> Result<SeqNo, MetadataError> {
        let mut transaction = self.pg_pool.begin().await?;

        let table_name = config.table_name.clone();
        let result = sqlx::query_scalar!(
            r#"
            INSERT INTO tables (name, config)
            VALUES ($1, $2)
            RETURNING id
            "#,
            table_name.to_string(),
            serde_json::to_value(config)?
        )
        .fetch_one(&mut *transaction)
        .await;

        match result {
            Ok(id) => {
                let table_id = TableID::from(id);
                let changelog_changes =
                    serde_json::to_value(ChangelogEntry::TablesV1(TableChangelogEntryV1::TableCreated(table_id)))?;

                let seq_no = sqlx::query_scalar!(
                    r#"
                    INSERT INTO changelog (changes)
                    VALUES ($1)
                    RETURNING id
                    "#,
                    changelog_changes
                )
                .fetch_one(&mut *transaction)
                .await?;

                transaction.commit().await?;
                Ok(SeqNo::from(seq_no))
            },
            Err(sqlx::Error::Database(db_error)) if db_error.code().as_deref() == Some("23505") => {
                transaction.rollback().await?;
                Err(MetadataError::TableAlreadyExists(table_name))
            },
            Err(e) => {
                transaction.rollback().await?;
                Err(e.into())
            },
        }
    }

    async fn drop_table(&self, table_name: TableName) -> Result<SeqNo, MetadataError> {
        let mut transaction = self.pg_pool.begin().await?;

        let tbl_id = sqlx::query_scalar!(
            r#"
            SELECT id FROM tables WHERE name = $1 AND deleted_at IS NULL FOR UPDATE
            "#,
            table_name.to_string()
        )
        .fetch_optional(&mut *transaction)
        .await?;

        let table_id = match tbl_id {
            Some(id) => TableID::from(id),
            None => return Err(MetadataError::TableNotFound(table_name)),
        };

        let changelog_changes = serde_json::to_value(ChangelogEntry::TablesV1(
            TableChangelogEntryV1::TableDropped(table_id),
        ))?;

        let seq_no = sqlx::query_scalar!(
            r#"
            INSERT INTO changelog (changes)
            VALUES ($1)
            RETURNING id
            "#,
            changelog_changes
        )
        .fetch_one(&mut *transaction)
        .await?;

        sqlx::query!(
            r#"
            UPDATE tables SET deleted_at = NOW() WHERE name = $1 AND deleted_at IS NULL
            "#,
            table_name.to_string()
        )
        .execute(&mut *transaction)
        .await?;

        transaction.commit().await?;

        Ok(SeqNo::from(seq_no))
    }

    async fn get_table(&self, table_name: TableName) -> Result<TableConfig, MetadataError> {
        let row = sqlx::query!(
            r#"
            SELECT id, config FROM tables WHERE name = $1 AND deleted_at IS NULL
            "#,
            table_name.to_string()
        )
        .fetch_optional(&self.pg_pool)
        .await?;

        match row {
            Some(row) => {
                let mut config: TableConfig =
                    serde_json::from_value(row.config).map_err(MetadataError::JsonSerdeError)?;
                config.table_id = Some(TableID::from(row.id));
                Ok(config)
            },
            None => Err(MetadataError::TableNotFound(table_name)),
        }
    }

    async fn get_table_by_id(&self, table_id: TableID) -> Result<TableConfig, MetadataError> {
        let row = sqlx::query!(
            r#"
            SELECT id, config FROM tables WHERE id = $1
            "#,
            table_id.0
        )
        .fetch_optional(&self.pg_pool)
        .await?;

        match row {
            Some(row) => {
                let mut config: TableConfig =
                    serde_json::from_value(row.config).map_err(MetadataError::JsonSerdeError)?;
                config.table_id = Some(TableID::from(row.id));
                Ok(config)
            },
            None => Err(MetadataError::TableIDNotFound(table_id)),
        }
    }

    async fn list_tables(&self) -> Result<Vec<TableConfig>, MetadataError> {
        let rows = sqlx::query!(
            r#"
            SELECT id, config FROM tables WHERE deleted_at IS NULL
            "#
        )
        .fetch_all(&self.pg_pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let mut config: TableConfig =
                    serde_json::from_value(row.config).map_err(MetadataError::JsonSerdeError)?;
                config.table_id = Some(TableID::from(row.id));
                Ok(config)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        requires_docker,
        test_utils::setup_test_db,
    };

    #[tokio::test]
    async fn test_tables() {
        requires_docker!();
        // Setup test database
        let (metadata_store, _container) = setup_test_db().await.unwrap();

        let result = metadata_store.get_table(TableName::from("test_table")).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MetadataError::TableNotFound(_)));

        // Create a table
        metadata_store
            .create_table(TableConfig {
                table_id: None,
                table_name: TableName::from("test_table"),
            })
            .await
            .unwrap();

        // Try to create the same table again
        let result = metadata_store
            .create_table(TableConfig {
                table_id: None,
                table_name: TableName::from("test_table"),
            })
            .await;

        // Verify that the second creation returns an error
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), MetadataError::TableAlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_schedule_and_get_wal_compaction_job() {
        requires_docker!();
        // Setup test database
        let (metadata_store, _container) = setup_test_db().await.unwrap();

        // Schedule a WAL compaction job
        metadata_store.schedule_job(JobParams::WALCompaction).await.unwrap();

        // Confirm job is pending
        let job = metadata_store.get_job(JobID::from(1)).await.unwrap();
        assert_eq!(job.status, JobStatus::Pending);

        // Get pending jobs
        let pending_jobs = metadata_store.get_pending_jobs().await.unwrap();

        // Verify that we have exactly one job
        assert_eq!(pending_jobs.len(), 1);

        // Verify the job ID is positive
        let (_, job_params) = &pending_jobs[0];

        // Verify the job parameters match WALCompaction
        match job_params {
            JobParams::WALCompaction => {},
            JobParams::TableBufferCompaction(_) => {
                panic!("Expected WALCompaction job, got TableBufferCompaction");
            },
            JobParams::TableTreeCompaction(..) => {
                panic!("Expected WALCompaction job, got TableTreeCompaction");
            },
        }

        // Schedule another job - should not create a duplicate
        metadata_store.schedule_job(JobParams::WALCompaction).await.unwrap();

        // Verify we still have only one job
        let pending_jobs_after_second_schedule = metadata_store.get_pending_jobs().await.unwrap();
        assert_eq!(pending_jobs_after_second_schedule.len(), 1);
    }
}
