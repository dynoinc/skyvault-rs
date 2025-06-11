use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    sync::{
        Arc,
        Mutex,
    },
};

use bytes::Bytes;
use futures::{
    Stream,
    StreamExt,
    pin_mut,
};
use prost::Message;
use thiserror::Error;
use tracing::{
    debug,
    error,
};

use crate::{
    metadata::{
        self,
        ChangelogEntry,
        ChangelogEntryWithID,
        Level,
        MetadataError,
        MetadataStore,
        RunsChangelogEntryV1,
        SeqNo,
        TableChangelogEntryV1,
        TableConfig,
        TableID,
        TableName,
    },
    proto,
    runs,
    storage::{
        ObjectStore,
        StorageError,
    },
};

#[derive(Error, Debug)]
pub enum ForestError {
    #[error("Metadata error: {0}")]
    MetadataError(#[from] MetadataError),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),

    #[error("Bad snapshot: {0}")]
    BadSnapshot(#[from] prost::DecodeError),
}

#[derive(Default, Clone)]
pub struct TableTree {
    pub buffer: BTreeMap<metadata::SeqNo, metadata::RunMetadata>,
    pub tree: BTreeMap<metadata::Level, BTreeMap<runs::Key, metadata::RunMetadata>>,
}

#[derive(Default, Clone)]
pub struct Snapshot {
    pub seq_no: metadata::SeqNo,

    // Tables
    pub tables: HashMap<metadata::TableName, TableConfig>,

    // Runs
    pub wal: BTreeMap<metadata::SeqNo, metadata::RunMetadata>,
    pub trees: HashMap<metadata::TableID, TableTree>,
}

impl From<Snapshot> for proto::Snapshot {
    fn from(snapshot: Snapshot) -> Self {
        let mut response = proto::Snapshot {
            seq_no: snapshot.seq_no.into(),

            // Tables
            tables: snapshot
                .tables
                .iter()
                .map(|(name, config)| (name.to_string(), config.into()))
                .collect(),

            // Runs
            wal: snapshot.wal.values().cloned().map(|r| r.into()).collect(),
            trees: vec![],
        };

        for (table_id, table) in snapshot.trees.iter() {
            let mut table_response = proto::TableTree {
                table_id: (*table_id).into(),
                buffer: table.buffer.values().cloned().map(|r| r.into()).collect(),
                levels: vec![],
            };

            for (level, level_data) in table.tree.iter() {
                let level_response = proto::TableLevel {
                    level: (*level).into(),
                    runs: level_data.values().cloned().map(|r| r.into()).collect(),
                };

                table_response.levels.push(level_response);
            }

            response.trees.push(table_response);
        }

        response
    }
}

impl From<proto::Snapshot> for Snapshot {
    fn from(snapshot: proto::Snapshot) -> Self {
        let mut wal = BTreeMap::new();
        let mut trees = HashMap::new();

        for table in snapshot.trees {
            let mut table_data = TableTree {
                buffer: BTreeMap::new(),
                tree: BTreeMap::new(),
            };

            for (i, tree) in table.levels.into_iter().enumerate() {
                let mut level_data = BTreeMap::new();
                for run in tree.runs {
                    match run.stats {
                        Some(proto::run_metadata::Stats::StatsV1(ref stats)) => {
                            level_data.insert(stats.min_key.clone(), run.into());
                        },
                        None => panic!("Stats are not present"),
                    }
                }

                table_data.tree.insert(Level::from(i as u64), level_data);
            }

            trees.insert(TableID::from(table.table_id), table_data);
        }

        for run in snapshot.wal {
            match run.belongs_to {
                Some(proto::run_metadata::BelongsTo::WalSeqNo(seq_no)) => {
                    wal.insert(SeqNo::from(seq_no), run.into());
                },
                Some(other) => panic!("Invalid belongs to for wal: {other:?}"),
                None => panic!("Belongs to is not present"),
            }
        }

        Self {
            seq_no: snapshot.seq_no.into(),
            tables: snapshot
                .tables
                .into_iter()
                .map(|(name, config)| (TableName::from(name), config.into()))
                .collect(),
            wal,
            trees,
        }
    }
}

impl Snapshot {
    pub async fn from_parts<I>(
        seq_no: metadata::SeqNo,
        tables: HashMap<metadata::TableName, TableConfig>,
        runs: I,
    ) -> Self
    where
        I: IntoIterator<Item = metadata::RunMetadata>,
    {
        let mut wal = BTreeMap::new();
        let mut trees: HashMap<_, TableTree> = HashMap::new();

        for run_metadata in runs {
            let min_key = match run_metadata.stats {
                runs::Stats::StatsV1(ref stats) => stats.min_key.clone(),
            };

            match run_metadata.belongs_to {
                metadata::BelongsTo::WalSeqNo(seq_no) => {
                    wal.insert(seq_no, run_metadata);
                },
                metadata::BelongsTo::TableBuffer(ref table_id, seq_no) => {
                    trees.entry(*table_id).or_default().buffer.insert(seq_no, run_metadata);
                },
                metadata::BelongsTo::TableTreeLevel(ref table_id, level) => {
                    trees
                        .entry(*table_id)
                        .or_default()
                        .tree
                        .entry(level)
                        .or_default()
                        .insert(min_key, run_metadata);
                },
            }
        }

        Self {
            seq_no,
            tables,
            wal,
            trees,
        }
    }

    pub async fn from_bytes(snapshot: Bytes) -> Result<Self, ForestError> {
        let snapshot = proto::Snapshot::decode(snapshot.as_ref())?;
        Ok(Snapshot::from(snapshot))
    }
}

#[cfg_attr(test, mockall::automock)]
pub trait ForestTrait: Send + Sync + 'static {
    fn get_state(&self) -> Arc<Snapshot>;
}

pub type Forest = Arc<dyn ForestTrait>;

/// Forest maintains an in-memory map of all runs.
/// It streams the changelog from metadata store and loads run metadata for
/// runs.
#[derive(Clone)]
pub struct ForestImpl {
    metadata_store: MetadataStore,
    state: Arc<Mutex<Arc<Snapshot>>>,
}

impl ForestImpl {
    pub async fn latest(
        metadata_store: MetadataStore,
        object_store: ObjectStore,
    ) -> Result<Arc<Snapshot>, ForestError> {
        let (snapshot_id, entries) = metadata_store.get_latest_snapshot().await?;
        let snapshot = match snapshot_id {
            Some(snapshot_id) => {
                let bytes = object_store.get_snapshot(snapshot_id).await?;
                Snapshot::from_bytes(bytes).await?
            },
            None => Snapshot::default(),
        };

        let forest = Self {
            metadata_store,
            state: Arc::new(Mutex::new(Arc::new(snapshot))),
        };

        for entry in entries {
            forest.process_changelog_entry(entry).await?;
        }

        Ok(forest.get_state())
    }

    /// Creates a new Forest instance with a custom changelog stream processor.
    pub async fn watch<F, S>(
        metadata_store: MetadataStore,
        object_store: ObjectStore,
        stream_transformer: F,
    ) -> Result<Forest, ForestError>
    where
        F: FnOnce(Box<dyn Stream<Item = Result<ChangelogEntryWithID, MetadataError>> + Send + Unpin>) -> S,
        S: Stream<Item = Result<ChangelogEntryWithID, MetadataError>> + Send + 'static,
    {
        let (snapshot_id, stream) = metadata_store.stream_changelog().await?;
        let state = match snapshot_id {
            Some(snapshot_id) => {
                let snapshot = object_store.get_snapshot(snapshot_id).await?;
                Snapshot::from_bytes(snapshot).await?
            },
            None => Snapshot::default(),
        };

        let forest = Self {
            metadata_store,
            state: Arc::new(Mutex::new(Arc::new(state))),
        };

        // Transform the stream
        let transformed_stream = stream_transformer(Box::new(stream));

        // Start processing in a background task
        let processor = forest.clone();
        tokio::spawn(async move {
            if let Err(e) = processor.process_changelog_stream(transformed_stream).await {
                panic!("Changelog processor terminated: {e}");
            }
        });

        Ok(Arc::new(forest))
    }

    /// Continuously processes the changelog stream and updates the in-memory
    /// map.
    async fn process_changelog_stream(
        &self,
        stream: impl Stream<Item = Result<ChangelogEntryWithID, MetadataError>> + '_,
    ) -> Result<(), ForestError> {
        // Pin the stream to the stack
        pin_mut!(stream);

        while let Some(result) = stream.next().await {
            debug!("Received changelog entry: {:?}", result);
            self.process_changelog_entry(result?).await?;
        }

        Err(ForestError::Internal("Changelog stream ended unexpectedly".to_string()))
    }

    /// Process a single changelog entry and update the live runs map.
    async fn process_changelog_entry(&self, entry: ChangelogEntryWithID) -> Result<(), ForestError> {
        let seq_no = entry.id;
        match entry.changes {
            ChangelogEntry::RunsV1(v1) => self.process_runs_changelog_entry(seq_no, v1).await,
            ChangelogEntry::TablesV1(v1) => self.process_tables_changelog_entry(seq_no, v1).await,
        }
    }

    async fn process_runs_changelog_entry(
        &self,
        seq_no: SeqNo,
        entry: RunsChangelogEntryV1,
    ) -> Result<(), ForestError> {
        let runs_added = entry.runs_added;
        let runs_removed = entry.runs_removed;

        let new_runs = self.metadata_store.get_run_metadata_batch(runs_added).await?;

        let mut state = self.state.lock().unwrap();
        let new_state = Arc::make_mut(&mut state);

        // Remove runs that are no longer present
        new_state.wal.retain(|_, m| !runs_removed.contains(&m.id));
        for table in new_state.trees.values_mut() {
            table.buffer.retain(|_, m| !runs_removed.contains(&m.id));
            for table_entries in table.tree.values_mut() {
                table_entries.retain(|_, m| !runs_removed.contains(&m.id));
            }
            table.tree.retain(|_, table_entries| !table_entries.is_empty());
        }
        new_state
            .trees
            .retain(|_, table| !table.buffer.is_empty() || !table.tree.is_empty());

        // Add new runs
        for new_run in new_runs.into_values() {
            match new_run.belongs_to {
                metadata::BelongsTo::WalSeqNo(seq_no) => {
                    new_state.wal.insert(seq_no, new_run);
                },
                metadata::BelongsTo::TableBuffer(table_id, seq_no) => {
                    new_state
                        .trees
                        .entry(table_id)
                        .or_default()
                        .buffer
                        .insert(seq_no, new_run);
                },
                metadata::BelongsTo::TableTreeLevel(table_id, level) => {
                    let min_key = match new_run.stats {
                        runs::Stats::StatsV1(ref stats) => stats.min_key.clone(),
                    };

                    new_state
                        .trees
                        .entry(table_id)
                        .or_default()
                        .tree
                        .entry(level)
                        .or_default()
                        .insert(min_key, new_run);
                },
            }
        }

        new_state.seq_no = seq_no;
        Ok(())
    }

    async fn process_tables_changelog_entry(
        &self,
        seq_no: SeqNo,
        entry: TableChangelogEntryV1,
    ) -> Result<(), ForestError> {
        let table_id = match entry {
            TableChangelogEntryV1::TableCreated(table_id) => table_id,
            TableChangelogEntryV1::TableDropped(table_id) => table_id,
        };
        let table_config = self.metadata_store.get_table_by_id(table_id).await?;

        let mut state = self.state.lock().unwrap();
        let new_state = Arc::make_mut(&mut state);

        match entry {
            TableChangelogEntryV1::TableCreated(_) => {
                tracing::debug!("Table created: {:?}", table_config);
                new_state.tables.insert(table_config.table_name.clone(), table_config);
            },
            TableChangelogEntryV1::TableDropped(_) => {
                tracing::debug!("Table removed: {:?}", table_config);
                new_state.tables.remove(&table_config.table_name);
                new_state.trees.remove(&table_id);
            },
        }

        new_state.seq_no = seq_no;
        Ok(())
    }
}

impl ForestTrait for ForestImpl {
    fn get_state(&self) -> Arc<Snapshot> {
        self.state.lock().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        requires_docker,
        test_utils::{
            setup_test_db,
            setup_test_object_store,
        },
    };

    #[tokio::test]
    async fn test_forest_handles_table_create_and_drop() {
        requires_docker!();

        // Setup test database and object store
        let metadata_store = setup_test_db().await.unwrap();
        let object_store = setup_test_object_store().await.unwrap();

        // Create a test table
        let table_config = TableConfig {
            table_id: None,
            table_name: TableName::from("test_forest_table"),
        };

        metadata_store.create_table(table_config.clone()).await.unwrap();
        metadata_store
            .drop_table(table_config.table_name.clone())
            .await
            .unwrap();
        assert!(
            ForestImpl::latest(metadata_store.clone(), object_store.clone())
                .await
                .is_ok()
        );
    }
}
