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

#[derive(Default, Clone, Debug)]
pub struct TableTree {
    pub buffer: BTreeMap<metadata::SeqNo, metadata::RunMetadata>,
    pub tree: BTreeMap<metadata::Level, BTreeMap<runs::Key, metadata::RunMetadata>>,
}

#[derive(Default, Clone, Debug)]
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

        forest.apply_changelog_entries(entries).await?;
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
            self.apply_changelog_entries(vec![result?]).await?;
        }

        Err(ForestError::Internal("Changelog stream ended unexpectedly".to_string()))
    }

    /// Apply multiple changelog entries efficiently with batched database calls
    async fn apply_changelog_entries(&self, entries: Vec<ChangelogEntryWithID>) -> Result<(), ForestError> {
        // Collect all run IDs that need to be fetched across all entries
        let mut all_runs_to_add = Vec::new();
        let mut all_runs_to_remove = Vec::new();
        let mut table_ids_to_fetch = Vec::new();

        for entry in &entries {
            match &entry.changes {
                ChangelogEntry::RunsV1(v1) => {
                    all_runs_to_add.extend(v1.runs_added.iter().cloned());
                    all_runs_to_remove.extend(v1.runs_removed.iter().cloned());
                },
                ChangelogEntry::TablesV1(v1) => {
                    let table_id = match v1 {
                        TableChangelogEntryV1::TableCreated(table_id) => *table_id,
                        TableChangelogEntryV1::TableDropped(table_id) => *table_id,
                    };
                    table_ids_to_fetch.push(table_id);
                },
            }
        }

        // Optimize: only fetch runs that aren't immediately removed
        // Remove runs that are both added and removed in the same batch
        all_runs_to_add.retain(|run_id| !all_runs_to_remove.contains(run_id));

        // Fetch all data in parallel batches
        let all_run_metadata = self.metadata_store.get_run_metadata_batch(all_runs_to_add).await?;

        let mut table_configs = HashMap::new();
        for table_id in table_ids_to_fetch {
            let config = self.metadata_store.get_table_by_id(table_id).await?;
            table_configs.insert(table_id, config);
        }

        // Apply all updates atomically without holding lock across async operations
        let mut state = self.state.lock().unwrap();
        let new_state = Arc::make_mut(&mut state);

        // First apply all run removals in batch
        if !all_runs_to_remove.is_empty() {
            new_state.wal.retain(|_, m| !all_runs_to_remove.contains(&m.id));
            for table in new_state.trees.values_mut() {
                table.buffer.retain(|_, m| !all_runs_to_remove.contains(&m.id));
                for table_entries in table.tree.values_mut() {
                    table_entries.retain(|_, m| !all_runs_to_remove.contains(&m.id));
                }
            }
        }

        // Then apply all other changes
        for entry in entries {
            let seq_no = entry.id;
            match entry.changes {
                ChangelogEntry::RunsV1(v1) => {
                    // Add new runs using pre-fetched metadata
                    for run_id in v1.runs_added {
                        if let Some(new_run) = all_run_metadata.get(&run_id).cloned() {
                            match new_run.belongs_to {
                                metadata::BelongsTo::WalSeqNo(seq_no) => {
                                    new_state.wal.insert(seq_no, new_run);
                                },
                                metadata::BelongsTo::TableBuffer(table_id, seq_no) => {
                                    if let Some(table) = new_state.trees.get_mut(&table_id) {
                                        table.buffer.insert(seq_no, new_run);
                                    }
                                },
                                metadata::BelongsTo::TableTreeLevel(table_id, level) => {
                                    let min_key = match new_run.stats {
                                        runs::Stats::StatsV1(ref stats) => stats.min_key.clone(),
                                    };

                                    if let Some(table) = new_state.trees.get_mut(&table_id) {
                                        table.tree.entry(level).or_default().insert(min_key, new_run);
                                    }
                                },
                            }
                        }
                    }
                },
                ChangelogEntry::TablesV1(v1) => {
                    let table_id = match v1 {
                        TableChangelogEntryV1::TableCreated(table_id) => table_id,
                        TableChangelogEntryV1::TableDropped(table_id) => table_id,
                    };
                    let table_config = table_configs.get(&table_id).unwrap().clone();

                    match v1 {
                        TableChangelogEntryV1::TableCreated(_) => {
                            debug!("Table created: {:?}", table_config);
                            new_state.tables.insert(table_config.table_name.clone(), table_config);
                            new_state.trees.insert(table_id, TableTree::default());
                        },
                        TableChangelogEntryV1::TableDropped(_) => {
                            debug!("Table removed: {:?}", table_config);
                            new_state.tables.remove(&table_config.table_name);
                            new_state.trees.remove(&table_id);
                        },
                    }
                },
            }

            new_state.seq_no = seq_no;
        }

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

    #[tokio::test]
    async fn test_forest_handles_table_create_and_drop() {
        use std::sync::Arc;

        use crate::{
            metadata::{
                ChangelogEntry,
                ChangelogEntryWithID,
                MockMetadataStoreTrait,
                SeqNo,
                TableChangelogEntryV1,
                TableID,
            },
            storage::MockObjectStoreTrait,
        };

        // Create mock metadata store
        let mut mock_metadata_store = MockMetadataStoreTrait::new();

        // Mock get_latest_snapshot to return some table changelog entries
        let table_id = TableID::from(1);
        let table_config = TableConfig {
            table_id: Some(table_id),
            table_name: TableName::from("test_forest_table"),
        };

        mock_metadata_store
            .expect_get_latest_snapshot()
            .once()
            .returning(move || {
                let entries = vec![
                    ChangelogEntryWithID {
                        id: SeqNo::from(1),
                        changes: ChangelogEntry::TablesV1(TableChangelogEntryV1::TableCreated(table_id)),
                    },
                    ChangelogEntryWithID {
                        id: SeqNo::from(2),
                        changes: ChangelogEntry::TablesV1(TableChangelogEntryV1::TableDropped(table_id)),
                    },
                ];
                Box::pin(async { Ok((None, entries)) })
            });

        // Mock get_run_metadata_batch to return empty since table entries don't involve
        // runs
        mock_metadata_store
            .expect_get_run_metadata_batch()
            .returning(|_| Box::pin(async { Ok(HashMap::new()) }));

        // Mock get_table_by_id calls for both create and drop operations
        let table_config_clone = table_config.clone();
        mock_metadata_store
            .expect_get_table_by_id()
            .with(mockall::predicate::eq(table_id))
            .times(2) // Called once for create, once for drop
            .returning(move |_| {
                let config = table_config_clone.clone();
                Box::pin(async move { Ok(config) })
            });

        let metadata_store = Arc::new(mock_metadata_store);

        // Create mock object store (no snapshot to load)
        let mock_object_store = MockObjectStoreTrait::new();
        // No expectations set since get_latest_snapshot returns None for snapshot_id

        let object_store = Arc::new(mock_object_store);

        // Test ForestImpl::latest processes the changelog entries correctly
        let result = ForestImpl::latest(metadata_store, object_store).await;
        assert!(result.is_ok(), "Forest should handle table create and drop: {result:?}");

        let snapshot = result.unwrap();
        assert_eq!(snapshot.seq_no, SeqNo::from(2));
        // Table should not be present since it was created then dropped
        assert!(
            snapshot.tables.is_empty(),
            "Tables should be empty after create and drop"
        );
    }

    #[tokio::test]
    async fn test_batched_changelog_processing() {
        use std::sync::Arc;

        use crate::{
            metadata::{
                ChangelogEntry,
                ChangelogEntryWithID,
                MockMetadataStoreTrait,
                SeqNo,
                TableChangelogEntryV1,
                TableID,
            },
            storage::MockObjectStoreTrait,
        };

        // Create mock metadata store
        let mut mock_metadata_store = MockMetadataStoreTrait::new();

        // Mock get_latest_snapshot to return multiple table creation entries
        let table_ids = [TableID::from(1), TableID::from(2), TableID::from(3)];
        let table_configs = [
            TableConfig {
                table_id: Some(table_ids[0]),
                table_name: TableName::from("test_batch_table_0"),
            },
            TableConfig {
                table_id: Some(table_ids[1]),
                table_name: TableName::from("test_batch_table_1"),
            },
            TableConfig {
                table_id: Some(table_ids[2]),
                table_name: TableName::from("test_batch_table_2"),
            },
        ];

        mock_metadata_store
            .expect_get_latest_snapshot()
            .once()
            .returning(move || {
                let entries = vec![
                    ChangelogEntryWithID {
                        id: SeqNo::from(1),
                        changes: ChangelogEntry::TablesV1(TableChangelogEntryV1::TableCreated(table_ids[0])),
                    },
                    ChangelogEntryWithID {
                        id: SeqNo::from(2),
                        changes: ChangelogEntry::TablesV1(TableChangelogEntryV1::TableCreated(table_ids[1])),
                    },
                    ChangelogEntryWithID {
                        id: SeqNo::from(3),
                        changes: ChangelogEntry::TablesV1(TableChangelogEntryV1::TableCreated(table_ids[2])),
                    },
                ];
                Box::pin(async { Ok((None, entries)) })
            });

        // Mock get_run_metadata_batch to return empty since table entries don't involve
        // runs
        mock_metadata_store
            .expect_get_run_metadata_batch()
            .returning(|_| Box::pin(async { Ok(HashMap::new()) }));

        // Mock get_table_by_id calls for all three table creations
        for (i, &table_id) in table_ids.iter().enumerate() {
            let config = table_configs[i].clone();
            mock_metadata_store
                .expect_get_table_by_id()
                .with(mockall::predicate::eq(table_id))
                .once()
                .returning(move |_| {
                    let config = config.clone();
                    Box::pin(async move { Ok(config) })
                });
        }

        let metadata_store = Arc::new(mock_metadata_store);

        // Create mock object store
        let mock_object_store = MockObjectStoreTrait::new();
        let object_store = Arc::new(mock_object_store);

        // Test ForestImpl::latest processes multiple changelog entries correctly
        let result = ForestImpl::latest(metadata_store, object_store).await;
        assert!(
            result.is_ok(),
            "Forest should handle multiple table creations: {result:?}"
        );

        let snapshot = result.unwrap();
        assert_eq!(snapshot.seq_no, SeqNo::from(3));
        assert_eq!(snapshot.tables.len(), 3);
        assert!(snapshot.tables.contains_key(&TableName::from("test_batch_table_0")));
        assert!(snapshot.tables.contains_key(&TableName::from("test_batch_table_1")));
        assert!(snapshot.tables.contains_key(&TableName::from("test_batch_table_2")));
    }

    #[tokio::test]
    async fn test_batched_processing_optimizes_cancelled_runs() {
        use std::sync::Arc;

        use crate::{
            metadata::{
                ChangelogEntry,
                ChangelogEntryWithID,
                MockMetadataStoreTrait,
                RunsChangelogEntryV1,
                SeqNo,
            },
            runs::RunID,
        };

        // Create a mock metadata store
        let mut mock_metadata_store = MockMetadataStoreTrait::new();

        // Set expectation that get_run_metadata_batch will be called with empty vector
        // because the optimization should filter out all runs that are cancelled
        mock_metadata_store
            .expect_get_run_metadata_batch()
            .returning(|_| Box::pin(async { Ok(HashMap::new()) }));

        let metadata_store = Arc::new(mock_metadata_store);

        let cancelled_run_1 = RunID::from("cancelled_run_1");
        let cancelled_run_2 = RunID::from("cancelled_run_2");

        // Create entries where runs are added and then removed (cancelled out)
        let entries_with_cancellations = vec![
            ChangelogEntryWithID {
                id: SeqNo::from(1),
                changes: ChangelogEntry::RunsV1(RunsChangelogEntryV1 {
                    runs_added: vec![cancelled_run_1.clone(), cancelled_run_2.clone()],
                    runs_removed: vec![],
                }),
            },
            ChangelogEntryWithID {
                id: SeqNo::from(2),
                changes: ChangelogEntry::RunsV1(RunsChangelogEntryV1 {
                    runs_added: vec![],
                    runs_removed: vec![cancelled_run_1.clone(), cancelled_run_2.clone()],
                }),
            },
        ];

        // Create a forest instance with the mock
        let forest = ForestImpl {
            metadata_store,
            state: Arc::new(Mutex::new(Arc::new(Snapshot::default()))),
        };

        // This should succeed and the mock will verify that get_run_metadata_batch
        // was never called, proving the optimization worked
        let result = forest.apply_changelog_entries(entries_with_cancellations).await;

        // Should succeed because no runs actually need to be fetched
        assert!(result.is_ok(), "Should succeed when runs are cancelled out: {result:?}");

        // Verify the final state has the correct sequence number and no runs
        let state = forest.get_state();
        assert_eq!(state.seq_no, SeqNo::from(2));
        assert!(state.wal.is_empty(), "WAL should be empty after cancellation");

        // Mock expectations are automatically verified when the mock is dropped
    }

    #[tokio::test]
    async fn test_batched_processing_calls_metadata_for_non_cancelled_runs() {
        use std::sync::Arc;

        use crate::{
            metadata::{
                ChangelogEntry,
                ChangelogEntryWithID,
                MockMetadataStoreTrait,
                RunsChangelogEntryV1,
                SeqNo,
            },
            runs::RunID,
        };

        // Create a mock metadata store
        let mut mock_metadata_store = MockMetadataStoreTrait::new();

        let persistent_run = RunID::from("persistent_run");

        // Set expectation that get_run_metadata_batch SHOULD be called exactly once
        // for the non-cancelled run
        mock_metadata_store
            .expect_get_run_metadata_batch()
            .once()
            .withf(move |run_ids| run_ids.len() == 1 && run_ids.contains(&persistent_run))
            .returning(|_| Box::pin(async { Ok(HashMap::new()) })); // Return empty to simulate missing run

        let metadata_store = Arc::new(mock_metadata_store);

        let cancelled_run = RunID::from("cancelled_run");
        let persistent_run = RunID::from("persistent_run");

        // Create entries where one run is cancelled but another persists
        let entries_with_partial_cancellation = vec![
            ChangelogEntryWithID {
                id: SeqNo::from(1),
                changes: ChangelogEntry::RunsV1(RunsChangelogEntryV1 {
                    runs_added: vec![cancelled_run.clone(), persistent_run.clone()],
                    runs_removed: vec![],
                }),
            },
            ChangelogEntryWithID {
                id: SeqNo::from(2),
                changes: ChangelogEntry::RunsV1(RunsChangelogEntryV1 {
                    runs_added: vec![],
                    runs_removed: vec![cancelled_run.clone()], // Only remove one run
                }),
            },
        ];

        // Create a forest instance with the mock
        let forest = ForestImpl {
            metadata_store,
            state: Arc::new(Mutex::new(Arc::new(Snapshot::default()))),
        };

        // This should call get_run_metadata_batch exactly once for the persistent run
        let result = forest.apply_changelog_entries(entries_with_partial_cancellation).await;

        // Should succeed (the mock returns empty HashMap, so no runs get added)
        assert!(
            result.is_ok(),
            "Should succeed when processing non-cancelled runs: {result:?}"
        );

        // Verify the final state has the correct sequence number
        let state = forest.get_state();
        assert_eq!(state.seq_no, SeqNo::from(2));

        // Mock expectations are automatically verified when the mock is dropped
    }

    #[tokio::test]
    async fn test_batched_processing_with_empty_runs() {
        use std::sync::Arc;

        use crate::{
            metadata::{
                ChangelogEntry,
                ChangelogEntryWithID,
                MockMetadataStoreTrait,
                RunsChangelogEntryV1,
                SeqNo,
            },
            runs::RunID,
        };

        // Create mock metadata store
        let mut mock_metadata_store = MockMetadataStoreTrait::new();

        // Set expectation that get_run_metadata_batch will be called with empty vector
        // because all runs are cancelled out
        mock_metadata_store
            .expect_get_run_metadata_batch()
            .returning(|_| Box::pin(async { Ok(HashMap::new()) }));

        let metadata_store = Arc::new(mock_metadata_store);

        // Create changelog entries where all runs are cancelled out
        let cancelled_run_1 = RunID::from("cancelled_run_1");
        let cancelled_run_2 = RunID::from("cancelled_run_2");

        let entries = vec![
            // First entry adds runs
            ChangelogEntryWithID {
                id: SeqNo::from(1),
                changes: ChangelogEntry::RunsV1(RunsChangelogEntryV1 {
                    runs_added: vec![cancelled_run_1.clone(), cancelled_run_2.clone()],
                    runs_removed: vec![],
                }),
            },
            // Second entry removes all the runs
            ChangelogEntryWithID {
                id: SeqNo::from(2),
                changes: ChangelogEntry::RunsV1(RunsChangelogEntryV1 {
                    runs_added: vec![],
                    runs_removed: vec![cancelled_run_1.clone(), cancelled_run_2.clone()],
                }),
            },
        ];

        let forest = ForestImpl {
            metadata_store,
            state: Arc::new(Mutex::new(Arc::new(Snapshot::default()))),
        };

        // This should succeed because all runs are cancelled out, so no metadata
        // fetching is needed
        let result = forest.apply_changelog_entries(entries).await;
        assert!(result.is_ok(), "Should succeed when all runs are cancelled out");

        // Verify the state is updated with the correct sequence number
        let state = forest.get_state();
        assert_eq!(state.seq_no, SeqNo::from(2));

        // Mock expectations are automatically verified when the mock is dropped
    }
}
