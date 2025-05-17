use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::{Stream, StreamExt, pin_mut};
use prost::Message;
use thiserror::Error;
use tracing::{debug, error};

use crate::metadata::{
    self, ChangelogEntry, ChangelogEntryWithID, Level, MetadataError, MetadataStore, SeqNo, TableID,
};
use crate::storage::{ObjectStore, StorageError};
use crate::{proto, runs};

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
pub struct Table {
    pub buffer: BTreeMap<metadata::SeqNo, metadata::RunMetadata>,
    pub tree: BTreeMap<metadata::Level, BTreeMap<runs::Key, metadata::RunMetadata>>,
}

#[derive(Default, Clone)]
pub struct Snapshot {
    pub seq_no: metadata::SeqNo,
    pub wal: BTreeMap<metadata::SeqNo, metadata::RunMetadata>,
    pub tables: HashMap<metadata::TableID, Table>,
}

impl From<Snapshot> for proto::Snapshot {
    fn from(snapshot: Snapshot) -> Self {
        let mut response = proto::Snapshot {
            seq_no: snapshot.seq_no.into(),
            wal: snapshot.wal.values().cloned().map(|r| r.into()).collect(),
            tables: vec![],
        };

        for (table_id, table) in snapshot.tables.iter() {
            let mut table_response = proto::Table {
                id: (*table_id).into(),
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

        response
    }
}

impl From<proto::Snapshot> for Snapshot {
    fn from(snapshot: proto::Snapshot) -> Self {
        let mut wal = BTreeMap::new();
        let mut tables = HashMap::new();

        for table in snapshot.tables {
            let mut table_data = Table {
                buffer: BTreeMap::new(),
                tree: BTreeMap::new(),
            };

            for (i, level) in table.levels.into_iter().enumerate() {
                let mut level_data = BTreeMap::new();
                for run in level.tree {
                    match run.stats {
                        Some(proto::run_metadata::Stats::StatsV1(ref stats)) => {
                            level_data.insert(stats.min_key.clone(), run.into());
                        },
                        None => panic!("Stats are not present"),
                    }
                }

                table_data.tree.insert(Level::from(i as u64), level_data);
            }

            tables.insert(TableID::from(table.id), table_data);
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
            wal,
            tables,
        }
    }
}

impl Snapshot {
    pub async fn from_raw<I>(seq_no: metadata::SeqNo, runs: I) -> Self
    where
        I: IntoIterator<Item = metadata::RunMetadata>,
    {
        let mut wal = BTreeMap::new();
        let mut tables: HashMap<_, Table> = HashMap::new();

        for run_metadata in runs {
            let min_key = match run_metadata.stats {
                runs::Stats::StatsV1(ref stats) => stats.min_key.clone(),
            };

            match run_metadata.belongs_to {
                metadata::BelongsTo::WalSeqNo(seq_no) => {
                    wal.insert(seq_no, run_metadata);
                },
                metadata::BelongsTo::TableBuffer(ref table_id, seq_no) => {
                    tables
                        .entry(*table_id)
                        .or_default()
                        .buffer
                        .insert(seq_no, run_metadata);
                },
                metadata::BelongsTo::TableTree(ref table_id, level) => {
                    tables
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
            wal,
            tables,
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
/// It streams the changelog from metadata store and loads run metadata for runs.
#[derive(Clone)]
pub struct ForestImpl {
    metadata_store: MetadataStore,
    state: Arc<Mutex<Arc<Snapshot>>>,
}

impl ForestImpl {
    pub async fn latest(
        metadata_store: MetadataStore,
        object_store: ObjectStore,
    ) -> Result<Forest, ForestError> {
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

        Ok(Arc::new(forest))
    }

    /// Creates a new Forest instance and starts the changelog stream processor.
    pub async fn watch(
        metadata_store: MetadataStore,
        object_store: ObjectStore,
    ) -> Result<Forest, ForestError> {
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

        // Start processing changelog in a background task
        let processor = forest.clone();
        tokio::spawn(async move {
            if let Err(e) = processor.process_changelog_stream(stream).await {
                error!("Changelog processor terminated with error: {e}");
            }
        });

        Ok(Arc::new(forest))
    }

    /// Continuously processes the changelog stream and updates the in-memory map.
    async fn process_changelog_stream(
        &self,
        stream: impl Stream<Item = Result<ChangelogEntryWithID, MetadataError>> + '_,
    ) -> Result<(), ForestError> {
        // Pin the stream to the stack
        pin_mut!(stream);

        while let Some(result) = stream.next().await {
            debug!("Received changelog entry: {:?}", result);
            match result {
                Ok(entry) => {
                    self.process_changelog_entry(entry).await?;
                },
                Err(e) => {
                    error!("Error reading from changelog: {e}");
                    // Continue processing despite errors
                },
            }
        }

        Err(ForestError::Internal(
            "Changelog stream ended unexpectedly".to_string(),
        ))
    }

    /// Process a single changelog entry and update the live runs map.
    async fn process_changelog_entry(
        &self,
        entry: ChangelogEntryWithID,
    ) -> Result<(), ForestError> {
        let (runs_added, runs_removed) = match entry.changes {
            ChangelogEntry::V1(v1) => (
                v1.runs_added,
                v1.runs_removed.into_iter().collect::<HashSet<_>>(),
            ),
        };

        let new_runs = self
            .metadata_store
            .get_run_metadata_batch(runs_added)
            .await?;

        let mut state = self.state.lock().unwrap();
        let new_state = Arc::make_mut(&mut state);

        // Remove runs that are no longer present
        new_state.wal.retain(|_, m| !runs_removed.contains(&m.id));
        for table in new_state.tables.values_mut() {
            table.buffer.retain(|_, m| !runs_removed.contains(&m.id));
            for table_entries in table.tree.values_mut() {
                table_entries.retain(|_, m| !runs_removed.contains(&m.id));
            }
            table
                .tree
                .retain(|_, table_entries| !table_entries.is_empty());
        }
        new_state
            .tables
            .retain(|_, table| !table.buffer.is_empty() || !table.tree.is_empty());

        // Add new runs
        for new_run in new_runs.into_values() {
            match new_run.belongs_to {
                metadata::BelongsTo::WalSeqNo(seq_no) => {
                    new_state.wal.insert(seq_no, new_run);
                },
                metadata::BelongsTo::TableBuffer(table_id, seq_no) => {
                    new_state
                        .tables
                        .entry(table_id)
                        .or_default()
                        .buffer
                        .insert(seq_no, new_run);
                },
                metadata::BelongsTo::TableTree(table_id, level) => {
                    let min_key = match new_run.stats {
                        runs::Stats::StatsV1(ref stats) => stats.min_key.clone(),
                    };

                    new_state
                        .tables
                        .entry(table_id)
                        .or_default()
                        .tree
                        .entry(level)
                        .or_default()
                        .insert(min_key, new_run);
                },
            }
        }

        new_state.seq_no = entry.id;
        Ok(())
    }
}

impl ForestTrait for ForestImpl {
    fn get_state(&self) -> Arc<Snapshot> {
        self.state.lock().unwrap().clone()
    }
}
