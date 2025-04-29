use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use futures::{Stream, StreamExt, pin_mut};
use thiserror::Error;
use tracing::{debug, error};

use crate::metadata::{self, ChangelogEntry, MetadataError, MetadataStore};
use crate::runs;

#[derive(Error, Debug)]
pub enum ForestError {
    #[error("Metadata error: {0}")]
    MetadataError(#[from] MetadataError),

    #[error("Internal error: {0}")]
    Internal(String),
}

#[derive(Clone)]
pub struct State {
    pub wal: BTreeMap<i64, metadata::RunMetadata>,
    pub tables: HashMap<String, BTreeMap<u64, BTreeMap<String, metadata::RunMetadata>>>,
}

/// Forest maintains an in-memory map of all runs.
/// It streams the changelog from metadata store and loads run metadata for runs.
#[derive(Clone)]
pub struct Forest {
    metadata_store: MetadataStore,
    state: Arc<Mutex<Arc<State>>>,
}

impl Forest {
    /// Creates a new Forest instance and starts the changelog stream processor.
    pub async fn new(metadata_store: MetadataStore) -> Result<Self, ForestError> {
        let (snapshot, stream) = metadata_store.get_changelog().await?;

        let mut runs = HashSet::new();
        for entry in snapshot {
            match entry {
                ChangelogEntry::V1(v1) => {
                    for run_id in v1.runs_added {
                        runs.insert(run_id);
                    }
                    for run_id in v1.runs_removed {
                        runs.remove(&run_id);
                    }
                },
            }
        }

        let run_ids = runs.into_iter().collect::<Vec<_>>();
        let run_metadatas = metadata_store.get_run_metadata_batch(run_ids).await?;
        let mut wal = BTreeMap::new();
        let mut tables = HashMap::new();

        for run_metadata in run_metadatas.into_values() {
            let min_key = match run_metadata.stats {
                runs::Stats::StatsV1(ref stats) => stats.min_key.clone(),
            };

            match run_metadata.belongs_to {
                metadata::BelongsTo::WalSeqNo(seq_no) => {
                    wal.insert(seq_no, run_metadata);
                },
                metadata::BelongsTo::TableTree(ref table_name, level) => {
                    tables.entry(table_name.clone())
                        .or_insert_with(BTreeMap::new)
                        .entry(level)
                        .or_insert_with(BTreeMap::new)
                        .insert(min_key, run_metadata);
                },
            }
        }

        let forest = Self {
            metadata_store,
            state: Arc::new(Mutex::new(Arc::new(State { wal, tables }))),
        };

        // Start processing changelog in a background task
        let processor = forest.clone();
        tokio::spawn(async move {
            if let Err(e) = processor.process_changelog_stream(stream).await {
                error!("Changelog processor terminated with error: {e}");
            }
        });

        Ok(forest)
    }

    /// Returns the current set of live runs.
    pub fn get_state(&self) -> Arc<State> {
        self.state.lock().unwrap().clone()
    }

    /// Continuously processes the changelog stream and updates the in-memory map.
    async fn process_changelog_stream(
        &self,
        stream: impl Stream<Item = Result<ChangelogEntry, MetadataError>> + '_,
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
    async fn process_changelog_entry(&self, entry: ChangelogEntry) -> Result<(), ForestError> {
        let (runs_added, runs_removed) = match entry {
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
        for (_, table_levels) in new_state.tables.iter_mut() {
            for (_, table_entries) in table_levels.iter_mut() {
                table_entries.retain(|_, m| !runs_removed.contains(&m.id));
            }

            table_levels.retain(|_, table_entries| !table_entries.is_empty());
        }
        new_state.tables.retain(|_, table_levels| !table_levels.is_empty());

        // Add new runs
        for new_run in new_runs.into_values() {
            match new_run.belongs_to {
                metadata::BelongsTo::WalSeqNo(seq_no) => {
                    new_state.wal.insert(seq_no, new_run);
                },
                metadata::BelongsTo::TableTree(ref table_name, level) => {
                    let min_key = match new_run.stats {
                        runs::Stats::StatsV1(ref stats) => stats.min_key.clone(),
                    };

                    new_state.tables.entry(table_name.clone())
                        .or_insert_with(BTreeMap::new)
                        .entry(level)
                        .or_insert_with(BTreeMap::new)
                        .insert(min_key, new_run);
                }
            }
        }

        Ok(())
    }
}
