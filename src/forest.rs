use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use futures::{Stream, StreamExt, pin_mut};
use thiserror::Error;
use tracing::{debug, error};

use crate::metadata::{self, ChangelogEntry, MetadataError, MetadataStore};

#[derive(Error, Debug)]
pub enum ForestError {
    #[error("Metadata error: {0}")]
    MetadataError(#[from] MetadataError),

    #[error("Internal error: {0}")]
    Internal(String),
}

#[derive(Clone)]
pub struct State {
    pub wal: Vec<metadata::RunMetadata>,
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
        let (snapshot, stream) = metadata_store.clone().get_changelog().await?;

        let mut wal_runs = HashSet::new();
        for entry in snapshot {
            match entry {
                ChangelogEntry::V1(v1) => {
                    for run_id in v1.runs_added {
                        wal_runs.insert(run_id);
                    }
                    for run_id in v1.runs_removed {
                        wal_runs.remove(&run_id);
                    }
                },
            }
        }

        let mut wal: Vec<metadata::RunMetadata> = metadata_store
            .get_run_metadata_batch(wal_runs)
            .await
            .expect("Failed to get run metadata")
            .into_values()
            .collect();

        wal.sort_by(|a, b| {
            match (&a.belongs_to, &b.belongs_to) {
                (metadata::BelongsTo::WalSeqNo(a_seq), metadata::BelongsTo::WalSeqNo(b_seq)) => {
                    a_seq.cmp(b_seq)
                },
                // Handle other cases (though they shouldn't occur for WAL runs)
                _ => std::cmp::Ordering::Equal,
            }
        });

        let forest = Self {
            metadata_store,
            state: Arc::new(Mutex::new(Arc::new(State { wal }))),
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
        let new_wal = Arc::make_mut(&mut state)
            .wal
            .iter()
            .filter(|run| !runs_removed.contains(&run.id))
            .cloned()
            .chain(new_runs.into_values())
            .collect();

        *state = Arc::new(State { wal: new_wal });

        Ok(())
    }
}
