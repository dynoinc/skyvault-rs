use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use futures::{StreamExt, pin_mut};
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

type Result<T> = std::result::Result<T, ForestError>;

/// Forest maintains an in-memory map of all live runs.
/// It streams the changelog from metadata store and loads run metadata for live runs.
#[derive(Clone)]
pub struct Forest {
    metadata_store: MetadataStore,
    live_runs: Arc<Mutex<Arc<HashMap<String, metadata::RunMetadata>>>>,
}

impl Forest {
    /// Creates a new Forest instance and starts the changelog stream processor.
    pub async fn new(metadata_store: MetadataStore) -> Result<Self> {
        let forest = Self {
            metadata_store: metadata_store.clone(),
            live_runs: Arc::new(Mutex::new(Arc::new(HashMap::new()))),
        };

        // Start processing changelog in a background task
        let processor = forest.clone();
        tokio::spawn(async move {
            if let Err(e) = processor.process_changelog_stream().await {
                error!("Changelog processor terminated with error: {e}");
            }
        });

        Ok(forest)
    }

    /// Returns the current set of live runs.
    pub fn get_live_runs(&self) -> Arc<HashMap<String, metadata::RunMetadata>> {
        self.live_runs.lock().unwrap().clone()
    }

    /// Continuously processes the changelog stream and updates the in-memory map.
    async fn process_changelog_stream(&self) -> Result<()> {
        let stream = self.metadata_store.stream_changelog();

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
    async fn process_changelog_entry(&self, entry: ChangelogEntry) -> Result<()> {
        let (runs_added, runs_removed) = match entry {
            ChangelogEntry::V1(v1) => (v1.runs_added, v1.runs_removed),
        };

        let metadata_batch = self.metadata_store.get_run_metadata_batch(&runs_added).await?;

        let mut live_runs = self.live_runs.lock().unwrap();
        let new_map = Arc::make_mut(&mut live_runs);

        for run_id in runs_removed {
            new_map.remove(&run_id);
        }
        for run_id in runs_added {
            new_map.insert(run_id.clone(), metadata_batch[&run_id].clone());
        }

        *live_runs = Arc::new(new_map.clone());

        Ok(())
    }
}