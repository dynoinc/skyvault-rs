use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use futures::{StreamExt, pin_mut};
use thiserror::Error;
use tracing::{debug, error};

use crate::metadata::{ChangelogEntry, MetadataError, MetadataStore};
use crate::proto;

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
    live_runs: Arc<Mutex<Arc<HashMap<String, proto::RunMetadata>>>>,
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
    pub fn get_live_runs(&self) -> Arc<HashMap<String, proto::RunMetadata>> {
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
                    debug!(
                        "Processing changelog entry: sequence={}",
                        entry.sequence_number
                    );
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
        let metadata_batch = self.metadata_store.get_run_metadata_batch(&entry.runs_added).await?;

        let mut live_runs = self.live_runs.lock().unwrap();
        let new_map = Arc::make_mut(&mut live_runs);

        for run_id in &entry.runs_removed {
            new_map.remove(run_id);
        }

        for (run_id, metadata) in metadata_batch {
            new_map.insert(run_id.clone(), metadata);
        }

        *live_runs = Arc::new(new_map.clone());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::pin_mut;
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::proto;
    use crate::test_utils::setup_test_db;

    #[tokio::test]
    async fn test_forest_tracks_live_runs() {
        // Setup a real test DB
        let (metadata_store, _container) = setup_test_db().await.unwrap();

        // Create metadata for test runs
        let run1 = proto::RunMetadata {
            id: "run1".to_string(),
            stats: Some(proto::run_metadata::Stats::StatsV1(proto::StatsV1 {
                min_key: "run1-min".to_string(),
                max_key: "run1-max".to_string(),
                size_bytes: 100,
            })),
        };

        let run2 = proto::RunMetadata {
            id: "run2".to_string(),
            stats: Some(proto::run_metadata::Stats::StatsV1(proto::StatsV1 {
                min_key: "run2-min".to_string(),
                max_key: "run2-max".to_string(),
                size_bytes: 200,
            })),
        };

        let run3 = proto::RunMetadata {
            id: "run3".to_string(),
            stats: Some(proto::run_metadata::Stats::StatsV1(proto::StatsV1 {
                min_key: "run3-min".to_string(),
                max_key: "run3-max".to_string(),
                size_bytes: 300,
            })),
        };

        let run4 = proto::RunMetadata {
            id: "run4".to_string(),
            stats: Some(proto::run_metadata::Stats::StatsV1(proto::StatsV1 {
                min_key: "run4-min".to_string(),
                max_key: "run4-max".to_string(),
                size_bytes: 400,
            })),
        };

        // Add initial changelog entries
        metadata_store
            .append_changelog(vec![run1.clone(), run2.clone()], vec![])
            .await
            .unwrap();

        // Create a Forest instance
        let forest = Forest::new(metadata_store.clone()).await.unwrap();

        // Wait for changelog processing
        sleep(Duration::from_millis(200)).await;

        // Verify the forest tracked runs correctly
        let live_runs = forest.get_live_runs();
        assert_eq!(live_runs.len(), 2);
        assert!(live_runs.contains_key("run1"));
        assert!(live_runs.contains_key("run2"));

        // Add another changelog entry
        metadata_store
            .append_changelog(vec![run3.clone()], vec!["run1".to_string()])
            .await
            .unwrap();

        // Wait for changelog processing
        sleep(Duration::from_millis(200)).await;

        // Verify the forest updated correctly
        let live_runs = forest.get_live_runs();
        assert_eq!(live_runs.len(), 2);
        assert!(live_runs.contains_key("run2"));
        assert!(live_runs.contains_key("run3"));
        assert!(!live_runs.contains_key("run1"));

        // Add a final changelog entry
        metadata_store
            .append_changelog(vec![run4.clone()], vec!["run2".to_string()])
            .await
            .unwrap();

        // Wait for changelog processing
        sleep(Duration::from_millis(200)).await;

        // Verify the forest updated correctly
        let live_runs = forest.get_live_runs();
        assert_eq!(live_runs.len(), 2);
        assert!(live_runs.contains_key("run3"));
        assert!(live_runs.contains_key("run4"));
        assert!(!live_runs.contains_key("run1"));
        assert!(!live_runs.contains_key("run2"));
    }

    #[tokio::test]
    async fn test_forest_stream_pinning() {
        // This test focuses on handling the stream correctly with pinning
        let (metadata_store, _container) = setup_test_db().await.unwrap();

        // Create a stream and pin it
        let stream = metadata_store.stream_changelog();
        pin_mut!(stream);

        // Add a changelog entry
        let run1 = proto::RunMetadata {
            id: "pin-test-run1".to_string(),
            stats: Some(proto::run_metadata::Stats::StatsV1(proto::StatsV1 {
                min_key: "min".to_string(),
                max_key: "max".to_string(),
                size_bytes: 100,
            })),
        };

        metadata_store
            .append_changelog(vec![run1], vec![])
            .await
            .unwrap();

        // Wait for entry and read it
        let result = timeout(Duration::from_secs(5), stream.next()).await;
        assert!(result.is_ok(), "Timed out waiting for changelog entry");

        if let Ok(Some(Ok(entry))) = result {
            assert_eq!(entry.sequence_number, 1);
            assert_eq!(entry.runs_added, vec!["pin-test-run1"]);
            assert_eq!(entry.runs_removed, Vec::<String>::new());
        } else {
            panic!("Expected a valid changelog entry");
        }
    }
}
