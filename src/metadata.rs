use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use async_stream::stream;
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemError;
use aws_sdk_dynamodb::operation::create_table::CreateTableError;
use aws_sdk_dynamodb::operation::get_item::GetItemError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, Put, TransactWriteItem,
};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use futures::Stream;
use serde_dynamo::aws_sdk_dynamodb_1::{from_item, from_items, to_item};
use thiserror::Error;

use crate::proto;
#[cfg(test)]
use crate::test_utils::setup_test_db;

// Define table attributes and schema as global constants
const RUNS_TABLE_NAME: &str = "runs";
const CHANGELOG_TABLE_NAME: &str = "changelog";
const CHANGELOG_TABLE_PK: &str = "placeholder_pk";
static RUNS_TABLE_ATTRIBUTES: OnceLock<Vec<AttributeDefinition>> = OnceLock::new();
static RUNS_TABLE_SCHEMA: OnceLock<Vec<KeySchemaElement>> = OnceLock::new();
static CHANGELOG_TABLE_ATTRIBUTES: OnceLock<Vec<AttributeDefinition>> = OnceLock::new();
static CHANGELOG_TABLE_SCHEMA: OnceLock<Vec<KeySchemaElement>> = OnceLock::new();

// Initialize the global constants
fn init_table_definitions() {
    let _ = RUNS_TABLE_ATTRIBUTES.set(vec![
        AttributeDefinition::builder()
            .attribute_name("id")
            .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
            .build()
            .expect("Failed to build attribute definition"),
    ]);

    let _ = RUNS_TABLE_SCHEMA.set(vec![
        KeySchemaElement::builder()
            .attribute_name("id")
            .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
            .build()
            .expect("Failed to build key schema element"),
    ]);

    let _ = CHANGELOG_TABLE_ATTRIBUTES.set(vec![
        AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
            .build()
            .expect("Failed to build attribute definition"),
        AttributeDefinition::builder()
            .attribute_name("sk")
            .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::N)
            .build()
            .expect("Failed to build attribute definition"),
    ]);

    let _ = CHANGELOG_TABLE_SCHEMA.set(vec![
        KeySchemaElement::builder()
            .attribute_name("pk")
            .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
            .build()
            .expect("Failed to build key schema element"),
        KeySchemaElement::builder()
            .attribute_name("sk")
            .key_type(aws_sdk_dynamodb::types::KeyType::Range)
            .build()
            .expect("Failed to build key schema element"),
    ]);
}

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("Create table error: {0}")]
    CreateTableError(#[from] SdkError<CreateTableError, HttpResponse>),

    #[error("Transact write error: {0}")]
    TransactWriteError(#[from] SdkError<TransactWriteItemsError, HttpResponse>),

    #[error("Get item error: {0}")]
    GetItemError(#[from] SdkError<GetItemError, HttpResponse>),

    #[error("Query error: {0}")]
    QueryError(#[from] SdkError<QueryError, HttpResponse>),

    #[error("Batch get item error: {0}")]
    BatchGetItemError(#[from] SdkError<BatchGetItemError, HttpResponse>),

    #[error("Counter initialization error: {0}")]
    CounterInitError(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Serde error: {0}")]
    SerdeError(#[from] serde_dynamo::Error),
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
pub struct ChangeLogEntryWithID {
    pk: String,
    sk: i64,
    entry: ChangelogEntry,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct NextId {
    pk: String,
    sk: i64,
    pub next_seq_no: i64,
    pub next_changelog_id: i64,
}

#[derive(Clone)]
pub struct MetadataStore {
    client: DynamoDbClient,
}

impl MetadataStore {
    pub async fn new(client: DynamoDbClient) -> Result<Self, MetadataError> {
        init_table_definitions();

        create_table(
            &client,
            RUNS_TABLE_NAME,
            RUNS_TABLE_ATTRIBUTES.wait().clone(),
            RUNS_TABLE_SCHEMA.wait().clone(),
        )
        .await?;

        create_table(
            &client,
            CHANGELOG_TABLE_NAME,
            CHANGELOG_TABLE_ATTRIBUTES.wait().clone(),
            CHANGELOG_TABLE_SCHEMA.wait().clone(),
        )
        .await?;

        // Initialize the changelog counter if it doesn't exist
        let store = Self { client };
        store.init_next_id().await?;

        Ok(store)
    }

    // Initialize the changelog counter if it doesn't exist
    async fn init_next_id(&self) -> Result<(), MetadataError> {
        // Initialize counter with a condition to only insert if it doesn't exist
        let next_id = NextId {
            pk: CHANGELOG_TABLE_PK.to_string(),
            sk: i64::MAX,
            next_seq_no: 1,
            next_changelog_id: 1,
        };

        let item = to_item(next_id)?;
        match self
            .client
            .put_item()
            .table_name(CHANGELOG_TABLE_NAME)
            .set_item(Some(item))
            .condition_expression("attribute_not_exists(pk)")
            .send()
            .await
        {
            Ok(_) => {
                tracing::info!("Initialized changelog counter");
                Ok(())
            },
            Err(SdkError::ServiceError(err))
                if err.err().is_conditional_check_failed_exception() =>
            {
                // Another process initialized the counter concurrently
                // or it already exists
                tracing::info!("Changelog counter already exists");
                Ok(())
            },
            Err(err) => Err(MetadataError::CounterInitError(err.to_string())),
        }
    }

    pub async fn get_next_id(&self) -> Result<NextId, MetadataError> {
        let next_id = self
            .client
            .get_item()
            .table_name(CHANGELOG_TABLE_NAME)
            .key("pk", AttributeValue::S(CHANGELOG_TABLE_PK.to_string()))
            .key("sk", AttributeValue::N(i64::MAX.to_string()))
            .send()
            .await?;
        let next_id = match next_id.item {
            Some(item) => from_item::<NextId>(item)?,
            None => {
                return Err(MetadataError::CounterInitError(
                    "Changelog counter not found".to_string(),
                ));
            },
        };
        Ok(next_id)
    }

    /// Fetches all existing changelog entries and returns them as a vector.
    pub async fn get_changelog_snapshot(
        &self,
    ) -> Result<(Vec<ChangelogEntry>, i64), MetadataError> {
        let mut entries = Vec::new();
        let mut last_seen_sk: i64 = 0;
        let mut query_builder = self
            .client
            .query()
            .table_name(CHANGELOG_TABLE_NAME)
            .key_condition_expression("pk = :pkVal AND sk BETWEEN :skVal AND :maxVal")
            .expression_attribute_values(
                ":pkVal",
                AttributeValue::S(CHANGELOG_TABLE_PK.to_string()),
            )
            .expression_attribute_values(
                ":skVal",
                AttributeValue::N((last_seen_sk + 1).to_string()),
            )
            .expression_attribute_values(":maxVal", AttributeValue::N((i64::MAX - 1).to_string()));

        loop {
            let response = query_builder
                .clone()
                .send()
                .await
                .map_err(MetadataError::QueryError)?;

            if let Some(items) = response.items {
                for item in items {
                    match from_item::<ChangeLogEntryWithID>(item) {
                        Ok(entry) => {
                            last_seen_sk = entry.sk;
                            entries.push(entry.entry);
                        },
                        Err(e) => {
                            return Err(MetadataError::SerdeError(e));
                        },
                    }
                }
            }

            if let Some(key) = response.last_evaluated_key {
                query_builder = query_builder.set_exclusive_start_key(Some(key));
            } else {
                break;
            }
        }

        Ok((entries, last_seen_sk))
    }

    /// Returns a snapshot of existing changelog entries and a stream of new entries.
    /// The stream continuously polls for new entries after reaching the end of the snapshot.
    pub async fn get_changelog(
        self,
    ) -> Result<
        (
            Vec<ChangelogEntry>,
            impl Stream<Item = Result<ChangelogEntry, MetadataError>>,
        ),
        MetadataError,
    > {
        let (snapshot, last_seen_sk) = self.get_changelog_snapshot().await?;

        let stream = stream! {
            let mut last_seen_sk = last_seen_sk;
            let poll_interval = Duration::from_secs(1);

            loop {
                let mut query_builder = self
                    .client
                    .query()
                    .table_name(CHANGELOG_TABLE_NAME)
                    .key_condition_expression("pk = :pkVal AND sk BETWEEN :skVal AND :maxVal")
                    .expression_attribute_values(":pkVal", AttributeValue::S(CHANGELOG_TABLE_PK.to_string()))
                    .expression_attribute_values(":skVal", AttributeValue::N((last_seen_sk + 1).to_string()))
                    .expression_attribute_values(":maxVal", AttributeValue::N((i64::MAX - 1).to_string()));

                let mut has_new_items = false;
                loop {
                    let response = match query_builder.clone().send().await {
                        Ok(resp) => resp,
                        Err(e) => {
                            yield Err(MetadataError::QueryError(e));
                            break;
                        }
                    };

                    if let Some(items) = response.items {
                        for item in items {
                            match from_item::<ChangeLogEntryWithID>(item) {
                                Ok(entry) => {
                                    has_new_items = true;
                                    last_seen_sk = entry.sk;
                                    yield Ok(entry.entry);
                                },
                                Err(e) => {
                                    yield Err(MetadataError::SerdeError(e));
                                }
                            }
                        }
                    }

                    if let Some(key) = response.last_evaluated_key {
                        query_builder = query_builder.set_exclusive_start_key(Some(key));
                    } else {
                        break;
                    }
                }

                // If no new items were found in the last query cycle, wait before polling again
                if !has_new_items {
                    tokio::time::sleep(poll_interval).await;
                }
            }
        };

        Ok((snapshot, stream))
    }

    pub async fn append_wal(
        &self,
        run_id: String,
        stats: crate::runs::Stats,
    ) -> Result<(), MetadataError> {
        loop {
            // Get the current counter value
            let next_id_item = self
                .client
                .get_item()
                .table_name(CHANGELOG_TABLE_NAME)
                .key("pk", AttributeValue::S(CHANGELOG_TABLE_PK.to_string()))
                .key("sk", AttributeValue::N(i64::MAX.to_string()))
                .send()
                .await?;

            let mut next_id = match next_id_item.item {
                Some(item) => from_item::<NextId>(item)?,
                None => {
                    return Err(MetadataError::CounterInitError(
                        "Changelog counter not found".to_string(),
                    ));
                },
            };

            let mut write_requests = Vec::new();
            let item_count = match stats {
                crate::runs::Stats::StatsV1(ref s) => s.item_count,
            };

            let seq_no = next_id.next_seq_no;
            let changelog_id = next_id.next_changelog_id;
            next_id.next_seq_no += item_count as i64;
            next_id.next_changelog_id += 1;

            let update_ids = TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(CHANGELOG_TABLE_NAME)
                        .set_item(Some(to_item(next_id)?))
                        .condition_expression("next_changelog_id = :c")
                        .expression_attribute_values(
                            ":c",
                            AttributeValue::N(changelog_id.to_string()),
                        )
                        .build()
                        .expect("Failed to build put request"),
                )
                .build();

            write_requests.push(update_ids);

            // Add changelog entry
            let changelog_entry_item = to_item(ChangeLogEntryWithID {
                pk: CHANGELOG_TABLE_PK.to_string(),
                sk: seq_no,
                entry: ChangelogEntry::V1(ChangelogEntryV1 {
                    runs_added: vec![run_id.clone()],
                    runs_removed: vec![],
                }),
            })?;
            let changelog_entry = TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(CHANGELOG_TABLE_NAME)
                        .set_item(Some(changelog_entry_item))
                        .build()
                        .expect("Failed to build put request"),
                )
                .build();

            write_requests.push(changelog_entry);

            // Add runs
            write_requests.push(
                TransactWriteItem::builder()
                    .put(
                        Put::builder()
                            .table_name(RUNS_TABLE_NAME)
                            .set_item(Some(to_item(RunMetadata {
                                id: run_id.clone(),
                                belongs_to: BelongsTo::WalSeqNo(seq_no),
                                stats: stats.clone(),
                            })?))
                            .condition_expression("attribute_not_exists(id)")
                            .build()
                            .expect("Failed to build put request"),
                    )
                    .build(),
            );

            // Try to commit the transaction
            match self
                .client
                .transact_write_items()
                .set_transact_items(Some(write_requests))
                .send()
                .await
            {
                Ok(_) => return Ok(()),
                Err(SdkError::ServiceError(err))
                    if err.err().is_transaction_canceled_exception() =>
                {
                    // If there's a conflict, retry
                    tracing::info!("Transaction conflict, retrying with new seq_no value");
                    continue;
                },
                Err(err) => return Err(MetadataError::TransactWriteError(err)),
            }
        }
    }

    pub async fn get_run_metadata_batch(
        &self,
        run_ids: impl IntoIterator<Item = String>,
    ) -> Result<HashMap<String, RunMetadata>, MetadataError> {
        // Prepare keys for BatchGetItem
        let keys: Vec<HashMap<String, AttributeValue>> = run_ids
            .into_iter()
            .map(|id| {
                let mut key = HashMap::new();
                key.insert("id".to_string(), AttributeValue::S(id));
                key
            })
            .collect();

        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        // Create request
        let request = self.client.batch_get_item().request_items(
            RUNS_TABLE_NAME,
            aws_sdk_dynamodb::types::KeysAndAttributes::builder()
                .set_keys(Some(keys))
                .build()
                .expect("Failed to build keys and attributes"),
        );

        // Execute request
        let result = request.send().await?;

        // Process results
        let mut metadata_map = HashMap::new();

        if let Some(mut responses) = result.responses {
            if let Some(items) = responses.remove(RUNS_TABLE_NAME) {
                for metadata in from_items::<RunMetadata>(items)? {
                    metadata_map.insert(metadata.id.clone(), metadata);
                }
            }
        }

        Ok(metadata_map)
    }
}

async fn create_table(
    client: &DynamoDbClient,
    table_name: &str,
    attribute_definitions: Vec<AttributeDefinition>,
    key_schema: Vec<KeySchemaElement>,
) -> Result<(), MetadataError> {
    match client
        .create_table()
        .table_name(table_name)
        .set_attribute_definitions(Some(attribute_definitions))
        .set_key_schema(Some(key_schema))
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(SdkError::ServiceError(err)) if err.err().is_resource_in_use_exception() => {
            tracing::info!("Table already exists: {}", table_name);
            Ok(())
        },
        Err(err) => Err(MetadataError::CreateTableError(err)),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{StreamExt, pin_mut};

    use super::*;
    use crate::runs::{Stats, StatsV1};

    #[tokio::test]
    async fn test_metadata_store_creation() {
        let (store, _container) = setup_test_db().await.expect("Failed to setup test DB");

        // Verify tables were created by trying to get a non-existent item
        let result = store
            .client
            .get_item()
            .table_name(RUNS_TABLE_NAME)
            .key("id", AttributeValue::S("non-existent".to_string()))
            .send()
            .await;

        assert!(result.is_ok(), "Table should exist and be accessible");
    }

    #[tokio::test]
    async fn test_append_wal() {
        let (store, _container) = setup_test_db().await.expect("Failed to setup test DB");

        let run_id = "test-run-1".to_string();
        let stats = Stats::StatsV1(StatsV1 {
            min_key: "min-key".to_string(),
            max_key: "max-key".to_string(),
            size_bytes: 100,
            item_count: 10,
        });

        // Append a WAL entry
        store
            .append_wal(run_id.clone(), stats.clone())
            .await
            .expect("Failed to append WAL");

        // Verify the run was added by getting its metadata
        let metadata = store
            .get_run_metadata_batch([run_id.clone()])
            .await
            .expect("Failed to get run metadata");

        assert_eq!(metadata.len(), 1, "Should have one run metadata entry");
        assert!(
            metadata.contains_key(&run_id),
            "Should contain the added run ID"
        );

        let run_metadata = metadata.get(&run_id).unwrap();
        match &run_metadata.stats {
            Stats::StatsV1(s) => {
                assert_eq!(s.item_count, 10, "Item count should match what was stored");
                assert_eq!(s.min_key, "min-key", "Min key should match what was stored");
                assert_eq!(s.max_key, "max-key", "Max key should match what was stored");
                assert_eq!(s.size_bytes, 100, "Size bytes should match what was stored");
            },
        }
    }

    #[tokio::test]
    async fn test_get_run_metadata_batch() {
        let (store, _container) = setup_test_db().await.expect("Failed to setup test DB");

        // Add multiple runs
        let run_ids = vec!["batch-test-1", "batch-test-2", "batch-test-3"];
        for (i, run_id) in run_ids.iter().enumerate() {
            let stats = Stats::StatsV1(StatsV1 {
                min_key: format!("min-key-{}", i),
                max_key: format!("max-key-{}", i),
                size_bytes: (i as u64 + 1) * 100,
                item_count: (i + 1) as u64,
            });

            store
                .append_wal(run_id.to_string(), stats)
                .await
                .expect("Failed to append WAL");
        }

        // Get batch with all run IDs
        let run_ids_strings: Vec<String> = run_ids.iter().map(|id| id.to_string()).collect();
        let metadata = store
            .get_run_metadata_batch(run_ids_strings)
            .await
            .expect("Failed to get batch metadata");

        assert_eq!(
            metadata.len(),
            run_ids.len(),
            "Should retrieve all requested run metadata"
        );

        // Verify each run's metadata
        for (i, run_id) in run_ids.iter().enumerate() {
            let run_metadata = metadata.get(*run_id).expect("Run metadata should exist");
            match &run_metadata.stats {
                Stats::StatsV1(s) => {
                    assert_eq!(
                        s.item_count,
                        (i + 1) as u64,
                        "Item count should match what was stored"
                    );
                    assert_eq!(
                        s.min_key,
                        format!("min-key-{}", i),
                        "Min key should match what was stored"
                    );
                    assert_eq!(
                        s.max_key,
                        format!("max-key-{}", i),
                        "Max key should match what was stored"
                    );
                    assert_eq!(
                        s.size_bytes,
                        (i as u64 + 1) * 100,
                        "Size bytes should match what was stored"
                    );
                },
            }
        }

        // Test with empty array
        let empty_metadata = store
            .get_run_metadata_batch(Vec::<String>::new())
            .await
            .expect("Failed to handle empty batch");
        assert_eq!(
            empty_metadata.len(),
            0,
            "Empty request should return empty result"
        );

        // Test with non-existent IDs
        let nonexistent = store
            .get_run_metadata_batch(vec!["nonexistent".to_string()])
            .await
            .expect("Failed to handle nonexistent ID");
        assert_eq!(
            nonexistent.len(),
            0,
            "Nonexistent ID should return empty result"
        );
    }

    #[tokio::test]
    async fn test_stream_changelog() {
        let (store, _container) = setup_test_db().await.expect("Failed to setup test DB");

        // Add some runs to generate changelog entries
        let run_ids = vec!["stream-test-1", "stream-test-2", "stream-test-3"];
        for (i, run_id) in run_ids.iter().enumerate() {
            let stats = Stats::StatsV1(StatsV1 {
                min_key: format!("min-key-{}", i),
                max_key: format!("max-key-{}", i),
                size_bytes: (i as u64 + 1) * 100,
                item_count: 1,
            });

            store
                .append_wal(run_id.to_string(), stats)
                .await
                .expect("Failed to append WAL");
        }

        // Create a stream of changelog entries
        let (snapshot, stream) = store
            .get_changelog()
            .await
            .expect("Failed to get changelog");
        pin_mut!(stream);

        // Collect entries with a timeout to avoid hanging
        let mut changelog_entries = snapshot;
        let mut entry_count = 0;

        // Use timeout to prevent test from hanging
        while let Ok(Some(entry_result)) =
            tokio::time::timeout(Duration::from_secs(2), stream.next()).await
        {
            let entry = entry_result.expect("Failed to get changelog entry");

            // Verify entry structure
            match &entry {
                ChangelogEntry::V1(v1) => {
                    assert!(
                        !v1.runs_added.is_empty(),
                        "Changelog entry should contain added runs"
                    );
                    assert!(v1.runs_removed.is_empty(), "No runs should be removed");

                    // Match one of our expected run IDs
                    assert!(
                        run_ids
                            .iter()
                            .any(|id| v1.runs_added.contains(&id.to_string())),
                        "Added run should be one of our test runs"
                    );
                },
            }

            changelog_entries.push(entry);
            entry_count += 1;

            // Break after we've collected all expected entries
            if entry_count >= run_ids.len() {
                break;
            }
        }

        assert_eq!(
            changelog_entries.len(),
            run_ids.len(),
            "Should receive all changelog entries"
        );
    }

    #[tokio::test]
    async fn test_concurrent_appends() {
        let (store, _container) = setup_test_db().await.expect("Failed to setup test DB");

        // Create multiple concurrent append operations
        let mut handles = Vec::new();
        for i in 0..5 {
            let store_clone = store.clone();
            let run_id = format!("concurrent-test-{}", i);
            let stats = Stats::StatsV1(StatsV1 {
                min_key: format!("min-key-{}", i),
                max_key: format!("max-key-{}", i),
                size_bytes: (i as u64 + 1) * 100,
                item_count: 1,
            });

            handles.push(tokio::spawn(async move {
                store_clone.append_wal(run_id, stats).await
            }));
        }

        // Wait for all operations to complete
        for handle in handles {
            let result = handle.await.expect("Task panicked");
            assert!(result.is_ok(), "Concurrent append should succeed");
        }

        // Verify we have all the runs
        let run_ids: Vec<String> = (0..5).map(|i| format!("concurrent-test-{}", i)).collect();
        let metadata = store
            .get_run_metadata_batch(run_ids)
            .await
            .expect("Failed to get metadata");

        assert_eq!(metadata.len(), 5, "All 5 concurrent runs should be stored");
    }

    #[tokio::test]
    async fn test_init_changelog_counter_idempotence() {
        let (store, _container) = setup_test_db().await.expect("Failed to setup test DB");

        // Call init_changelog_counter again explicitly
        let result = store.init_next_id().await;
        assert!(result.is_ok(), "Second initialization should succeed");

        // Create a new store with the same client to test initialization during new()
        let store2 = MetadataStore::new(store.client.clone()).await;
        assert!(store2.is_ok(), "Creating a second store should succeed");
    }
}
