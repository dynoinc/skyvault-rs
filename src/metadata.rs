use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use async_stream::stream;
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::create_table::CreateTableError;
use aws_sdk_dynamodb::operation::get_item::GetItemError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, Put, TransactWriteItem, Update,
};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use futures::Stream;
use thiserror::Error;

use crate::proto;

// Define table attributes and schema as global constants
const RUNS_TABLE_NAME: &str = "runs";
const CHANGELOG_TABLE_NAME: &str = "changelog";
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

    #[error("Counter initialization error: {0}")]
    CounterInitError(String),

    #[error("Parse error: {0}")]
    ParseError(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangelogEntry {
    pub sequence_number: i64,
    pub runs_added: Vec<String>,
    pub runs_removed: Vec<String>,
}

impl TryFrom<HashMap<String, AttributeValue>> for ChangelogEntry {
    type Error = MetadataError;

    fn try_from(item: HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let sequence_number = item
            .get("sk")
            .and_then(|av| av.as_n().ok())
            .ok_or_else(|| MetadataError::ParseError("Missing or invalid 'sk'".to_string()))?
            .parse::<i64>()
            .map_err(|e| MetadataError::ParseError(format!("Failed to parse 'sk': {}", e)))?;

        let runs_added = item
            .get("runs_added")
            .and_then(|av| av.as_l().ok())
            .map(|l| {
                l.iter()
                    .filter_map(|av| av.as_s().ok().cloned())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();

        let runs_removed = item
            .get("runs_removed")
            .and_then(|av| av.as_l().ok())
            .map(|l| {
                l.iter()
                    .filter_map(|av| av.as_s().ok().cloned())
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();

        Ok(ChangelogEntry {
            sequence_number,
            runs_added,
            runs_removed,
        })
    }
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
        store.init_changelog_counter().await?;

        Ok(store)
    }

    // Initialize the changelog counter if it doesn't exist
    async fn init_changelog_counter(&self) -> Result<(), MetadataError> {
        // Initialize counter with a condition to only insert if it doesn't exist
        match self
            .client
            .put_item()
            .table_name(CHANGELOG_TABLE_NAME)
            .item("pk", AttributeValue::S("placeholder".to_string()))
            .item("sk", AttributeValue::N(i64::MAX.to_string()))
            .item("next_changelog_num", AttributeValue::N("1".to_string()))
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

    /// Returns a stream of changelog entries, starting from the beginning.
    /// The stream continuously polls for new entries after reaching the end.
    pub fn stream_changelog(
        &self,
    ) -> impl Stream<Item = Result<ChangelogEntry, MetadataError>> + '_ {
        stream! {
            let mut last_seen_sk: i64 = 0;
            let poll_interval = Duration::from_secs(1);

            loop {
                let mut query_builder = self
                    .client
                    .query()
                    .table_name(CHANGELOG_TABLE_NAME)
                    .key_condition_expression("pk = :pkVal AND sk > :skVal")
                    .expression_attribute_values(":pkVal", AttributeValue::S("changelog".to_string()))
                    .expression_attribute_values(":skVal", AttributeValue::N(last_seen_sk.to_string()));

                let mut has_new_items = false;
                loop {
                    let response = match query_builder.clone().send().await {
                        Ok(resp) => resp,
                        Err(e) => {
                            yield Err(MetadataError::QueryError(e));
                            // Consider adding a backoff strategy here instead of breaking immediately
                            break;
                        }
                    };

                    if let Some(items) = response.items {
                        if items.is_empty() {
                            break; // No more items in this query page
                        }
                        has_new_items = true;
                        for item in items {
                           match ChangelogEntry::try_from(item) {
                                Ok(entry) => {
                                    last_seen_sk = entry.sequence_number;
                                    yield Ok(entry);
                                },
                                Err(e) => {
                                    yield Err(e);
                                    // Decide if we should stop the stream on parse error
                                    // For now, we continue to the next item
                                }
                            }
                        }
                    }

                    // Check if there are more pages
                    if let Some(last_evaluated_key) = response.last_evaluated_key {
                        query_builder = query_builder.set_exclusive_start_key(Some(last_evaluated_key));
                    } else {
                        break; // No more pages
                    }
                }

                // If no new items were found in the last query cycle, wait before polling again
                if !has_new_items {
                    tokio::time::sleep(poll_interval).await;
                }
                // If there was an error in the inner loop, we might want to add a delay here too
                // before retrying the outer loop.
            }
        }
    }

    pub async fn append_changelog(
        &self,
        runs_added: Vec<proto::RunMetadata>,
        runs_removed: Vec<String>,
    ) -> Result<(), MetadataError> {
        loop {
            // Get the current counter value
            let counter_result = self
                .client
                .get_item()
                .table_name(CHANGELOG_TABLE_NAME)
                .key("pk", AttributeValue::S("placeholder".to_string()))
                .key("sk", AttributeValue::N(i64::MAX.to_string()))
                .send()
                .await?;

            let counter_value = counter_result
                .item()
                .and_then(|item| item.get("next_changelog_num"))
                .and_then(|av| av.as_n().ok())
                .ok_or_else(|| {
                    MetadataError::CounterInitError("Failed to get counter value".to_string())
                })?
                .parse::<i64>()
                .map_err(|e| {
                    MetadataError::CounterInitError(format!("Failed to parse counter value: {}", e))
                })?;

            let mut write_requests = Vec::new();

            let update_counter = TransactWriteItem::builder()
                .update(
                    Update::builder()
                        .table_name(CHANGELOG_TABLE_NAME)
                        .key("pk", AttributeValue::S("placeholder".to_string()))
                        .key("sk", AttributeValue::N(i64::MAX.to_string()))
                        .update_expression("SET next_changelog_num = next_changelog_num + :incr")
                        .expression_attribute_values(":incr", AttributeValue::N("1".to_string()))
                        .condition_expression("next_changelog_num = :current_val")
                        .expression_attribute_values(
                            ":current_val",
                            AttributeValue::N(counter_value.to_string()),
                        )
                        .build()
                        .expect("Failed to build update request"),
                )
                .build();

            write_requests.push(update_counter);

            // Add changelog entry
            let changelog_entry = TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(CHANGELOG_TABLE_NAME)
                        .item("pk", AttributeValue::S("changelog".to_string()))
                        .item("sk", AttributeValue::N(counter_value.to_string()))
                        .item(
                            "runs_added",
                            AttributeValue::L(
                                runs_added
                                    .iter()
                                    .map(|run| AttributeValue::S(run.id.clone()))
                                    .collect(),
                            ),
                        )
                        .item(
                            "runs_removed",
                            AttributeValue::L(
                                runs_removed
                                    .iter()
                                    .map(|id| AttributeValue::S(id.clone()))
                                    .collect(),
                            ),
                        )
                        .build()
                        .expect("Failed to build put request"),
                )
                .build();

            write_requests.push(changelog_entry);

            // Add runs
            for run in &runs_added {
                write_requests.push(
                    TransactWriteItem::builder()
                        .put(
                            Put::builder()
                                .table_name(RUNS_TABLE_NAME)
                                .item("id", AttributeValue::S(run.id.clone()))
                                .item("stats", match &run.stats {
                                    Some(proto::run_metadata::Stats::StatsV1(stats)) => {
                                        let mut map = HashMap::new();
                                        map.insert(
                                            "min_key".to_string(),
                                            AttributeValue::S(stats.min_key.clone()),
                                        );
                                        map.insert(
                                            "max_key".to_string(),
                                            AttributeValue::S(stats.max_key.clone()),
                                        );
                                        map.insert(
                                            "size_bytes".to_string(),
                                            AttributeValue::N(stats.size_bytes.to_string()),
                                        );
                                        AttributeValue::M(map)
                                    },
                                    None => AttributeValue::Null(true),
                                })
                                .build()
                                .expect("Failed to build put request"),
                        )
                        .build(),
                );
            }

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
                    tracing::info!("Transaction conflict, retrying with new counter value");
                    continue;
                },
                Err(err) => return Err(MetadataError::TransactWriteError(err)),
            }
        }
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
    use aws_sdk_dynamodb::config::{Credentials, Region};
    use futures::StreamExt;
    use testcontainers_modules::dynamodb_local::DynamoDb;
    use testcontainers_modules::testcontainers::ContainerAsync;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use tokio::time::timeout;

    use super::*;

    async fn setup_test_db() -> (MetadataStore, ContainerAsync<DynamoDb>) {
        let container = DynamoDb::default()
            .start()
            .await
            .expect("Failed to start DynamoDB local container");
        let port = container
            .get_host_port_ipv4(8000)
            .await
            .expect("Failed to get port");
        let endpoint_url = format!("http://localhost:{}", port);

        let config = aws_sdk_dynamodb::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .endpoint_url(endpoint_url)
            .credentials_provider(Credentials::new("dummy", "dummy", None, None, "dummy"))
            .build();

        let client = DynamoDbClient::from_conf(config);

        let metadata_store = MetadataStore::new(client)
            .await
            .expect("Failed to create metadata store");

        (metadata_store, container)
    }

    #[tokio::test]
    async fn test_append_changelog() {
        let (metadata_store, _container) = setup_test_db().await;

        // Create test run metadata
        let run1 = proto::RunMetadata {
            id: "run1".to_string(),
            stats: Some(proto::run_metadata::Stats::StatsV1(proto::StatsV1 {
                min_key: "a".to_string(),
                max_key: "z".to_string(),
                size_bytes: 100,
            })),
        };

        let run2 = proto::RunMetadata {
            id: "run2".to_string(),
            stats: Some(proto::run_metadata::Stats::StatsV1(proto::StatsV1 {
                min_key: "b".to_string(),
                max_key: "y".to_string(),
                size_bytes: 200,
            })),
        };

        // Test appending to changelog
        let result = metadata_store
            .append_changelog(vec![run1.clone()], vec![])
            .await;
        assert!(
            result.is_ok(),
            "Failed to append to changelog: {:?}",
            result
        );

        // Test appending multiple runs
        let result = metadata_store
            .append_changelog(vec![run2.clone()], vec![])
            .await;
        assert!(
            result.is_ok(),
            "Failed to append multiple runs: {:?}",
            result
        );

        // Test appending with tombstones
        let result = metadata_store
            .append_changelog(vec![], vec!["run1".to_string()])
            .await;
        assert!(
            result.is_ok(),
            "Failed to append with tombstones: {:?}",
            result
        );

        // Test concurrent appends
        let metadata_store_clone = metadata_store.clone();
        let run3 = proto::RunMetadata {
            id: "run3".to_string(),
            stats: Some(proto::run_metadata::Stats::StatsV1(proto::StatsV1 {
                min_key: "c".to_string(),
                max_key: "x".to_string(),
                size_bytes: 300,
            })),
        };

        let run4 = proto::RunMetadata {
            id: "run4".to_string(),
            stats: Some(proto::run_metadata::Stats::StatsV1(proto::StatsV1 {
                min_key: "d".to_string(),
                max_key: "w".to_string(),
                size_bytes: 400,
            })),
        };

        let handle1 =
            tokio::spawn(async move { metadata_store.append_changelog(vec![run3], vec![]).await });

        let handle2 = tokio::spawn(async move {
            metadata_store_clone
                .append_changelog(vec![run4], vec![])
                .await
        });

        let result1 = handle1.await.expect("Task panicked");
        let result2 = handle2.await.expect("Task panicked");

        assert!(
            result1.is_ok() || result2.is_ok(),
            "Both concurrent appends failed"
        );
    }

    #[tokio::test]
    async fn test_stream_changelog() {
        let (metadata_store, _container) = setup_test_db().await;

        // Add initial entries
        let run1 = proto::RunMetadata {
            id: "run1".to_string(),
            stats: None,
        };
        let run2 = proto::RunMetadata {
            id: "run2".to_string(),
            stats: None,
        };
        metadata_store
            .append_changelog(vec![run1.clone()], vec![])
            .await
            .unwrap();
        metadata_store
            .append_changelog(vec![run2.clone()], vec!["run1".to_string()])
            .await
            .unwrap();

        // Pin the stream
        let mut stream = Box::pin(metadata_store.stream_changelog());

        // Check initial entries
        let entry1 = timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("Timeout waiting for entry 1")
            .unwrap()
            .unwrap();
        assert_eq!(entry1.sequence_number, 1);
        assert_eq!(entry1.runs_added, vec!["run1"]);
        assert_eq!(entry1.runs_removed, Vec::<String>::new());

        let entry2 = timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("Timeout waiting for entry 2")
            .unwrap()
            .unwrap();
        assert_eq!(entry2.sequence_number, 2);
        assert_eq!(entry2.runs_added, vec!["run2"]);
        assert_eq!(entry2.runs_removed, vec!["run1"]);

        // Add more entries while streaming
        let run3 = proto::RunMetadata {
            id: "run3".to_string(),
            stats: None,
        };
        metadata_store
            .append_changelog(vec![run3.clone()], vec![])
            .await
            .unwrap();

        // Check the newly added entry (polling might take a second)
        let entry3 = timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("Timeout waiting for entry 3")
            .unwrap()
            .unwrap();
        assert_eq!(entry3.sequence_number, 3);
        assert_eq!(entry3.runs_added, vec!["run3"]);
        assert_eq!(entry3.runs_removed, Vec::<String>::new());

        // Add another entry
        let run4 = proto::RunMetadata {
            id: "run4".to_string(),
            stats: None,
        };
        metadata_store
            .append_changelog(vec![run4.clone()], vec!["run2".to_string()])
            .await
            .unwrap();

        // Check the next entry
        let entry4 = timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("Timeout waiting for entry 4")
            .unwrap()
            .unwrap();
        assert_eq!(entry4.sequence_number, 4);
        assert_eq!(entry4.runs_added, vec!["run4"]);
        assert_eq!(entry4.runs_removed, vec!["run2"]);
    }
}
