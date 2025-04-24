use std::sync::OnceLock;

use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::create_table::CreateTableError;
use aws_sdk_dynamodb::operation::get_item::GetItemError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, Put, TransactWriteItem, Update,
};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
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

    #[error("Counter initialization error: {0}")]
    CounterInitError(String),
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
        // Check if counter exists
        let result = self
            .client
            .get_item()
            .table_name(CHANGELOG_TABLE_NAME)
            .key("pk", AttributeValue::S("placeholder".to_string()))
            .key("sk", AttributeValue::N(i64::MAX.to_string()))
            .send()
            .await?;

        // If counter doesn't exist, initialize it
        if result.item().is_none() {
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
                    tracing::info!("Changelog counter already initialized by another process");
                    Ok(())
                },
                Err(err) => Err(MetadataError::CounterInitError(err.to_string())),
            }
        } else {
            tracing::info!("Changelog counter already exists");
            Ok(())
        }
    }
}

impl MetadataStore {
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
                                        let mut map = std::collections::HashMap::new();
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
        .on_demand_throughput(aws_sdk_dynamodb::types::OnDemandThroughput::builder().build())
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
