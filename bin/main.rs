use std::net::SocketAddr;

use anyhow::{Context, Result};
use aws_sdk_dynamodb::Client as DynamoDbClient;
use aws_sdk_s3::Client as S3Client;
use skyvault::{metadata, storage};
use structopt::StructOpt;
use tracing::info;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Debug, StructOpt)]
#[structopt(name = "skyvault", about = "A gRPC server for skyvault.")]
pub struct Config {
    #[structopt(long, env = "SKYVAULT_GRPC_ADDR", default_value = "0.0.0.0:50051")]
    pub grpc_addr: String,

    #[structopt(
        long,
        env = "SKYVAULT_METADATA_DB_PATH",
        default_value = "./metadata.db"
    )]
    pub metadata_db_path: String,

    #[structopt(long, env = "SKYVAULT_STORAGE_DIR", default_value = "./storage")]
    pub storage_dir: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup tracing subscriber
    let subscriber = FmtSubscriber::builder()
        // Use EnvFilter to allow RUST_LOG environment variable to control level
        .with_env_filter(EnvFilter::from_default_env().add_directive("skyvault=info".parse()?))
        // Use pretty formatting for console output
        .pretty()
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Setting default tracing subscriber failed");

    let config = Config::from_args();
    info!(config = ?config, "Starting skyvault");

    let addr: SocketAddr = config
        .grpc_addr
        .parse()
        .with_context(|| format!("Failed to parse gRPC address: {}", config.grpc_addr))?;

    // Initialize AWS SDK
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

    // Create DynamoDB client
    let dynamodb_client = DynamoDbClient::new(&aws_config);
    let metadata = metadata::MetadataStore::new(dynamodb_client);

    // Create S3 client
    let s3_client = S3Client::new(&aws_config);
    let storage = storage::ObjectStore::new(s3_client);

    info!(address = %addr, "Starting gRPC server");
    skyvault::server(addr, metadata, storage)
        .await
        .with_context(|| "Failed to start gRPC server")?;
    info!("gRPC server stopped");
    Ok(())
}
