use anyhow::{
    Context,
    Result,
};
use clap::Parser;
use skyvault::{
    config::{
        PostgresConfig,
        SentryConfig,
    },
    k8s,
    observability,
};
use tracing::{
    error,
    info,
};

#[derive(Debug, Parser, Clone)]
#[command(name = "reset-cluster", about = "Reset Skyvault database by truncating all tables.")]
struct Config {
    #[clap(flatten)]
    postgres: PostgresConfig,

    #[clap(flatten)]
    sentry: SentryConfig,

    #[arg(long, env = "RESET_DRY_RUN")]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize crypto provider (required for TLS)
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install aws-lc-rs CryptoProvider");

    let config = Config::parse();
    let _sentry = observability::init_tracing_and_sentry(config.sentry.clone());

    info!("Starting database reset");

    if config.dry_run {
        info!("DRY RUN MODE - No actual changes will be made");
    }

    // Initialize K8s client for database connection
    let current_namespace = k8s::get_namespace()
        .await
        .context("Failed to get current Kubernetes namespace")?;
    let k8s_client = k8s::create_k8s_client()
        .await
        .context("Failed to create Kubernetes client")?;

    info!("Working in namespace: {}", current_namespace);

    let reset_result = clear_database_tables(k8s_client, &current_namespace, &config.postgres, config.dry_run).await;

    match reset_result {
        Ok(()) => {
            info!("Database reset completed successfully");
            Ok(())
        },
        Err(e) => {
            error!("Database reset failed: {}", e);
            Err(e)
        },
    }
}

async fn clear_database_tables(
    k8s_client: kube::Client,
    namespace: &str,
    postgres_config: &PostgresConfig,
    dry_run: bool,
) -> Result<()> {
    use sqlx::postgres::PgPoolOptions;

    if dry_run {
        info!("DRY RUN: Would clear database tables");
        return Ok(());
    }

    // Connect directly to the database to execute raw SQL
    let metadata_url = postgres_config.to_url(k8s_client, namespace).await?;
    let pg_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&metadata_url)
        .await
        .context("Failed to connect to database")?;

    info!("Truncating database tables...");

    // Execute the truncation queries (same as in migration 0002_reset.sql)
    let truncate_queries = [
        "TRUNCATE TABLE tables",
        "TRUNCATE TABLE runs",
        "TRUNCATE TABLE changelog",
        "TRUNCATE TABLE snapshots",
        "TRUNCATE TABLE jobs",
    ];

    for query in &truncate_queries {
        info!("Executing: {}", query);
        sqlx::query(query)
            .execute(&pg_pool)
            .await
            .with_context(|| format!("Failed to execute: {query}"))?;
    }

    info!("Database tables cleared successfully");
    Ok(())
}
