use aws_sdk_s3::config::{
    Credentials,
    SharedCredentialsProvider,
};
use clap::Parser;
use kube::Client;

use crate::k8s;

#[derive(Debug, Parser, Clone)]
pub struct SentryConfig {
    #[arg(long, env = "SENTRY_DSN", default_value = "")]
    pub dsn: String,

    #[arg(long, env = "SENTRY_SAMPLE_RATE", default_value = "0.0")]
    pub sample_rate: f32,
}

#[derive(Debug, Parser, Clone)]
pub struct OtelConfig {
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT", default_value = "")]
    pub endpoint: String,

    #[arg(long, env = "OTEL_EXPORTER_OTLP_PROTOCOL", default_value = "http")]
    pub protocol: String,
}

#[derive(Debug, Parser, Clone)]
pub struct PostgresConfig {
    #[arg(long, env = "SKYVAULT_POSTGRES_USER", default_value = "postgres")]
    pub user: String,
    #[arg(long, env = "SKYVAULT_POSTGRES_HOST", default_value = "localhost")]
    pub host: String,
    #[arg(long, env = "SKYVAULT_POSTGRES_PORT", default_value_t = 5432)]
    pub port: u16,
    #[arg(long, env = "SKYVAULT_POSTGRES_DB", default_value = "skyvault")]
    pub db_name: String,
    #[arg(long, env = "SKYVAULT_POSTGRES_SSLMODE", default_value = "prefer")]
    pub sslmode: String,
}

impl PostgresConfig {
    pub async fn to_url(&self, k8s_client: Client, namespace: &str) -> Result<String, anyhow::Error> {
        const PASSWORD_SECRET_NAME: &str = "skyvault-postgres-password";
        const PASSWORD_KEY: &str = "POSTGRES_PASSWORD";

        let password = k8s::read_secret(k8s_client, namespace, PASSWORD_SECRET_NAME, PASSWORD_KEY).await?;
        Ok(format!(
            "postgres://{}:{}@{}:{}/{}?sslmode={}",
            self.user, password, self.host, self.port, self.db_name, self.sslmode
        ))
    }
}

#[derive(Debug, Parser, Clone)]
pub struct S3Config {
    #[arg(long, env = "SKYVAULT_S3_BUCKET", default_value = "skyvault-bucket")]
    pub bucket_name: String,
}

impl S3Config {
    pub async fn to_config(
        &self,
        k8s_client: Client,
        namespace: &str,
    ) -> Result<aws_sdk_s3::config::Config, anyhow::Error> {
        const AWS_SECRET_NAME: &str = "skyvault-aws-credentials";
        const AWS_ACCESS_KEY_ID_KEY: &str = "AWS_ACCESS_KEY_ID";
        const AWS_SECRET_ACCESS_KEY_KEY: &str = "AWS_SECRET_ACCESS_KEY";

        let access_key =
            k8s::read_secret(k8s_client.clone(), namespace, AWS_SECRET_NAME, AWS_ACCESS_KEY_ID_KEY).await?;
        let secret_key = k8s::read_secret(k8s_client, namespace, AWS_SECRET_NAME, AWS_SECRET_ACCESS_KEY_KEY).await?;
        let credentials = Credentials::new(access_key, secret_key, None, None, "k8s");

        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .load()
            .await;

        let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
            .force_path_style(true)
            .build();

        Ok(s3_config)
    }
}
