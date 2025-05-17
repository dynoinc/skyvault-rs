use anyhow::{Context, Result as AnyhowResult, anyhow};
use aws_sdk_s3::config::Credentials;
use http::Request;
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};
use tokio::fs;

pub const AWS_SECRET_NAME: &str = "skyvault-aws-credentials";
pub const AWS_ACCESS_KEY_ID_KEY: &str = "AWS_ACCESS_KEY_ID";
pub const AWS_SECRET_ACCESS_KEY_KEY: &str = "AWS_SECRET_ACCESS_KEY";

pub async fn get_namespace() -> std::result::Result<String, std::io::Error> {
    let namespace_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
    let current_namespace = fs::read_to_string(namespace_path).await?.trim().to_string();
    Ok(current_namespace)
}

pub async fn create_k8s_client() -> std::result::Result<Client, kube::Error> {
    let client = Client::try_default().await?;
    let req = Request::builder()
        .uri("/livez")
        .body(Vec::new())
        .map_err(kube::Error::HttpError)?;
    client.request_text(req).await?;
    Ok(client)
}

pub async fn get_aws_credentials(client: Client, namespace: &str) -> AnyhowResult<Credentials> {
    let api: Api<Secret> = Api::namespaced(client, namespace);
    match api.get(AWS_SECRET_NAME).await {
        Ok(secret) => {
            let data = secret.data.ok_or_else(|| {
                anyhow!(
                    "Secret '{}' in namespace '{}' does not contain any data",
                    AWS_SECRET_NAME,
                    namespace
                )
            })?;

            let access_bytes = data.get(AWS_ACCESS_KEY_ID_KEY).ok_or_else(|| {
                anyhow!(
                    "Secret '{}' in namespace '{}' does not contain key '{}'",
                    AWS_SECRET_NAME,
                    namespace,
                    AWS_ACCESS_KEY_ID_KEY
                )
            })?;
            let secret_bytes = data.get(AWS_SECRET_ACCESS_KEY_KEY).ok_or_else(|| {
                anyhow!(
                    "Secret '{}' in namespace '{}' does not contain key '{}'",
                    AWS_SECRET_NAME,
                    namespace,
                    AWS_SECRET_ACCESS_KEY_KEY
                )
            })?;

            let access_key = String::from_utf8(access_bytes.0.clone()).context(format!(
                "Failed to decode key '{AWS_ACCESS_KEY_ID_KEY}' from secret '{AWS_SECRET_NAME}'"
            ))?;
            let secret_key = String::from_utf8(secret_bytes.0.clone()).context(format!(
                "Failed to decode key '{AWS_SECRET_ACCESS_KEY_KEY}' from secret \
                 '{AWS_SECRET_NAME}'"
            ))?;

            Ok(Credentials::new(access_key, secret_key, None, None, "k8s"))
        },
        Err(e) => Err(anyhow!(
            "Failed to get secret '{}' in namespace '{}': {}",
            AWS_SECRET_NAME,
            namespace,
            e
        )),
    }
}
