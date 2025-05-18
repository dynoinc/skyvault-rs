use anyhow::{Context, Result as AnyhowResult, anyhow};
use http::Request;
use k8s_openapi::api::core::v1::Secret;
use kube::{Api, Client};
use tokio::fs;

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

pub async fn read_secret(
    client: Client,
    namespace: &str,
    secret_name: &str,
    key: &str,
) -> AnyhowResult<String> {
    let api: Api<Secret> = Api::namespaced(client, namespace);
    match api.get(secret_name).await {
        Ok(secret) => {
            let data = secret.data.ok_or_else(|| {
                anyhow!(
                    "Secret '{}' in namespace '{}' does not contain any data",
                    secret_name,
                    namespace
                )
            })?;

            let value_bytes = data.get(key).ok_or_else(|| {
                anyhow!(
                    "Secret '{}' in namespace '{}' does not contain key '{}'",
                    secret_name,
                    namespace,
                    key
                )
            })?;

            String::from_utf8(value_bytes.0.clone()).with_context(|| {
                format!("Failed to decode key '{key}' from secret '{secret_name}'")
            })
        },
        Err(e) => Err(anyhow!(
            "Failed to get secret '{}' in namespace '{}': {}",
            secret_name,
            namespace,
            e
        )),
    }
}
