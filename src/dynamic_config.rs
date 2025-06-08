// src/dynamic_config.rs
use std::{
    collections::BTreeMap,
    env,
    sync::Arc,
    time::Duration,
};

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::{
    ConfigMap,
    Pod,
};
use kube::{
    Api,
    Client,
    ResourceExt,
    runtime::watcher::{
        self,
        Event,
        watcher,
    },
};
use thiserror::Error;
use tokio::sync::{
    RwLock,
    Semaphore,
};
use tracing::{
    error,
    info,
    warn,
};

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),
    #[error("Serde JSON error: {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("ConfigMap '{0}' not found in namespace '{1}'")]
    ConfigMapNotFound(String, String),
    #[error("ConfigMap data field is missing or empty")]
    MissingOrEmptyData,
    #[error("Failed to acquire write lock for config update: {0}")]
    LockError(String),
    #[error("Failed to determine instance name: {0}")]
    InstanceNameError(String),
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    // Writer
    pub writer_uploads_semaphore: Arc<Semaphore>,

    // Orchestrator
    pub orchestrator_job_retry_limit: i32,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self::from(ParsedConfigMap::default())
    }
}

#[derive(Debug, Clone)]
struct ParsedConfigMap {
    // Writer
    pub writer_concurrent_uploads: u32,

    // Orchestrator
    pub orchestrator_job_retry_limit: i32,
}

impl Default for ParsedConfigMap {
    fn default() -> Self {
        Self {
            // Writer
            writer_concurrent_uploads: 4,

            // Orchestrator
            orchestrator_job_retry_limit: 3,
        }
    }
}

impl ParsedConfigMap {
    fn from_k8s_data(data: &BTreeMap<String, String>) -> Result<Self, ConfigError> {
        let mut config = Self::default();

        if let Some(uploads_str) = data.get("writer_concurrent_uploads") {
            if let Ok(uploads) = uploads_str.parse::<u32>() {
                config.writer_concurrent_uploads = uploads;
            }
        }

        if let Some(retry_str) = data.get("orchestrator_job_retry_limit") {
            if let Ok(retries) = retry_str.parse::<i32>() {
                config.orchestrator_job_retry_limit = retries;
            }
        }

        Ok(config)
    }
}

impl From<ParsedConfigMap> for AppConfig {
    fn from(parsed_config: ParsedConfigMap) -> Self {
        AppConfig {
            writer_uploads_semaphore: Arc::new(Semaphore::new(parsed_config.writer_concurrent_uploads as usize)),
            orchestrator_job_retry_limit: parsed_config.orchestrator_job_retry_limit,
        }
    }
}

impl AppConfig {
    fn apply(&mut self, new_config: ParsedConfigMap) {
        let current_permits = self.writer_uploads_semaphore.available_permits();
        let new_permits = new_config.writer_concurrent_uploads as usize;

        if new_permits > current_permits {
            // Add permits if new limit is higher
            self.writer_uploads_semaphore.add_permits(new_permits - current_permits);
        } else if new_permits < current_permits {
            // Remove permits if new limit is lower by acquiring them
            for _ in 0..(current_permits - new_permits) {
                // Try to acquire permits, but don't wait if they're not available
                if let Ok(permit) = self.writer_uploads_semaphore.try_acquire() {
                    // Immediately drop the permit to effectively remove it
                    drop(permit);
                }
            }
        }

        // Orchestrator
        self.orchestrator_job_retry_limit = new_config.orchestrator_job_retry_limit;
    }
}

/// Type alias for the shared, dynamically updatable application configuration.
pub type SharedAppConfig = Arc<RwLock<AppConfig>>;

async fn determine_instance_name(client: Client, namespace: &str) -> Result<String, ConfigError> {
    let hostname = env::var("HOSTNAME")
        .map_err(|_| ConfigError::InstanceNameError("HOSTNAME environment variable not set".to_string()))?;

    let pods_api: Api<Pod> = Api::namespaced(client, namespace);
    let pod = pods_api
        .get(&hostname)
        .await
        .map_err(|e| ConfigError::InstanceNameError(format!("Failed to get pod {hostname}: {e}")))?;

    let labels = pod.metadata.labels.unwrap_or_default();
    let component_label = labels
        .get("app.kubernetes.io/component")
        .ok_or_else(|| ConfigError::InstanceNameError("Pod missing app.kubernetes.io/component label".to_string()))?;

    // Extract instance name from component label (e.g., "skyvault-writer" ->
    // "writer")
    let instance_name = component_label.strip_prefix("skyvault-").ok_or_else(|| {
        ConfigError::InstanceNameError(format!(
            "Component label '{component_label}' does not start with 'skyvault-'"
        ))
    })?;

    Ok(instance_name.to_string())
}

async fn load_initial_config(client: Client, namespace: &str, instance_name: &str) -> Result<AppConfig, ConfigError> {
    let api: Api<ConfigMap> = Api::namespaced(client, namespace);
    let configmap_name = format!("skyvault-dynamic-config-{instance_name}");
    info!("Attempting to load initial configuration from ConfigMap '{namespace}/{configmap_name}'");

    match api.get(&configmap_name).await {
        Ok(cm) => {
            if let Some(data_map) = &cm.data {
                if data_map.is_empty() {
                    warn!("ConfigMap '{namespace}/{configmap_name}' 'data' field is empty. Using defaults.");
                    Ok(AppConfig::from(ParsedConfigMap::default()))
                } else {
                    let deserializable_config = ParsedConfigMap::from_k8s_data(data_map)?;
                    Ok(AppConfig::from(deserializable_config))
                }
            } else {
                warn!("ConfigMap '{namespace}/{configmap_name}' has no 'data' field. Using defaults.");
                Ok(AppConfig::from(ParsedConfigMap::default()))
            }
        },
        Err(kube::Error::Api(ae)) if ae.code == 404 => {
            warn!("ConfigMap '{namespace}/{configmap_name}' not found. Using default configuration.");
            Ok(AppConfig::from(ParsedConfigMap::default()))
        },
        Err(e) => Err(ConfigError::KubeError(e)),
    }
}

async fn watch_config_changes_task(
    client: Client,
    namespace: String,
    instance_name: String,
    shared_config: SharedAppConfig,
) {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), &namespace);
    let watcher_params = watcher::Config::default();
    let configmap_name = format!("skyvault-dynamic-config-{instance_name}");

    info!("Starting ConfigMap watcher for '{namespace}/{configmap_name}'");

    loop {
        let mut watch_stream = watcher(api.clone(), watcher_params.clone()).boxed();
        info!("Watcher stream (re)established for ConfigMap '{namespace}/{configmap_name}'");

        while let Some(event_result) = watch_stream.next().await {
            match event_result {
                Ok(event) => match event {
                    Event::Apply(cm) => {
                        if cm.name_any() == configmap_name {
                            info!("ConfigMap '{namespace}/{configmap_name}' added/modified. Processing update.");
                            if let Some(data_map) = &cm.data {
                                let new_config = ParsedConfigMap::from_k8s_data(data_map).unwrap_or_default();
                                let mut config_guard = shared_config.write().await;
                                config_guard.apply(new_config.clone());
                                info!("Successfully reloaded configuration: {new_config:?}");
                            } else {
                                warn!(
                                    "ConfigMap '{namespace}/{configmap_name}' event received but 'data' field was \
                                     empty."
                                );
                            }
                        }
                    },
                    Event::Delete(cm) => {
                        if cm.name_any() == configmap_name {
                            warn!(
                                "ConfigMap '{namespace}/{configmap_name}' was deleted. Reverting to default \
                                 configuration."
                            );
                            let mut config_guard = shared_config.write().await;
                            config_guard.apply(ParsedConfigMap::default());
                        }
                    },
                    _ => {
                        info!("Received other event type from ConfigMap watcher");
                    },
                },
                Err(e) => {
                    error!("Error watching ConfigMap: {:?}. Attempting to restart watcher.", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    break;
                },
            }
        }

        error!("ConfigMap watch stream closed. Restarting after delay.");
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

pub async fn initialize_dynamic_config(client: Client, namespace: &str) -> Result<SharedAppConfig, ConfigError> {
    let instance_name = determine_instance_name(client.clone(), namespace).await?;
    info!("Determined instance name: {instance_name}");

    let initial_config = load_initial_config(client.clone(), namespace, &instance_name).await?;

    info!("Initial dynamic configuration loaded: {initial_config:?}");
    let shared_config = Arc::new(RwLock::new(initial_config));

    let watcher_client = client.clone();
    let watcher_shared_config = shared_config.clone();
    tokio::spawn(watch_config_changes_task(
        watcher_client,
        namespace.to_string(),
        instance_name,
        watcher_shared_config,
    ));

    Ok(shared_config)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn from_k8s_data(data: &BTreeMap<String, String>) -> Result<AppConfig, ConfigError> {
        let deserializable_config = ParsedConfigMap::from_k8s_data(data)?;
        Ok(AppConfig::from(deserializable_config))
    }

    #[test]
    fn test_app_config_from_k8s_data_empty() {
        let data = BTreeMap::new();
        let config = from_k8s_data(&data).unwrap();
        assert_eq!(config.writer_uploads_semaphore.available_permits(), 4);
    }

    #[test]
    fn test_app_config_from_k8s_data_valid() {
        let mut data = BTreeMap::new();
        data.insert("writer_concurrent_uploads".to_string(), "10".to_string());
        let config = from_k8s_data(&data).unwrap();
        assert_eq!(config.writer_uploads_semaphore.available_permits(), 10);
    }

    #[test]
    fn test_app_config_from_k8s_data_invalid_value() {
        let mut data = BTreeMap::new();
        data.insert("writer_concurrent_uploads".to_string(), "not_a_number".to_string());
        let config = from_k8s_data(&data).unwrap();
        // Should fall back to default because "not_a_number" is not parsable to u32
        assert_eq!(config.writer_uploads_semaphore.available_permits(), 4);
    }

    #[test]
    fn test_app_config_from_k8s_data_missing_key() {
        let mut data = BTreeMap::new();
        data.insert("other_key".to_string(), "some_value".to_string());
        let config = from_k8s_data(&data).unwrap();
        // concurrent_uploads key is missing, so it should use the default
        assert_eq!(config.writer_uploads_semaphore.available_permits(), 4);
    }

    #[test]
    fn test_app_config_default_trait() {
        let config = AppConfig::from(ParsedConfigMap::default());
        assert_eq!(config.writer_uploads_semaphore.available_permits(), 4);
    }

    #[test]
    fn test_parsed_config_map_with_job_retry_limit() {
        let mut data = BTreeMap::new();
        data.insert("writer_concurrent_uploads".to_string(), "6".to_string());
        data.insert("orchestrator_job_retry_limit".to_string(), "10".to_string());
        let config = from_k8s_data(&data).unwrap();
        assert_eq!(config.writer_uploads_semaphore.available_permits(), 6);
        assert_eq!(config.orchestrator_job_retry_limit, 10);
    }
}
