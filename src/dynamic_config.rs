// src/dynamic_config.rs
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::Duration,
};

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::ConfigMap;
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

// --- Error Handling ---
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
}

// --- Configuration Struct ---
#[derive(Debug, Clone)]
pub struct AppConfig {
    // Semaphore to control concurrent uploads
    pub uploads_semaphore: Arc<Semaphore>,
    pub job_retry_limit: i32,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self::from(ParsedConfigMap::default())
    }
}

#[derive(Debug, Clone)]
struct ParsedConfigMap {
    pub concurrent_uploads: u32,
    pub job_retry_limit: i32,
}

impl Default for ParsedConfigMap {
    fn default() -> Self {
        Self {
            concurrent_uploads: 4,
            job_retry_limit: 3,
        }
    }
}

impl ParsedConfigMap {
    fn from_k8s_data(data: &BTreeMap<String, String>) -> Result<Self, ConfigError> {
        let mut config = Self::default();

        if let Some(uploads_str) = data.get("concurrent_uploads") {
            if let Ok(uploads) = uploads_str.parse::<u32>() {
                config.concurrent_uploads = uploads;
            }
        }

        if let Some(retry_str) = data.get("job_retry_limit") {
            if let Ok(retries) = retry_str.parse::<i32>() {
                config.job_retry_limit = retries;
            }
        }

        Ok(config)
    }
}

impl From<ParsedConfigMap> for AppConfig {
    fn from(parsed_config: ParsedConfigMap) -> Self {
        AppConfig {
            uploads_semaphore: Arc::new(Semaphore::new(parsed_config.concurrent_uploads as usize)),
            job_retry_limit: parsed_config.job_retry_limit,
        }
    }
}

impl AppConfig {
    fn apply(&mut self, new_config: ParsedConfigMap) {
        let current_permits = self.uploads_semaphore.available_permits();
        let new_permits = new_config.concurrent_uploads as usize;

        if new_permits > current_permits {
            // Add permits if new limit is higher
            self.uploads_semaphore.add_permits(new_permits - current_permits);
        } else if new_permits < current_permits {
            // Remove permits if new limit is lower by acquiring them
            for _ in 0..(current_permits - new_permits) {
                // Try to acquire permits, but don't wait if they're not available
                if let Ok(permit) = self.uploads_semaphore.try_acquire() {
                    // Immediately drop the permit to effectively remove it
                    drop(permit);
                }
            }
        }

        // Update job_retry_limit
        self.job_retry_limit = new_config.job_retry_limit;
    }
}

/// Type alias for the shared, dynamically updatable application configuration.
pub type SharedAppConfig = Arc<RwLock<AppConfig>>;

async fn load_initial_config(client: Client, namespace: &str) -> Result<AppConfig, ConfigError> {
    let api: Api<ConfigMap> = Api::namespaced(client, namespace);
    info!("Attempting to load initial configuration from ConfigMap '{namespace}/skyvault-dynamic-config'");

    match api.get("skyvault-dynamic-config").await {
        Ok(cm) => {
            if let Some(data_map) = &cm.data {
                if data_map.is_empty() {
                    warn!("ConfigMap '{namespace}/skyvault-dynamic-config' 'data' field is empty. Using defaults.");
                    Ok(AppConfig::from(ParsedConfigMap::default()))
                } else {
                    let deserializable_config = ParsedConfigMap::from_k8s_data(data_map)?;
                    Ok(AppConfig::from(deserializable_config))
                }
            } else {
                warn!("ConfigMap '{namespace}/skyvault-dynamic-config' has no 'data' field. Using defaults.");
                Ok(AppConfig::from(ParsedConfigMap::default()))
            }
        },
        Err(kube::Error::Api(ae)) if ae.code == 404 => {
            warn!("ConfigMap '{namespace}/skyvault-dynamic-config' not found. Using default configuration.");
            Ok(AppConfig::from(ParsedConfigMap::default()))
        },
        Err(e) => Err(ConfigError::KubeError(e)),
    }
}

async fn watch_config_changes_task(client: Client, namespace: String, shared_config: SharedAppConfig) {
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), &namespace);
    let watcher_params = watcher::Config::default();

    info!("Starting ConfigMap watcher for '{namespace}/skyvault-dynamic-config'");

    loop {
        let mut watch_stream = watcher(api.clone(), watcher_params.clone()).boxed();
        info!("Watcher stream (re)established for ConfigMap '{namespace}/skyvault-dynamic-config'");

        while let Some(event_result) = watch_stream.next().await {
            match event_result {
                Ok(event) => match event {
                    Event::Apply(cm) => {
                        if cm.name_any() == "skyvault-dynamic-config" {
                            info!("ConfigMap '{namespace}/skyvault-dynamic-config' added/modified. Processing update.");
                            if let Some(data_map) = &cm.data {
                                let new_config = ParsedConfigMap::from_k8s_data(data_map).unwrap_or_default();
                                let mut config_guard = shared_config.write().await;
                                config_guard.apply(new_config.clone());
                                info!("Successfully reloaded configuration: {new_config:?}");
                            } else {
                                warn!(
                                    "ConfigMap '{namespace}/skyvault-dynamic-config' event received but 'data' field \
                                     was empty."
                                );
                            }
                        }
                    },
                    Event::Delete(cm) => {
                        if cm.name_any() == "skyvault-dynamic-config" {
                            warn!(
                                "ConfigMap '{namespace}/skyvault-dynamic-config' was deleted. Reverting to default \
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
    let initial_config = load_initial_config(client.clone(), namespace).await?;

    info!("Initial dynamic configuration loaded: {initial_config:?}");
    let shared_config = Arc::new(RwLock::new(initial_config));

    let watcher_client = client.clone();
    let watcher_shared_config = shared_config.clone();
    tokio::spawn(watch_config_changes_task(
        watcher_client,
        namespace.to_string(),
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
        assert_eq!(config.uploads_semaphore.available_permits(), 4);
    }

    #[test]
    fn test_app_config_from_k8s_data_valid() {
        let mut data = BTreeMap::new();
        data.insert("concurrent_uploads".to_string(), "10".to_string());
        let config = from_k8s_data(&data).unwrap();
        assert_eq!(config.uploads_semaphore.available_permits(), 10);
    }

    #[test]
    fn test_app_config_from_k8s_data_invalid_value() {
        let mut data = BTreeMap::new();
        data.insert("concurrent_uploads".to_string(), "not_a_number".to_string());
        let config = from_k8s_data(&data).unwrap();
        // Should fall back to default because "not_a_number" is not parsable to u32
        assert_eq!(config.uploads_semaphore.available_permits(), 4);
    }

    #[test]
    fn test_app_config_from_k8s_data_missing_key() {
        let mut data = BTreeMap::new();
        data.insert("other_key".to_string(), "some_value".to_string());
        let config = from_k8s_data(&data).unwrap();
        // concurrent_uploads key is missing, so it should use the default
        assert_eq!(config.uploads_semaphore.available_permits(), 4);
    }

    #[test]
    fn test_app_config_default_trait() {
        let config = AppConfig::from(ParsedConfigMap::default());
        assert_eq!(config.uploads_semaphore.available_permits(), 4);
    }
}
