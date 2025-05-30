use async_stream::stream;
use futures::{
    Stream,
    StreamExt,
    pin_mut,
};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::Api,
    runtime::watcher,
};
use thiserror::Error;
use tracing::{debug, warn};

#[derive(Error, Debug)]
pub enum PodWatcherError {
    #[error("Kubernetes request error: {0}")]
    K8sError(#[from] kube::Error),

    #[error("Failed to read namespace from ServiceAccount secret: {0}")]
    NamespaceReadError(#[from] std::io::Error),

    #[error("Pod missing app.kubernetes.io/component label")]
    MissingComponentLabel,

    #[error("Watcher error: {0}")]
    WatcherError(#[from] kube::runtime::watcher::Error),
}

/// Watches Kubernetes pods with a specific instance label and provides a
/// stream of pod changes.
///
/// Returns a tuple containing:
/// - A HashSet of current pod names
/// - A stream that yields pod addition and removal events
pub async fn watch(
    client: kube::Client,
    namespace: String,
) -> Result<impl Stream<Item = Result<PodChange, PodWatcherError>>, PodWatcherError> {
    // Get pod name from hostname
    let pod_name = hostname::get()?.to_string_lossy().into_owned();

    // Fetch our own pod
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let me = pods_api.get(&pod_name).await?;

    // Get the instance label
    let labels = me.metadata.labels.unwrap_or_default();
    let component = labels
        .get("app.kubernetes.io/component")
        .ok_or(PodWatcherError::MissingComponentLabel)?
        .clone();

    // Create label selector for pods with matching instance
    let label_selector = format!("app.kubernetes.io/component={component}");
    let raw_stream = watcher(
        pods_api,
        watcher::Config::default().labels(&label_selector),
    );

    let pod_stream = stream! {
        pin_mut!(raw_stream);

        while let Some(event) = raw_stream.next().await {
            match event {
                Ok(event) => {
                    match event {
                        watcher::Event::Apply(pod) | watcher::Event::InitApply(pod) => {
                            if let Some(ip) = pod.status.and_then(|status| status.pod_ip) {
                                yield Ok(PodChange::Added(ip));
                            }
                        }
                        watcher::Event::Delete(pod) => {
                            if let Some(ip) = pod.status.and_then(|status| status.pod_ip) {
                                yield Ok(PodChange::Removed(ip));
                            }
                        }
                        watcher::Event::Init => {
                            debug!("Pod watcher received Init event, starting initial sync.");
                        }
                        watcher::Event::InitDone => {
                            debug!("Pod watcher received InitDone event, initial sync completed.");
                        }
                    }
                }
                Err(e) => {
                    warn!("Watcher error: {:?}", e);
                }
            }
        }
    };

    Ok(pod_stream)
}

/// Represents a change in the pod list
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodChange {
    Added(String),
    Removed(String),
}
