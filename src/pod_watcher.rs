use std::collections::HashSet;
use std::fs;
use std::time::Duration;

use async_stream::stream;
use futures::{Stream, StreamExt, pin_mut};
use hostname::get as get_hostname;
use k8s_openapi::api::core::v1::Pod;
use kube::Client;
use kube::api::{Api, ListParams, WatchEvent, WatchParams};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PodWatcherError {
    #[error("Kubernetes request error: {0}")]
    K8sError(#[from] kube::Error),

    #[error("Failed to read namespace from ServiceAccount secret: {0}")]
    NamespaceReadError(#[from] std::io::Error),

    #[error("Pod missing app.kubernetes.io/instance-id label")]
    MissingInstanceIdLabel,
}

/// Watches Kubernetes pods with a specific instance-id label and provides a stream of pod changes.
///
/// Returns a tuple containing:
/// - A HashSet of current pod names
/// - A stream that yields pod addition and removal events
pub async fn watch() -> Result<
    (
        HashSet<String>,
        impl Stream<Item = Result<PodChange, PodWatcherError>>,
    ),
    PodWatcherError,
> {
    // Create Kubernetes client
    let client = Client::try_default().await?;

    // Read namespace from the mounted ServiceAccount secret
    let namespace = fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")?
        .trim()
        .to_string();

    // Get pod name from hostname
    let pod_name = get_hostname()?.to_string_lossy().into_owned();

    // Fetch our own pod
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let me = pods_api.get(&pod_name).await?;

    // Get the instance-id label
    let labels = me.metadata.labels.unwrap_or_default();
    let instance_id = labels
        .get("app.kubernetes.io/instance-id")
        .ok_or(PodWatcherError::MissingInstanceIdLabel)?
        .clone();

    // Create label selector for pods with matching instance-id
    let label_selector = format!("app.kubernetes.io/instance-id={}", instance_id);
    let lp = ListParams::default().labels(&label_selector);

    // List all matching pods
    let pod_list = pods_api.list(&lp).await?;

    // Extract pod names
    let pod_names: HashSet<String> = pod_list
        .iter()
        .filter_map(|pod| pod.metadata.name.clone())
        .collect();

    let mut last_version = pod_list.metadata.resource_version;

    let wp = WatchParams::default().labels(&label_selector).timeout(60); // 1-minute timeout for long polling

    // Create the stream of pod changes
    let stream = stream! {
        loop {
            let watch_future = pods_api.watch(&wp, &last_version.clone().unwrap_or_default()).await;

            match watch_future {
                Ok(watch_stream) => {
                    pin_mut!(watch_stream);

                    while let Some(event_result) = watch_stream.next().await {
                        match event_result {
                            Ok(event) => {
                                // Process the event
                                match event {
                                    WatchEvent::Added(pod) => {
                                        if let Some(name) = pod.metadata.name.clone() {
                                            yield Ok(PodChange::Added(name));
                                        }
                                    },
                                    WatchEvent::Deleted(pod) => {
                                        if let Some(name) = pod.metadata.name.clone() {
                                            yield Ok(PodChange::Removed(name));
                                        }
                                    },
                                    WatchEvent::Bookmark(bookmark) => {
                                        last_version = Some(bookmark.metadata.resource_version);
                                    },
                                    _ => {} // Ignore other event types
                                }
                            },
                            Err(e) => {
                                yield Err(PodWatcherError::K8sError(e));
                                break;
                            }
                        }
                    }
                },
                Err(e) => {
                    yield Err(PodWatcherError::K8sError(e));
                }
            }

            // If the watch connection breaks, wait before reconnecting
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };

    Ok((pod_names, stream))
}

/// Represents a change in the pod list
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodChange {
    Added(String),
    Removed(String),
}
