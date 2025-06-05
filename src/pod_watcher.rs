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
use tracing::{
    debug,
    warn,
};

use std::net::IpAddr;

/// Represents a change in the pod list
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodChange {
    Added(String, IpAddr),
    Removed(String),
}

#[derive(Error, Debug)]
pub enum PodWatcherError {
    #[error("Kubernetes request error: {0}")]
    K8s(#[from] kube::Error),
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
    label_selector: String,
) -> Result<impl Stream<Item = Result<PodChange, PodWatcherError>>, PodWatcherError> {
    let pods_api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
    let raw_stream = watcher(pods_api, watcher::Config::default().labels(&label_selector));

    let pod_stream = stream! {
        pin_mut!(raw_stream);

        while let Some(event) = raw_stream.next().await {
            match event {
                Ok(event) => {
                    match event {
                        watcher::Event::Apply(pod) | watcher::Event::InitApply(pod) => {
                            if let Some(name) = pod.metadata.name {
                                if let Some(status) = pod.status {
                                    if let Some(pod_ip) = status.pod_ip {
                                        if let Ok(ip_addr) = pod_ip.parse::<IpAddr>() {
                                            yield Ok(PodChange::Added(name, ip_addr));
                                        }
                                    }
                                }
                            }
                        }
                        watcher::Event::Delete(pod) => {
                            if let Some(name) = pod.metadata.name {
                                yield Ok(PodChange::Removed(name));
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