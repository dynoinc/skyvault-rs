use async_stream::stream;
use futures::{
    Stream,
    StreamExt,
    pin_mut,
};
use k8s_openapi::api::batch::v1::Job;
use kube::{
    api::Api,
    runtime::watcher::{
        self,
    },
};
use thiserror::Error;
use tracing::{
    debug,
    error,
    info,
    warn,
};

use crate::metadata::JobID;

#[derive(Error, Debug)]
pub enum JobWatcherError {
    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("Watcher stream error: {0}")]
    WatcherStreamError(#[from] kube::runtime::watcher::Error),

    #[error("Failed to parse JobID: {0}")]
    ParseError(#[from] std::num::ParseIntError),

    #[error("Job missing name metadata")]
    JobMissingName,

    #[error("Could not extract JobID string from job name: {0}")]
    JobIDExtractionError(String),
}

/// Represents a change in the job status
#[derive(Debug, Clone)]
pub enum JobChange {
    Added(JobID, String),     // JobID, Job Name
    Completed(JobID, String), // JobID, Job Name
    Failed(JobID, String),    // JobID, Job Name
}

/// Watches Kubernetes jobs with a specific label selector and provides a stream
/// of job changes.
///
/// Returns a stream that yields job status change events.
pub async fn watch(
    client: kube::Client,
    namespace: String,
    label_selector: &str,
) -> Result<impl Stream<Item = Result<JobChange, JobWatcherError>>, JobWatcherError> {
    let jobs_api: Api<Job> = Api::namespaced(client.clone(), &namespace);
    info!(
        "Starting K8s job watcher for namespace '{}' with selector: {}",
        namespace, label_selector
    );

    let watcher_config = watcher::Config::default().labels(label_selector);

    let raw_watcher_stream = watcher::watcher(jobs_api, watcher_config);

    let job_stream = stream! {
        pin_mut!(raw_watcher_stream);

        while let Some(event_res) = raw_watcher_stream.next().await {
            match event_res {
                Ok(event) => {
                    let (job_obj, event_type_str) = match &event {
                        watcher::Event::Apply(job) => (job, "Apply"),
                        watcher::Event::InitApply(job) => (job, "InitApply"),
                        watcher::Event::Delete(job) => (job, "Delete"),
                        watcher::Event::Init => {
                            debug!("Job watcher received Init event, starting initial sync.");
                            continue;
                        }
                        watcher::Event::InitDone => {
                            debug!("Job watcher received InitDone event, initial sync completed.");
                            continue;
                        }
                    };

                    let job_name = match job_obj.metadata.name.clone() {
                        Some(name) => name,
                        None => {
                            warn!("Job event ({}) for job without a name. Skipping.", event_type_str);
                            continue;
                        }
                    };

                    let job_id_str = match extract_job_id_from_name(&job_name) {
                        Some(id_str) => id_str,
                        None => {
                            error!("Failed to extract job ID string from name: {} (event: {}). Skipping.", job_name, event_type_str);
                            continue;
                        }
                    };

                    let job_id = match parse_job_id(&job_id_str) {
                        Ok(id) => id,
                        Err(e) => {
                            error!("Failed to parse job ID '{}' from name '{}' (event: {}): {}. Skipping.", job_id_str, job_name, event_type_str, e);
                            continue;
                        }
                    };

                    match event {
                        watcher::Event::Apply(job) | watcher::Event::InitApply(job) => {
                            let completed = job.status.as_ref()
                                .and_then(|s| s.succeeded)
                                .map(|count| count > 0)
                                .unwrap_or(false);

                            // Check if job has truly failed by comparing failed attempts against backoffLimit
                            let failed = job.status.as_ref()
                                .and_then(|s| s.failed)
                                .unwrap_or(0);

                            // Get backoffLimit from job spec (default is 6 if not specified)
                            let backoff_limit = job.spec.as_ref()
                                .and_then(|spec| spec.backoff_limit)
                                .unwrap_or(6);

                            let job_truly_failed = failed > 0 && failed >= backoff_limit;

                            if completed {
                                info!("Job {} (ID: {}) completed successfully", job_name, job_id);
                                yield Ok(JobChange::Completed(job_id, job_name));
                            } else if job_truly_failed {
                                info!("Job {} (ID: {}) failed after {} attempts (backoffLimit: {})", job_name, job_id, failed, backoff_limit);
                                yield Ok(JobChange::Failed(job_id, job_name));
                            } else {
                                if failed > 0 {
                                    info!("Job {} (ID: {}) has {} failed attempts, but still has retries remaining (backoffLimit: {})", job_name, job_id, failed, backoff_limit);
                                } else {
                                    info!("Tracking active/pending job: {} (ID: {})", job_name, job_id);
                                }
                                yield Ok(JobChange::Added(job_id, job_name));
                            }
                        }
                        watcher::Event::Delete(_deleted_job_obj) => {
                            warn!("Job {} (ID: {}) was deleted from Kubernetes. Marking as Failed for tracking.", job_name, job_id);
                            yield Ok(JobChange::Failed(job_id, job_name.clone()));
                        }
                        watcher::Event::Init | watcher::Event::InitDone => {}
                    }
                }
                Err(e) => {
                    error!(error = %e, "Error in job watcher stream event. The watcher might attempt to recover.");
                    yield Err(JobWatcherError::WatcherStreamError(e));
                }
            }
        }
        error!("Job watcher stream unexpectedly ended. This may indicate a persistent issue.");
    };

    Ok(job_stream)
}

/// Helper method to extract job ID from job name
/// Extracts the last segment after splitting by '-'.
/// Example: "wal-compaction-123" -> Some("123")
/// Example: "myjob" -> Some("myjob") (parse_job_id will then fail if not
/// numeric) Example: "" -> Some("") (parse_job_id will then fail)
pub fn extract_job_id_from_name(job_name: &str) -> Option<String> {
    // Ensure this aligns with how job names are constructed (e.g.,
    // format!("{job_type}-{job_id}")) rsplit will return the last part, or the
    // string itself if no '-' is present.
    job_name.rsplit('-').next().map(|s| s.to_string())
}

/// Helper to parse string job ID to numeric JobID
pub fn parse_job_id(job_id_str: &str) -> Result<JobID, std::num::ParseIntError> {
    job_id_str.parse::<i64>().map(JobID::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_job_id_from_name() {
        // Test valid job names
        assert_eq!(extract_job_id_from_name("wal-compaction-123"), Some("123".to_string()));

        assert_eq!(
            extract_job_id_from_name("table-buffer-compaction-456"),
            Some("456".to_string())
        );

        assert_eq!(
            extract_job_id_from_name("table-tree-compaction-789"),
            Some("789".to_string())
        );

        // Test cases for names without hyphens or empty strings
        assert_eq!(extract_job_id_from_name("invalid123"), Some("invalid123".to_string())); // parse_job_id will handle if "invalid123" is not a number

        assert_eq!(extract_job_id_from_name("123"), Some("123".to_string()));

        assert_eq!(extract_job_id_from_name(""), Some("".to_string())); // parse_job_id will fail for empty string
    }

    #[test]
    fn test_parse_job_id() {
        // Test valid job ID
        let job_id = parse_job_id("123").unwrap();
        assert_eq!(i64::from(job_id), 123);

        // Test invalid job ID
        assert!(parse_job_id("not-a-number").is_err());
    }
}
