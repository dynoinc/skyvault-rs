mod allocator;

pub mod proto {
    tonic::include_proto!("skyvault.v1");

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("skyvault_descriptor");
}

mod cache;
pub mod cache_service;
pub mod config;
mod consistent_hashring;
pub mod dynamic_config;
mod forest;
mod job_watcher;
pub mod jobs;
pub mod k8s;
mod k_way;
pub mod metadata;
pub mod observability;
pub mod orchestrator_service;
mod pod_watcher;
pub mod reader_service;
pub mod runs;
pub mod storage;
pub mod writer_service;

#[cfg(test)]
pub mod test_utils;

#[cfg(test)]
mod metadata_tests;
