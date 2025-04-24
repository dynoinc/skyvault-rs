use tonic::{Request, Response, Status};

use crate::metadata::MetadataStore;
use crate::proto::index_server::Index;
use crate::proto::{IndexRequest, IndexResponse};
use crate::storage::ObjectStore;

pub struct MyIndex {
    #[allow(dead_code)]
    metadata: MetadataStore,
    #[allow(dead_code)]
    storage: ObjectStore,
}

impl MyIndex {
    #[must_use]
    pub fn new(metadata: MetadataStore, storage: ObjectStore) -> Self {
        Self { metadata, storage }
    }
}

#[tonic::async_trait]
impl Index for MyIndex {
    async fn index_document(
        &self,
        _request: Request<IndexRequest>,
    ) -> Result<Response<IndexResponse>, Status> {
        Ok(Response::new(IndexResponse {}))
    }
}
