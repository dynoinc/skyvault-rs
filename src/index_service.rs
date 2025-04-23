use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::metadata::MetadataStore;
use crate::proto::index_server::Index;
use crate::proto::{IndexRequest, IndexResponse};
use crate::storage::ObjectStore;

pub struct MyIndex {
    #[allow(dead_code)]
    metadata: Arc<dyn MetadataStore>,
    #[allow(dead_code)]
    storage: Arc<dyn ObjectStore>,
}

impl MyIndex {
    pub fn new(metadata: Arc<dyn MetadataStore>, storage: Arc<dyn ObjectStore>) -> Self {
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
