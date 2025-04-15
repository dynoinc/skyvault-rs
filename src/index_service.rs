use slog::{debug, info};
use slog_scope;
use tonic::{Request, Response, Status};

use crate::{Index, skyvault};

#[derive(Default)]
pub struct MyIndex;

#[tonic::async_trait]
impl Index for MyIndex {
    async fn index_document(
        &self,
        req: Request<skyvault::IndexRequest>,
    ) -> Result<Response<skyvault::IndexResponse>, Status> {
        let log = slog_scope::logger();
        let request_id = req
            .metadata()
            .get("x-request-id")
            .map(|v| v.to_str().unwrap_or("unknown"))
            .unwrap_or("unknown");

        info!(log, "Received index request"; "request_id" => request_id);
        debug!(log, "Processing index request"; "request" => ?req.get_ref());

        // Process the index request here

        let response = skyvault::IndexResponse {};
        info!(log, "Index request processed successfully"; "request_id" => request_id);

        Ok(Response::new(response))
    }
}
