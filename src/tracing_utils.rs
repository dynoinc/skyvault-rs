use tonic::Request;

pub fn request_id<T>(req: &Request<T>) -> Option<String> {
    req.metadata()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}
