use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        Mutex,
    },
    task::{
        Context,
        Poll,
    },
    time::Instant,
};

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    routing::get,
};
use metrics::{
    Counter,
    Histogram,
    counter,
    histogram,
};
use metrics_exporter_prometheus::{
    PrometheusBuilder,
    PrometheusHandle,
};
use sentry::ClientInitGuard;
use tokio::net::TcpListener;
use tonic::Code;
use tower::{
    Layer,
    Service,
};
use tracing_subscriber::{
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

pub fn init_tracing_and_sentry() -> Option<ClientInitGuard> {
    let guard = std::env::var("SENTRY_DSN").ok().map(|dsn| {
        sentry::init((
            dsn,
            sentry::ClientOptions {
                release: Some(env!("CARGO_PKG_VERSION").into()),
                ..Default::default()
            },
        ))
    });

    let fmt_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true);

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(fmt_layer)
        .with(sentry_tracing::layer())
        .init();

    guard
}

pub fn init_metrics_recorder() -> PrometheusHandle {
    let builder = PrometheusBuilder::new();
    builder.install_recorder().expect("failed to install metrics recorder")
}

async fn metrics_handler(State(handle): State<PrometheusHandle>) -> (StatusCode, String) {
    (StatusCode::OK, handle.render())
}

pub async fn serve_metrics(addr: SocketAddr, handle: PrometheusHandle) {
    let app = Router::new().route("/metrics", get(metrics_handler)).with_state(handle);

    tracing::info!("Metrics server listening on {}", addr);
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, "Failed to bind metrics server address");
            return;
        },
    };
    if let Err(e) = axum::serve(listener, app.into_make_service()).await {
        tracing::error!(error = %e, "metrics server error");
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct MetricsKey {
    path: String,
    grpc_status: String,
}

#[derive(Clone)]
struct MetricsHandles {
    request_counter: Counter,
    duration_histogram: Histogram,
}

#[derive(Clone, Copy)]
pub struct MetricsLayer;

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService::new(inner)
    }
}

#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    metrics_cache: Arc<Mutex<HashMap<MetricsKey, MetricsHandles>>>,
}

impl<S> MetricsService<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            metrics_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for MetricsService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: 'static,
    ReqBody: Send + 'static,
    ResBody: 'static,
{
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;
    type Response = S::Response;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let path = req.uri().path().to_string();
        let start = Instant::now();
        let fut = self.inner.call(req);
        let metrics_cache = self.metrics_cache.clone();

        Box::pin(async move {
            let result = fut.await;
            let elapsed = start.elapsed().as_secs_f64();

            let grpc_status = match &result {
                Ok(response) => response
                    .headers()
                    .get("grpc-status")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("0")
                    .to_string(),
                Err(_) => "unknown".to_string(),
            };

            let key = MetricsKey {
                path: path.clone(),
                grpc_status: grpc_status.clone(),
            };

            // Get or create metrics handles from cache
            let handles = {
                let mut cache = metrics_cache.lock().unwrap();

                if let Some(handles) = cache.get(&key) {
                    Some(handles.clone())
                } else {
                    // Only parse path on cache miss
                    if let Some((service, method)) = parse_method(&path) {
                        let status_category = grpc_status_to_category(&grpc_status);
                        let grpc_status_name = grpc_status_to_name(&grpc_status);

                        let labels = vec![
                            ("service".to_string(), service.clone()),
                            ("method".to_string(), method.clone()),
                            ("grpc_status".to_string(), grpc_status_name),
                            ("status".to_string(), status_category.to_string()),
                        ];

                        let handles = MetricsHandles {
                            request_counter: counter!("skyvault/grpc_requests_total", &labels),
                            duration_histogram: histogram!("skyvault/grpc_request_duration_seconds", &labels),
                        };

                        let result = handles.clone();
                        cache.insert(key, handles);
                        Some(result)
                    } else {
                        // Invalid path, don't create metrics
                        None
                    }
                }
            };

            // Record metrics using cached handles if available
            if let Some(handles) = handles {
                handles.duration_histogram.record(elapsed);
                handles.request_counter.increment(1);
            }

            result
        })
    }
}

fn parse_method(path: &str) -> Option<(String, String)> {
    let mut parts = path.split('/');
    parts.next()?;
    let service_candidate = parts.next()?;
    if service_candidate.is_empty() {
        return None;
    }
    let method_candidate = parts.next()?;
    if method_candidate.is_empty() {
        return None;
    }
    Some((service_candidate.to_string(), method_candidate.to_string()))
}

fn grpc_status_to_name(code: &str) -> String {
    let code_int = code.parse::<i32>().unwrap_or(-1);
    let grpc_code = Code::from_i32(code_int);

    match code {
        "unknown" => "UNKNOWN".to_string(),
        _ => format!("{grpc_code:?}"),
    }
}

fn grpc_status_to_category(code: &str) -> &'static str {
    let code_int = code.parse::<i32>().unwrap_or(-1);
    let grpc_code = Code::from_i32(code_int);

    match grpc_code {
        Code::Ok => "ok",
        // Client errors - typically user/application errors
        Code::Cancelled
        | Code::InvalidArgument
        | Code::NotFound
        | Code::AlreadyExists
        | Code::PermissionDenied
        | Code::FailedPrecondition
        | Code::OutOfRange
        | Code::Unauthenticated => "client_error",
        // Server errors - typically service/infrastructure errors
        Code::Unknown
        | Code::DeadlineExceeded
        | Code::ResourceExhausted
        | Code::Aborted
        | Code::Unimplemented
        | Code::Internal
        | Code::Unavailable
        | Code::DataLoss => "server_error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_status_to_name() {
        assert_eq!(grpc_status_to_name("0"), "Ok");
        assert_eq!(grpc_status_to_name("3"), "InvalidArgument");
        assert_eq!(grpc_status_to_name("13"), "Internal");
        assert_eq!(grpc_status_to_name("unknown"), "UNKNOWN");
        assert_eq!(grpc_status_to_name("999"), "Unknown"); // Invalid code defaults to Unknown
    }

    #[test]
    fn test_grpc_status_to_category() {
        // OK status
        assert_eq!(grpc_status_to_category("0"), "ok");

        // Client errors
        assert_eq!(grpc_status_to_category("1"), "client_error"); // Cancelled
        assert_eq!(grpc_status_to_category("3"), "client_error"); // InvalidArgument
        assert_eq!(grpc_status_to_category("5"), "client_error"); // NotFound
        assert_eq!(grpc_status_to_category("16"), "client_error"); // Unauthenticated

        // Server errors
        assert_eq!(grpc_status_to_category("2"), "server_error"); // Unknown
        assert_eq!(grpc_status_to_category("13"), "server_error"); // Internal
        assert_eq!(grpc_status_to_category("14"), "server_error"); // Unavailable
        assert_eq!(grpc_status_to_category("999"), "server_error"); // Invalid code
    }

    #[test]
    fn test_parse_method() {
        // Valid gRPC paths
        assert_eq!(
            parse_method("/UserService/GetUser"),
            Some(("UserService".to_string(), "GetUser".to_string()))
        );
        assert_eq!(
            parse_method("/skyvault.CacheService/Get"),
            Some(("skyvault.CacheService".to_string(), "Get".to_string()))
        );

        // Valid path with extra parts (should still work)
        assert_eq!(
            parse_method("/UserService/GetUser/extra"),
            Some(("UserService".to_string(), "GetUser".to_string()))
        );

        // Invalid paths - empty components
        assert_eq!(parse_method("//GetUser"), None);
        assert_eq!(parse_method("/UserService/"), None);
        assert_eq!(parse_method("/UserService"), None);

        // Invalid paths - insufficient parts
        assert_eq!(parse_method("/"), None);
        assert_eq!(parse_method(""), None);
        assert_eq!(parse_method("UserService/GetUser"), None); // Missing leading slash
    }
}
