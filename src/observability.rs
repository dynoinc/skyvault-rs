use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
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
    counter,
    histogram,
};
use metrics_exporter_prometheus::{
    PrometheusBuilder,
    PrometheusHandle,
};
use sentry::ClientInitGuard;
use tokio::net::TcpListener;
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

#[derive(Clone, Copy)]
pub struct MetricsLayer;

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService { inner }
    }
}

#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for MetricsService<S>
where
    S: Service<http::Request<ReqBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Response: 'static,
    S::Error: 'static,
    ReqBody: Send + 'static,
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
        Box::pin(async move {
            let result = fut.await;
            if let Some((service, method)) = parse_method(&path) {
                let elapsed = start.elapsed().as_secs_f64();

                let common_labels = [
                    ("service".to_string(), service.clone()),
                    ("method".to_string(), method.clone()),
                ];

                histogram!("grpc_request_duration_seconds", &common_labels).record(elapsed);

                if result.is_ok() {
                    let status_ok_labels = common_labels
                        .iter()
                        .cloned()
                        .chain(std::iter::once(("status".to_string(), "ok".to_string())))
                        .collect::<Vec<_>>();
                    counter!("grpc_requests_total", &status_ok_labels).increment(1);
                } else {
                    let status_error_labels = common_labels
                        .iter()
                        .cloned()
                        .chain(std::iter::once(("status".to_string(), "error".to_string())))
                        .collect::<Vec<_>>();
                    counter!("grpc_requests_total", &status_error_labels).increment(1);
                }
            }
            result
        })
    }
}

fn parse_method(path: &str) -> Option<(String, String)> {
    let mut parts = path.split('/');
    parts.next()?;
    let service_candidate = parts.next()?;
    if service_candidate.is_empty()
        || service_candidate.starts_with('v') && service_candidate.chars().skip(1).all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let method_candidate = parts.next()?;
    if method_candidate.is_empty() {
        return None;
    }
    Some((service_candidate.to_string(), method_candidate.to_string()))
}
