use std::{
    future::Future,
    pin::Pin,
    task::{
        Context,
        Poll,
    },
    time::Instant,
};

use opentelemetry::{
    KeyValue,
    global,
    metrics::{
        Counter,
        Histogram,
    },
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use sentry::ClientInitGuard;
use sentry_tracing::EventFilter;
use tonic::Code;
use tower::{
    Layer,
    Service,
};
use tracing::{
    debug,
    error,
};
use tracing_subscriber::{
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

use crate::config::{
    OtelConfig,
    SentryConfig,
};

pub fn init_tracing_and_sentry(sentry_config: SentryConfig) -> Option<ClientInitGuard> {
    let guard = if sentry_config.dsn.is_empty() {
        None
    } else {
        Some(sentry::init((
            sentry_config.dsn,
            sentry::ClientOptions {
                release: Some(env!("CARGO_PKG_VERSION").into()),
                traces_sample_rate: sentry_config.sample_rate,
                ..Default::default()
            },
        )))
    };

    let fmt_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true);

    let sentry_layer = sentry_tracing::layer().event_filter(|md| match md.level() {
        &tracing::Level::ERROR => EventFilter::Event,
        _ => EventFilter::Ignore,
    });

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(fmt_layer)
        .with(sentry_layer)
        .init();

    guard
}

pub fn init_otel_metrics(otel_config: OtelConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if otel_config.endpoint.is_empty() {
        tracing::info!("No OTEL endpoint configured, skipping metrics initialization");
        return Ok(());
    }

    // Create OTLP metrics exporter
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(&otel_config.endpoint)
        .build()?;

    // Create a meter provider with the OTLP exporter
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_attributes(vec![KeyValue::new("service.name", "skyvault")])
                .build(),
        )
        .build();

    global::set_meter_provider(provider);
    tracing::info!(
        "OpenTelemetry metrics initialized with OTLP exporter endpoint: {}",
        otel_config.endpoint
    );
    Ok(())
}

#[derive(Clone, Copy)]
pub struct ObservabilityLayer;

impl<S> Layer<S> for ObservabilityLayer {
    type Service = ObservabilityService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ObservabilityService::new(inner)
    }
}

#[derive(Clone)]
pub struct ObservabilityService<S> {
    inner: S,

    request_counter: Counter<u64>,
    duration_histogram: Histogram<f64>,
}

impl<S> ObservabilityService<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            request_counter: global::meter("skyvault")
                .u64_counter("skyvault_server_grpc_requests_total")
                .build(),
            duration_histogram: global::meter("skyvault")
                .f64_histogram("skyvault_server_grpc_request_duration_seconds")
                .build(),
        }
    }
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for ObservabilityService<S>
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
        let start = Instant::now();
        let path = req.uri().path().to_string();
        let fut = self.inner.call(req);
        let service_clone = self.clone();

        Box::pin(async move {
            let result = fut.await;
            let elapsed = start.elapsed();

            let (grpc_status, grpc_message) = match &result {
                Ok(response) => {
                    let status = response
                        .headers()
                        .get("grpc-status")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("0")
                        .to_string();
                    let message = response
                        .headers()
                        .get("grpc-message")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| urlencoding::decode(s).ok())
                        .map(|s| s.to_string());
                    (status, message)
                },
                Err(_) => ("unknown".to_string(), None),
            };

            let (service, method) = parse_method(&path).unwrap_or(("unknown".to_string(), "unknown".to_string()));

            let attributes = vec![
                KeyValue::new("service", service.clone()),
                KeyValue::new("method", method.clone()),
                KeyValue::new("grpc_status", grpc_status_to_name(&grpc_status)),
                KeyValue::new("status", grpc_status_to_category(&grpc_status).to_string()),
            ];
            service_clone
                .duration_histogram
                .record(elapsed.as_secs_f64(), &attributes);
            service_clone.request_counter.add(1, &attributes);

            match grpc_status_to_category(&grpc_status) {
                "ok" => debug!(
                    service = %service,
                    method = %method,
                    grpc_status = %grpc_status,
                    duration_ms = elapsed.as_millis(),
                    "gRPC request completed"
                ),
                "client_error" => tracing::warn!(
                    service = %service,
                    method = %method,
                    grpc_status = %grpc_status,
                    grpc_message = %grpc_message.as_deref().unwrap_or(""),
                    duration_ms = elapsed.as_millis(),
                    "gRPC request completed with client error"
                ),
                _ => error!(
                    service = %service,
                    method = %method,
                    grpc_status = %grpc_status,
                    grpc_message = %grpc_message.as_deref().unwrap_or(""),
                    duration_ms = elapsed.as_millis(),
                    "gRPC request failed"
                ),
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
