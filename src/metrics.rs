use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use metrics::{histogram, increment_counter};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tower::{Layer, Service};

pub fn init_recorder() -> PrometheusHandle {
    let builder = PrometheusBuilder::new();
    let recorder = builder.build_recorder();
    let handle = builder.build_handle();
    metrics::set_boxed_recorder(Box::new(recorder)).expect("failed to install metrics recorder");
    handle
}

pub async fn serve(addr: SocketAddr, handle: PrometheusHandle) {
    let make_svc = make_service_fn(move |_| {
        let handle = handle.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                let handle = handle.clone();
                async move {
                    if req.uri().path() == "/metrics" {
                        let body = handle.render();
                        Ok::<_, hyper::Error>(Response::new(Body::from(body)))
                    } else {
                        Ok::<_, hyper::Error>(
                            Response::builder().status(404).body(Body::empty()).unwrap(),
                        )
                    }
                }
            }))
        }
    });

    if let Err(e) = Server::bind(&addr).serve(make_svc).await {
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

pub struct MetricsService<S> {
    inner: S,
}

impl<S, B> Service<http::Request<B>> for MetricsService<S>
where
    S: Service<http::Request<B>> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        let path = req.uri().path().to_string();
        let start = Instant::now();
        let fut = self.inner.call(req);
        Box::pin(async move {
            let result = fut.await;
            if let Some((service, method)) = parse_method(&path) {
                let elapsed = start.elapsed().as_secs_f64();
                histogram!("grpc_request_duration_seconds", elapsed, "service" => service.clone(), "method" => method.clone());
                if result.is_ok() {
                    increment_counter!("grpc_requests_total", "service" => service, "method" => method, "status" => "ok");
                } else {
                    increment_counter!("grpc_requests_total", "service" => service, "method" => method, "status" => "error");
                }
            }
            result
        })
    }
}

fn parse_method(path: &str) -> Option<(String, String)> {
    let mut parts = path.split('/');
    parts.next()?;
    let service = parts.next()?.to_string();
    let method = parts.next()?.to_string();
    Some((service, method))
}
