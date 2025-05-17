use sentry::ClientInitGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn init() -> Option<ClientInitGuard> {
    let guard = std::env::var("SENTRY_DSN").ok().map(|dsn| {
        sentry::init((dsn, sentry::ClientOptions {
            release: Some(env!("CARGO_PKG_VERSION").into()),
            ..Default::default()
        }))
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
