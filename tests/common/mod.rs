use std::{
    process::Stdio,
    time::Duration,
};

use anyhow::{
    Result,
    anyhow,
};
use tokio::process::{
    Child,
    Command,
};
use tokio_retry::{
    Retry,
    strategy::FixedInterval,
};
use tonic::transport::Channel;

/// Set up a gRPC connection to a service using kubectl port-forward.
/// Returns both the channel and a handle to the running process.
pub async fn setup_connection(service_name: &str) -> Result<(Channel, Child)> {
    // Get a random available port
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| anyhow!("Failed to bind to random port: {}", e))?;
    let local_port = listener
        .local_addr()
        .map_err(|e| anyhow!("Failed to get local port: {}", e))?
        .port();
    drop(listener);

    // Start port forwarding
    let child = Command::new("kubectl")
        .args([
            "port-forward",
            &format!("service/{service_name}"),
            &format!("{local_port}:50051"),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| anyhow!("Failed to spawn kubectl port-forward: {}", e))?;

    // Create channel using the forwarded port
    let endpoint = format!("http://127.0.0.1:{local_port}");
    eprintln!("Connecting to {endpoint} (using port-forward)");

    let retry_strategy = FixedInterval::from_millis(100).take(20);
    let channel = Retry::spawn(retry_strategy, || async {
        Channel::from_shared(endpoint.clone())
            .map_err(|e| anyhow!("Failed to create channel endpoint: {e}"))?
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await
            .map_err(|e| anyhow!("Failed to connect to service: {e}"))
    })
    .await?;

    Ok((channel, child))
}
