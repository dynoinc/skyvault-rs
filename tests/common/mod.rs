use std::process::Stdio;

use anyhow::{
    Result,
    anyhow,
};
use tokio::process::{
    Child,
    Command,
};
use tonic::transport::Channel;

/// Set up a gRPC connection to a minikube service.
/// This function runs `minikube service <service_name> --url
/// --format={{.IP}}:{{.Port}}` in the background and creates an insecure
/// gRPC channel. Returns both the channel and a handle to the running process.
pub async fn setup_connection(service_name: &str) -> Result<(Channel, Child)> {
    let mut command = Command::new("minikube");
    command
        .args(["service", service_name, "--url", "--format={{.IP}}:{{.Port}}"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|e| anyhow!("Failed to spawn minikube command: {}", e))?;

    // Read the initial output to get the service URL
    let stdout = child.stdout.take().ok_or_else(|| anyhow!("Failed to capture stdout"))?;

    let mut reader = tokio::io::BufReader::new(stdout);
    let mut service_url = String::new();

    use tokio::io::AsyncBufReadExt;
    reader
        .read_line(&mut service_url)
        .await
        .map_err(|e| anyhow!("Failed to read service URL: {}", e))?;

    let service_url = service_url.trim();

    // Validate the format matches IP:PORT pattern
    let ip_port_regex = regex::Regex::new(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+$")
        .map_err(|e| anyhow!("Failed to compile regex: {}", e))?;

    if !ip_port_regex.is_match(service_url) {
        // Kill the child process if URL format is invalid
        let _ = child.kill().await;
        return Err(anyhow!(
            "Unable to get service URL for {}. Got: '{}'",
            service_name,
            service_url
        ));
    }

    // Create insecure gRPC channel with health check
    let endpoint = format!("http://{service_url}");
    eprintln!("Connecting to {endpoint}");
    let channel = Channel::from_shared(endpoint)
        .map_err(|e| anyhow!("Failed to create channel endpoint: {}", e))?
        .connect_timeout(std::time::Duration::from_secs(2))
        .connect()
        .await
        .map_err(|e| {
            // Kill the child process if connection fails
            let _ = child.start_kill();
            anyhow!("Failed to connect to service: {}", e)
        })?;

    Ok((channel, child))
}
