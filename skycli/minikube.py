

import re
import subprocess
import grpc


def setup_connection(service_name: str) -> grpc.Channel:
    command = [
        "minikube",
        "service",
        service_name,
        "--url",
        "--format={{.IP}}:{{.Port}}",
    ]
    tunnel_process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    # Blocking wait for URL from stdout
    line = tunnel_process.stdout.readline().strip()
    match = re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+$", line)
    if match:
        service_url = line
    else:
        raise RuntimeError(f"Unable to get service URL for {service_name}. Got: '{line}'")

    channel = grpc.insecure_channel(service_url)
    return channel