# SkyVault Python Smoke Tests

This directory contains Python smoke tests for the SkyVault gRPC service.

## Setup

1.  **Install `uv`:** If you don't have `uv`, follow the installation instructions at [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv).
2.  **Create a virtual environment (from the `smoke-tests` directory):**
    ```bash
    uv venv
    ```
3.  **Activate the environment:**
    ```bash
    source .venv/bin/activate 
    # Or `.venv\Scripts\activate` on Windows
    ```
4.  **Install dependencies:**
    ```bash
    # Compile dependencies into a lock file (optional but recommended)
    uv pip compile pyproject.toml -o requirements.lock
    # Sync the environment with the lock file
    uv pip sync -r requirements.lock
    # Alternatively, install directly (without lock file):
    # uv pip install .
    ```
5.  **Generate gRPC code:** The tests require Python code generated from the `.proto` file. Run the build script (ensure you are in the `smoke-tests` directory):
    ```bash
    python build.py
    ```
    This will create a `generated/` directory containing `skyvault_pb2.py`, `skyvault_pb2_grpc.py` and `skyvault_pb2.pyi`.

## Running the Test

Ensure your Minikube cluster is running and the `skyvault-dev-primary` service is deployed.

Run the smoke test script (ensure you are in the `smoke-tests` directory and the virtual environment is active):

```bash
python smoke_test.py
```

The script will:
1.  Start a `minikube service skyvault-dev-primary --url ...` tunnel in the background.
2.  Parse the IP and port from the tunnel output.
3.  Connect to the `WriterService` via gRPC.
4.  Attempt to write a single record (`smoke_test_key_1`) to `smoke_test_table`.
5.  Print `PASSED` or `FAILED` and terminate the tunnel process.

## Troubleshooting

*   **`ImportError: No module named generated`**: Make sure you have run `python build.py` successfully from the `smoke-tests` directory while your virtual environment is active.
*   **`'minikube' command not found`**: Make sure Minikube is installed and accessible in your system's PATH.
*   **`Error: Proto file not found`**: Ensure the `../proto/proto/skyvault.proto` path relative to `smoke-tests/build.py` is correct and the file exists.
*   **gRPC connection errors / Timeout waiting for channel**: Verify the `skyvault-dev-primary` service is running correctly in Minikube (`minikube status`, `kubectl get pods -n <namespace>`). Check Minikube tunnel logs (`minikube service list`). Ensure the service name in `smoke_test.py` matches your deployment.
*   **Permission errors**: Ensure the script has permissions to run `minikube`.
*   **Tunnel process issues**: The script attempts to clean up the tunnel process. If it fails, you might need to manually kill the `minikube service` process (e.g., using `pkill -f 'minikube service skyvault-dev-primary'`). 