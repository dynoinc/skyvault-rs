import grpc
import subprocess
import re
import time
import sys
import os
import signal
import atexit

from proto import skyvault_pb2
from proto import skyvault_pb2_grpc

# Global variable to store the background process
tunnel_process = None

def cleanup_tunnel():
    """Terminate the minikube tunnel process if it's running."""
    global tunnel_process
    if tunnel_process and tunnel_process.poll() is None:
        print("\nTerminating minikube tunnel process...")
        # Get the process group ID (PGID)
        try:
            pgid = os.getpgid(tunnel_process.pid)
            # Send SIGTERM to the entire process group
            os.killpg(pgid, signal.SIGTERM)
            print(f"Sent SIGTERM to process group {pgid}.")
            # Wait a bit for graceful termination
            time.sleep(2) # Give it a couple of seconds
            # Check if it's still alive
            if tunnel_process.poll() is None:
                print("Tunnel process group did not terminate gracefully, sending SIGKILL.")
                os.killpg(pgid, signal.SIGKILL)
                tunnel_process.wait() # Wait for killing confirmation
                print("Tunnel process group killed.")
            else:
                print("Tunnel process group terminated gracefully.")

        except ProcessLookupError:
             print("Tunnel process already terminated.")
        except Exception as e:
            print(f"Error during tunnel cleanup: {e}. Attempting direct kill.", file=sys.stderr)
            try:
                tunnel_process.kill()
                tunnel_process.wait()
            except Exception as kill_e:
                 print(f"Error during direct kill attempt: {kill_e}", file=sys.stderr)

        tunnel_process = None

# Register the cleanup function to be called on exit, signals
atexit.register(cleanup_tunnel)
signal.signal(signal.SIGINT, lambda sig, frame: sys.exit(0)) # Handle Ctrl+C
signal.signal(signal.SIGTERM, lambda sig, frame: sys.exit(0)) # Handle termination signal

def get_minikube_service_url(service_name="skyvault-dev-primary", timeout=60):
    """Starts minikube tunnel in background and returns the service URL."""
    global tunnel_process
    command = [
        "minikube", "service", service_name,
        "--url", "--format={{.IP}}:{{.Port}}"
    ]
    print(f"Starting minikube tunnel: {' '.join(command)}")

    try:
        # Start the process in the background
        # Use Popen for non-blocking execution and capturing output/errors
        # Create a new process group so we can kill the process and its children reliably
        tunnel_process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=os.setsid # Create a new session and process group
        )

        url = None
        stderr_lines = []
        start_time = time.time()

        # Wait for the URL or timeout/error
        print("Waiting for service URL...")
        while time.time() - start_time < timeout:
            # Check stdout for the URL (non-blocking read might be better but readline is simpler for now)
            try:
                line = tunnel_process.stdout.readline()
                if line:
                    line = line.strip()
                    # Basic validation for IP:Port format
                    match = re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+$", line)
                    if match:
                        url = line
                        print(f"Successfully obtained service URL: {url}")
                        break # Exit loop once URL is found
                    else:
                        # might be other output before the URL
                        print(f"Tunnel stdout: {line}")
            except IOError:
                # stdout might be closed if process exits quickly
                pass

            # Check if the process exited prematurely
            if tunnel_process.poll() is not None:
                # Read remaining stderr
                stderr_output = "".join(stderr_lines) + tunnel_process.stderr.read()
                print(f"Error: Minikube tunnel process exited unexpectedly with code {tunnel_process.returncode}.", file=sys.stderr)
                if stderr_output:
                    print(f"Stderr:\n{stderr_output}", file=sys.stderr)
                tunnel_process = None # Mark as terminated
                return None

            # Non-blocking read from stderr (to capture errors without blocking)
            # This part is tricky with subprocess, might need select or threads for robustness
            # Simple approach: check stderr briefly if url not found yet
            try:
                 # This is still potentially blocking, a truly non-blocking read is more complex
                 # For simplicity, we'll rely on checking poll() and reading stderr if it exits.
                 pass
            except Exception:
                 pass

            time.sleep(0.5) # Avoid busy-waiting

        if url:
            return url
        else:
            print(f"Error: Failed to get service URL within {timeout} seconds.", file=sys.stderr)
            # Read any remaining stderr
            stderr_output = "".join(stderr_lines) + tunnel_process.stderr.read()
            if stderr_output:
                 print(f"Stderr:\n{stderr_output}", file=sys.stderr)
            cleanup_tunnel() # Ensure cleanup if timeout occurs
            return None

    except FileNotFoundError:
        print("Error: 'minikube' command not found. Make sure Minikube is installed and in your PATH.", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred while starting the tunnel: {e}", file=sys.stderr)
        cleanup_tunnel()
        return None

def run_smoke_test():
    """Connects to the SkyVault service via minikube tunnel and performs a write."""
    service_url = get_minikube_service_url()

    if not service_url:
        print("Smoke test failed: Could not obtain service URL.", file=sys.stderr)
        sys.exit(1)

    print(f"Connecting to SkyVault WriterService at {service_url}...")

    channel = None # Define channel outside try block for finally
    try:
        # Create insecure channel (assuming local dev setup)
        channel = grpc.insecure_channel(service_url)
        # Wait for the channel to be ready (optional but good practice)
        try:
            grpc.channel_ready_future(channel).result(timeout=10)
            print("gRPC channel ready.")
        except grpc.FutureTimeoutError:
            print(f"Error: Timed out waiting for gRPC channel to be ready at {service_url}", file=sys.stderr)
            sys.exit(1)

        stub = skyvault_pb2_grpc.WriterServiceStub(channel)

        # Prepare the request
        write_item = skyvault_pb2.WriteBatchItem(
            key="smoke_test_key_1",
            value=b"smoke_test_value_1"
        )
        table_request = skyvault_pb2.TableWriteBatchRequest(
            table_name="smoke_test_table",
            items=[write_item]
        )
        request = skyvault_pb2.WriteBatchRequest(tables=[table_request])

        print(f"Sending WriteBatch request to table '{table_request.table_name}' with key '{write_item.key}'")

        # Make the RPC call
        response = stub.WriteBatch(request, timeout=15) # Add timeout

        # WriteBatchResponse is currently empty, so success is just no exception
        print("WriteBatch RPC call successful.")
        print("Smoke test PASSED!")

    except grpc.RpcError as e:
        print(f"gRPC Error during WriteBatch call: {e.code()} - {e.details()}", file=sys.stderr)
        print("Smoke test FAILED.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during the smoke test: {e}", file=sys.stderr)
        print("Smoke test FAILED.", file=sys.stderr)
        sys.exit(1)
    finally:
        if channel:
            channel.close()
            print("gRPC channel closed.")
        # Explicitly call cleanup, though atexit should also handle it
        # Important to call AFTER gRPC channel is closed
        cleanup_tunnel()

if __name__ == "__main__":
    run_smoke_test() 
