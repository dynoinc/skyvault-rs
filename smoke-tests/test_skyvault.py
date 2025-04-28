import grpc
import subprocess
import re
import time
import sys
import atexit
import pytest

from proto import skyvault_pb2
from proto import skyvault_pb2_grpc

#
# Fixtures
#


@pytest.fixture(scope="session")
def service_connection():
    """Creates and manages a connection to the skyvault service via minikube."""
    # Start minikube service tunnel
    command = [
        "minikube",
        "service",
        "skyvault-dev-primary",
        "--url",
        "--format={{.IP}}:{{.Port}}",
    ]
    print(f"Starting minikube tunnel: {' '.join(command)}")
    tunnel_process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=sys.stderr, text=True
    )

    # Blocking wait for URL from stdout
    line = tunnel_process.stdout.readline().strip()
    match = re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+$", line)
    if match:
        service_url = line
        print(f"Service URL: {service_url}")
    else:
        pytest.fail(f"Failed to get service URL. Got: {line}")

    # Connect to the service
    channel = grpc.insecure_channel(service_url)
    try:
        grpc.channel_ready_future(channel).result(timeout=10)
    except grpc.FutureTimeoutError:
        tunnel_process.terminate()
        pytest.fail("Failed to connect to gRPC service")

    # Cleanup function to terminate tunnel process
    def cleanup():
        tunnel_process.terminate()
        tunnel_process.wait()

    # Register cleanup
    atexit.register(cleanup)

    yield channel

    # Cleanup after tests
    cleanup()
    atexit.unregister(cleanup)


@pytest.fixture(scope="session")
def stubs(service_connection):
    """Creates gRPC stubs for different services."""
    writer_stub = skyvault_pb2_grpc.WriterServiceStub(service_connection)
    reader_stub = skyvault_pb2_grpc.ReaderServiceStub(service_connection)
    orchestrator_stub = skyvault_pb2_grpc.OrchestratorServiceStub(service_connection)

    return {
        "writer": writer_stub,
        "reader": reader_stub,
        "orchestrator": orchestrator_stub,
    }


#
# Helper functions
#


def perform_write(stub, table_name, key, value_bytes):
    """Sends a WriteBatch request."""
    write_item = skyvault_pb2.WriteBatchItem(key=key, value=value_bytes)
    table_request = skyvault_pb2.TableWriteBatchRequest(
        table_name=table_name, items=[write_item]
    )
    request = skyvault_pb2.WriteBatchRequest(tables=[table_request])
    return stub.WriteBatch(request, timeout=15)


def perform_read_with_retry(
    stub, table_name, key, expected_value_bytes, retries=30, delay=1
):
    """Attempts to read a key, retrying until the expected value is found or retries run out."""
    table_request = skyvault_pb2.TableReadBatchRequest(
        table_name=table_name, keys=[key]
    )
    request = skyvault_pb2.GetBatchRequest(tables=[table_request])

    for attempt in range(retries):
        try:
            response = stub.GetBatch(request, timeout=10)
            if response.tables and response.tables[0].table_name == table_name:
                items = {item.key: item.value for item in response.tables[0].items}
                if key in items and items[key] == expected_value_bytes:
                    return True
        except Exception:
            pass  # Error handling omitted, just retry
        time.sleep(delay)

    return False


def trigger_compaction(stub):
    """Sends a KickOffWALCompaction request."""
    request = skyvault_pb2.KickOffWALCompactionRequest()
    response = stub.KickOffWALCompaction(request, timeout=20)

    # Wait for compaction job to complete with timeout
    start_time = time.time()
    while time.time() - start_time < 5:
        status_request = skyvault_pb2.GetJobStatusRequest(job_id=response.job_id)
        status_response = stub.GetJobStatus(status_request, timeout=10)
        if status_response.status == "completed":
            return response
        time.sleep(0.5)

    raise TimeoutError("Compaction job did not complete within 5 seconds")


#
# Test constants
#

TABLE_NAME = "smoke_test_table"
KEY_ONE = "smoke_test_key_1"
VALUE_ONE = b"smoke_test_value_1_verified"
KEY_TWO = "smoke_test_key_2_compact"
VALUE_TWO = b"smoke_test_value_2_compact_verified"

#
# Tests
#


@pytest.mark.smoke
def test_simple_write_and_read(stubs):
    """Test basic write and read operations work correctly."""
    writer_stub = stubs["writer"]
    reader_stub = stubs["reader"]

    # Write key-value pair
    perform_write(writer_stub, TABLE_NAME, KEY_ONE, VALUE_ONE)

    # Verify read works
    assert perform_read_with_retry(reader_stub, TABLE_NAME, KEY_ONE, VALUE_ONE), (
        f"Failed to read back key '{KEY_ONE}' after writing"
    )


@pytest.mark.smoke
def test_write_compact_read(stubs):
    """Test that data remains readable after WAL compaction."""
    writer_stub = stubs["writer"]
    reader_stub = stubs["reader"]
    orchestrator_stub = stubs["orchestrator"]

    # Write second key
    perform_write(writer_stub, TABLE_NAME, KEY_TWO, VALUE_TWO)

    # Verify second key is readable before compaction
    assert perform_read_with_retry(reader_stub, TABLE_NAME, KEY_TWO, VALUE_TWO), (
        f"Failed to read back key '{KEY_TWO}' before compaction"
    )

    # Trigger compaction
    trigger_compaction(orchestrator_stub)

    # Verify second key is still readable after compaction
    assert perform_read_with_retry(reader_stub, TABLE_NAME, KEY_TWO, VALUE_TWO), (
        f"Failed to read back key '{KEY_TWO}' after compaction"
    )

    # Verify first key is also still readable after compaction
    assert perform_read_with_retry(reader_stub, TABLE_NAME, KEY_ONE, VALUE_ONE), (
        f"Failed to read back key '{KEY_ONE}' after compaction"
    )
