import grpc
import subprocess
import re
import time
import sys
import pytest

from skyvault.v1 import skyvault_pb2
from skyvault.v1 import skyvault_pb2_grpc

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
        "skyvault-dev",
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
        pytest.fail("Failed to connect to gRPC service")

    yield channel


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


def create_table(stub, table_name):
    try:
        request = skyvault_pb2.CreateTableRequest(
            config=skyvault_pb2.TableConfig(table_name=table_name)
        )
        response = stub.CreateTable(request, timeout=10)
        return response.seq_no
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.ALREADY_EXISTS:
            raise


def perform_write(stub, table_name, key, value_bytes):
    """Sends a WriteBatch request."""
    write_item = skyvault_pb2.WriteBatchItem(key=key, value=value_bytes)
    table_request = skyvault_pb2.TableWriteBatchRequest(
        table_name=table_name, items=[write_item]
    )
    request = skyvault_pb2.WriteBatchRequest(tables=[table_request])

    start_time = time.time()
    while time.time() - start_time < 5:
        try:
            return stub.WriteBatch(request, timeout=15).seq_no
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.NOT_FOUND:
                raise
            print(f"Write failed: {e}")
            time.sleep(1)

    raise grpc.RpcError("Write failed after retries")


def perform_read(stub, table_name, key, expected_value_bytes):
    """Sends a GetBatch request."""
    table_request = skyvault_pb2.TableGetBatchRequest(table_name=table_name, keys=[key])
    request = skyvault_pb2.GetBatchRequest(tables=[table_request])
    response = stub.GetBatch(request, timeout=15)
    if response.tables and response.tables[0].table_name == table_name:
        items = {item.key: item.value for item in response.tables[0].items}
        if key in items and items[key] == expected_value_bytes:
            return True
    return False


def perform_read_with_retry(
    stub, table_name, seq_no, key, expected_value_bytes, retries=30, delay=1
):
    """Attempts to read a key, retrying until the expected value is found or retries run out."""
    table_request = skyvault_pb2.TableGetBatchRequest(table_name=table_name, keys=[key])
    request = skyvault_pb2.GetBatchRequest(tables=[table_request])

    for attempt in range(retries):
        try:
            response = stub.GetBatch(request, timeout=10)
            if response.tables and response.tables[0].table_name == table_name:
                items = {item.key: item.value for item in response.tables[0].items}
                if key in items and items[key] == expected_value_bytes:
                    return True
        except grpc.RpcError as e:
            # Only retry if we get failed_precondition (seq_no mismatch)
            if e.code() != grpc.StatusCode.FAILED_PRECONDITION:
                raise

        time.sleep(delay)

    return False


def trigger_wal_compaction(stub):
    request = skyvault_pb2.KickOffJobRequest(
        wal_compaction=True,
    )
    response = stub.KickOffJob(request, timeout=20)

    # Wait for compaction job to complete with timeout
    start_time = time.time()
    while time.time() - start_time < 5:
        status_request = skyvault_pb2.GetJobStatusRequest(job_id=response.job_id)
        status_response = stub.GetJobStatus(status_request, timeout=10)
        if not status_response.pending:
            return status_response.seq_no
        time.sleep(0.5)

    raise TimeoutError("Compaction job did not complete within 5 seconds")


def persist_snapshot(stub):
    request = skyvault_pb2.PersistSnapshotRequest()
    response = stub.PersistSnapshot(request, timeout=10)
    return response.seq_no


@pytest.mark.smoke
def test_simple_write_and_read(stubs):
    """Test basic write and read operations work correctly."""
    writer_stub = stubs["writer"]
    reader_stub = stubs["reader"]
    orchestrator_stub = stubs["orchestrator"]

    table_name = "test_simple_write_and_read"
    key = "test_simple_write_and_read_key"
    value = b"test_simple_write_and_read_value"

    create_table(orchestrator_stub, table_name)

    # Write key-value pair
    seq_no = perform_write(writer_stub, table_name, key, value)

    # Verify read works
    assert perform_read_with_retry(reader_stub, table_name, seq_no, key, value), (
        f"Failed to read back key '{key}' after writing"
    )


@pytest.mark.smoke
def test_write_compact_read(stubs):
    """Test that data remains readable after WAL compaction."""
    writer_stub = stubs["writer"]
    reader_stub = stubs["reader"]
    orchestrator_stub = stubs["orchestrator"]

    table_name = "test_write_compact_read"
    key_one = "test_write_compact_read_key_one"
    value_one = b"test_write_compact_read_value_one"
    key_two = "test_write_compact_read_key_two"
    value_two = b"test_write_compact_read_value_two"

    create_table(orchestrator_stub, table_name)

    # Write second key
    seq_no = perform_write(writer_stub, table_name, key_one, value_one)
    seq_no = perform_write(writer_stub, table_name, key_two, value_two)

    # Verify second key is readable before compaction
    assert perform_read_with_retry(
        reader_stub, table_name, seq_no, key_two, value_two
    ), f"Failed to read back key '{key_two}' before compaction"

    # Trigger compaction
    seq_no = trigger_wal_compaction(orchestrator_stub)

    # Verify second key is still readable after compaction
    assert perform_read_with_retry(
        reader_stub, table_name, seq_no, key_two, value_two
    ), f"Failed to read back key '{key_two}' after compaction"

    # Verify first key is also still readable after compaction
    assert perform_read_with_retry(
        reader_stub, table_name, seq_no, key_one, value_one
    ), f"Failed to read back key '{key_one}' after compaction"


@pytest.mark.smoke
def test_snapshot_persistence(stubs):
    """Test that snapshots are persisted correctly."""
    writer_stub = stubs["writer"]
    reader_stub = stubs["reader"]
    orchestrator_stub = stubs["orchestrator"]

    table_name = "test_snapshot_persistence"
    key = "test_snapshot_persistence_key"
    value = b"test_snapshot_persistence_value"

    create_table(orchestrator_stub, table_name)

    # Write some data
    seq_no = perform_write(writer_stub, table_name, key, value)

    # Trigger snapshot persistence
    persist_snapshot(orchestrator_stub)

    # Verify snapshot is persisted
    assert perform_read_with_retry(reader_stub, table_name, seq_no, key, value), (
        f"Failed to read back key '{key}' after snapshot persistence"
    )
