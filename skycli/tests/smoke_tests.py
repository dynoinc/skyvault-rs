import grpc
import time
import pytest
import concurrent.futures
import google.protobuf.empty_pb2

from proto.skyvault.v1 import skyvault_pb2
from proto.skyvault.v1 import skyvault_pb2_grpc
from minikube import setup_connection

#
# Fixtures
#


def create_connection(service_name):
    """Creates and manages a connection to a skyvault service via minikube."""
    try:
        channel = setup_connection(f"skyvault-{service_name}")
        print(f"Successfully connected to skyvault-{service_name} service")
        return channel
    except RuntimeError as e:
        pytest.fail(f"Failed to connect to {service_name} gRPC service: {e}")
    except grpc.FutureTimeoutError:
        pytest.fail(f"Failed to connect to {service_name} gRPC service: timeout")


@pytest.fixture(scope="session")
def connections():
    services = ["writer", "reader", "orchestrator"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(services)) as executor:
        future_to_service = {
            executor.submit(create_connection, service): service for service in services
        }
        connections = {}
        for future in concurrent.futures.as_completed(future_to_service):
            service = future_to_service[future]
            connections[service] = future.result()
    return connections


@pytest.fixture(scope="session")
def stubs(connections):
    """Creates gRPC stubs for different services."""
    return {
        "writer": skyvault_pb2_grpc.WriterServiceStub(connections["writer"]),
        "reader": skyvault_pb2_grpc.ReaderServiceStub(connections["reader"]),
        "orchestrator": skyvault_pb2_grpc.OrchestratorServiceStub(
            connections["orchestrator"]
        ),
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
        params=skyvault_pb2.JobParams(
            wal_compaction=google.protobuf.empty_pb2.Empty(),
        ),
    )
    response = stub.KickOffJob(request, timeout=20)

    # Wait for compaction job to complete with timeout
    start_time = time.time()
    while time.time() - start_time < 5:
        status_request = skyvault_pb2.GetJobStatusRequest(job_id=response.job_id)
        status_response = stub.GetJobStatus(status_request, timeout=10)
        if not status_response.status.pending:
            return status_response.status.seq_no
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
