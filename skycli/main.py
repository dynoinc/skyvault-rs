from contextlib import contextmanager
from typing import Generator
import typer
import sys
import subprocess
import re
import grpc

sys.path.append("../gen")

from skyvault.v1 import skyvault_pb2
from skyvault.v1 import skyvault_pb2_grpc

app = typer.Typer()


@contextmanager
def setup_connection[T](url: str, stub_class: type[T]) -> Generator[T, None, None]:
    if url:
        channel = grpc.insecure_channel(url)
        try:
            grpc.channel_ready_future(channel).result(timeout=10)
        except grpc.FutureTimeoutError:
            typer.fail("Failed to connect to gRPC service")
        yield stub_class(channel)
        return

    command = [
        "minikube",
        "service",
        "skyvault-dev",
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
        typer.fail(f"Failed to get service URL. Got: {line}")

    # Connect to the service
    channel = grpc.insecure_channel(service_url)
    try:
        grpc.channel_ready_future(channel).result(timeout=10)
    except grpc.FutureTimeoutError:
        typer.fail("Failed to connect to gRPC service")

    yield stub_class(channel)


tables_app = typer.Typer()
app.add_typer(tables_app, name="tables")


@tables_app.command()
def list(
    url: str = typer.Option(
        default="", help="The URL of the Skyvault orchestrator service"
    ),
):
    with setup_connection(
        url, skyvault_pb2_grpc.OrchestratorServiceStub
    ) as orchestrator_stub:
        response = orchestrator_stub.ListTables(skyvault_pb2.ListTablesRequest())
        if not response.table_names:
            typer.echo("No tables found")
            return

        typer.echo("Tables:")
        for table_name in response.table_names:
            typer.echo(f"  - {table_name}")


@tables_app.command()
def get(
    table_name: str,
    url: str = typer.Option(
        default="", help="The URL of the Skyvault orchestrator service"
    ),
):
    with setup_connection(
        url, skyvault_pb2_grpc.OrchestratorServiceStub
    ) as orchestrator_stub:
        response = orchestrator_stub.GetTable(
            skyvault_pb2.GetTableRequest(table_name=table_name)
        )
        print(response)


snapshot_app = typer.Typer()
app.add_typer(snapshot_app, name="snapshot")


@snapshot_app.command()
def dump(
    url: str = typer.Option(
        default="", help="The URL of the Skyvault orchestrator service"
    ),
):
    with setup_connection(
        url, skyvault_pb2_grpc.OrchestratorServiceStub
    ) as orchestrator_stub:
        response = orchestrator_stub.DumpSnapshot(skyvault_pb2.DumpSnapshotRequest())
        print(response)


if __name__ == "__main__":
    app()
