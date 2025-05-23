import sys
import subprocess
import re
import grpc
import fire

from google.protobuf import descriptor_pool, message_factory
from google.protobuf.json_format import ParseDict

sys.path.append("../gen")

from skyvault.v1 import skyvault_pb2_grpc


def setup_connection(url: str) -> grpc.Channel:
    if url:
        channel = grpc.insecure_channel(url)
        grpc.channel_ready_future(channel).result(timeout=10)
        return channel

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
        raise RuntimeError("Unable to open tunnel")

    channel = grpc.insecure_channel(service_url)
    grpc.channel_ready_future(channel).result(timeout=10)
    return channel


SERVICE_CONFIG = {
    "orchestrator": skyvault_pb2_grpc.OrchestratorServiceStub,
    "writer": skyvault_pb2_grpc.WriterServiceStub,
    "reader": skyvault_pb2_grpc.ReaderServiceStub,
}


def build_service(stub_cls, address: str):
    channel = setup_connection(address)
    stub = stub_cls(channel)

    pool = descriptor_pool.Default()
    factory = message_factory.MessageFactory(pool)

    svc_name = stub_cls.__name__.replace("Stub", "")
    full_svc_name = f"skyvault.v1.{svc_name}"
    svc_desc = pool.FindServiceByName(full_svc_name)

    methods = {}
    for m in svc_desc.methods:
        rpc_name = m.name.lower()

        def make_rpc(method_desc):
            def rpc(request: dict = None, **kwargs):
                msg = factory.GetPrototype(method_desc.input_type)()
                if request:
                    ParseDict(request, msg)
                for k, v in kwargs.items():
                    setattr(msg, k, v)
                resp = getattr(stub, method_desc.name)(msg)
                print(resp)

            rpc.__doc__ = f"{method_desc.name} → {method_desc.output_type.full_name}"
            return rpc

        methods[rpc_name] = make_rpc(m)

    return methods


def create_cli():
    return {name: build_service(stub, "") for name, stub in SERVICE_CONFIG.items()}


if __name__ == "__main__":
    fire.Fire(create_cli())
