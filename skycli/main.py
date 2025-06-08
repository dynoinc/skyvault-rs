import fire

from google.protobuf import descriptor_pool, message_factory
from google.protobuf.json_format import ParseDict

from minikube import setup_connection
from proto.skyvault.v1 import skyvault_pb2_grpc


SERVICE_CONFIG = {
    "orchestrator": skyvault_pb2_grpc.OrchestratorServiceStub,
    "writer": skyvault_pb2_grpc.WriterServiceStub,
    "reader": skyvault_pb2_grpc.ReaderServiceStub,
}


def build_service(service_name: str, stub_cls):
    pool = descriptor_pool.Default()
    svc_name = stub_cls.__name__.replace("Stub", "")
    full_svc_name = f"skyvault.v1.{svc_name}"
    svc_desc = pool.FindServiceByName(full_svc_name)

    methods = {}
    for m in svc_desc.methods:
        rpc_name = m.name.lower()

        def make_rpc(method_desc):
            def rpc(request: dict = None, **kwargs):
                channel = setup_connection(f"skyvault-{service_name}")
                stub = stub_cls(channel)
                msg = message_factory.GetMessageClass(method_desc.input_type)()
                if request:
                    ParseDict(request, msg)
                for k, v in kwargs.items():
                    setattr(msg, k, v)
                resp = getattr(stub, method_desc.name)(msg)
                print(resp)
                channel.close()

            rpc.__doc__ = f"{method_desc.name} → {method_desc.output_type.full_name}"
            return rpc

        methods[rpc_name] = make_rpc(m)

    return methods


def create_cli():
    return {name: build_service(name, stub) for name, stub in SERVICE_CONFIG.items()}


if __name__ == "__main__":
    fire.Fire(create_cli())
