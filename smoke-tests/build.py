import grpc_tools.protoc
import os
import sys
import pathlib
import shutil

def run_protoc():
    """Generates Python gRPC code from skyvault.proto."""
    # Path to the directory containing this script
    script_dir = pathlib.Path(__file__).parent.resolve()
    # Path to the root of the workspace
    workspace_root = script_dir.parent
    # Path to the directory containing the proto file
    proto_dir = workspace_root / "proto"
    proto_file = proto_dir / "skyvault.proto"
    # Output directory for generated code
    generated_dir = script_dir

    if not proto_file.exists():
        print(f"Error: Proto file not found at {proto_file}", file=sys.stderr)
        sys.exit(1)

    # Create __init__.py to make 'generated' a package
    (generated_dir / "__init__.py").touch()

    print(f"Generating Python code from {proto_file} into {generated_dir}...")

    # Include path for the directory containing skyvault.proto
    proto_include_proj = f"-I{workspace_root.resolve()}"
    # Include path for google/protobuf/empty.proto provided by grpcio-tools
    try:
        protobuf_include_path = pathlib.Path(grpc_tools.protoc.__file__).parent / '_proto'
        proto_include_google = f"-I{protobuf_include_path.resolve()}"
    except Exception as e:
        print(f"Error finding google protobuf include path: {e}", file=sys.stderr)
        print("Ensure grpcio-tools is installed correctly.", file=sys.stderr)
        sys.exit(1)

    command = [
        'grpc_tools.protoc',
        proto_include_proj, # To find proto/proto/skyvault.proto
        proto_include_google, # To find google/protobuf/empty.proto
        f"--python_out={generated_dir.resolve()}",
        f"--pyi_out={generated_dir.resolve()}", # Generate .pyi stub files
        f"--grpc_python_out={generated_dir.resolve()}",
        # Specify the proto file relative to the include path
        str(proto_file.relative_to(workspace_root)),
    ]

    print(f"Running command: {' '.join(command)}")
    # Use protoc.main instead of subprocess for better integration
    exit_code = grpc_tools.protoc.main(command)

    if exit_code == 0:
        print("Python gRPC code generated successfully.")
    else:
        print(f"Error: protoc command failed with exit code {exit_code}", file=sys.stderr)
        sys.exit(exit_code)

if __name__ == "__main__":
    run_protoc() 
