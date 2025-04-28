import sys
import pathlib


def _compile_proto():
    """Compiles Protocol Buffer files if the generated Python modules don't exist."""
    import grpc_tools.protoc

    # Path calculation
    script_dir = pathlib.Path(__file__).parent.resolve()  # proto/
    smoke_tests_dir = script_dir.parent  # smoke-tests/
    project_root = smoke_tests_dir.parent  # skyvault-rs/
    generated_dir = script_dir.parent

    # Include path for the directory containing skyvault.proto
    proto_include_proj = f"-I{project_root.resolve()}"

    # Include path for google/protobuf/empty.proto
    try:
        protobuf_include_path = (
            pathlib.Path(grpc_tools.protoc.__file__).parent / "_proto"
        )
        proto_include_google = f"-I{protobuf_include_path.resolve()}"
    except Exception as e:
        print(f"Error finding google protobuf include path: {e}", file=sys.stderr)
        return False

    command = [
        "grpc_tools.protoc",
        proto_include_proj,  # To find proto/skyvault.proto
        proto_include_google,  # To find google/protobuf/empty.proto
        f"--python_out={generated_dir.resolve()}",
        f"--pyi_out={generated_dir.resolve()}",  # Generate .pyi stub files
        f"--grpc_python_out={generated_dir.resolve()}",
        # Use absolute path to the proto file since it's outside smoke-tests
        "proto/skyvault.proto",
    ]

    # Compile proto files
    exit_code = grpc_tools.protoc.main(command)

    if exit_code != 0:
        print(
            f"Error: protoc command failed with exit code {exit_code}", file=sys.stderr
        )
        return False

    return True


# Compile proto on import
_proto_compiled = _compile_proto()

# Clean up namespace
del _compile_proto
del _proto_compiled
