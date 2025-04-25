FROM rust:latest as builder

# Install protobuf compiler and dependencies for sccache
RUN apt-get update && apt-get install -y protobuf-compiler pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Download rust toolchain first
COPY rust-toolchain.toml ./
RUN rustup show

# Create a fake project for caching dependencies
RUN mkdir -p src bin proto
RUN echo "fn main() {println!(\"fake\")}" > src/lib.rs
RUN echo "fn main() {println!(\"fake\")}" > bin/main.rs
RUN echo "syntax = \"proto3\"; package skyvault;" > proto/skyvault.proto
COPY Cargo.toml Cargo.lock build.rs .rustfmt.toml ./
RUN cargo build

# Build the actual project
# Delete the fake build artifacts
RUN rm -rf src bin proto target/debug/deps/skyvault*
COPY . .
RUN cargo build

# Runtime stage
FROM public.ecr.aws/lts/ubuntu:24.04_stable

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/debug/skyvault /app/skyvault

EXPOSE 50051
CMD ["/app/skyvault"]
