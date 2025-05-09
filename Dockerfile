FROM rust:slim AS builder

# Install protobuf compiler
RUN apt-get update && \
    apt-get install -y protobuf-compiler libprotobuf-dev --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Download rust toolchain first
COPY rust-toolchain.toml .
RUN rustup toolchain install --profile minimal $(rustup show active-toolchain | cut -d' ' -f1)

COPY . .
ENV SQLX_OFFLINE=true
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --locked

# Runtime stage
FROM public.ecr.aws/lts/ubuntu:24.04_stable

RUN apt-get update && \
    apt-get install -y ca-certificates --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/debug/skyvault /app/skyvault
COPY --from=builder /app/target/debug/worker /app/worker

EXPOSE 50051
CMD ["/app/skyvault"]
