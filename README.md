![Skyvault Logo](docs/small.png)

[![build](https://github.com/dynoinc/skyvault-rs/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/dynoinc/skyvault-rs/actions/workflows/build.yml)

Skyvault is a high-performance, scalable object-store backed key-value store.

## Architecture

![Skyvault Architecture](docs/arch.png)

## Technologies Used

| Technology                                   | Description                                                   |
|----------------------------------------------|---------------------------------------------------------------|
| [Tonic](https://github.com/hyperium/tonic)   | High performance gRPC framework for Rust                      |
| [PostgreSQL](https://www.postgresql.org/)    | Open source relational database                               |
| [SQLx](https://github.com/launchbadge/sqlx)  | Async SQL toolkit for Rust                                    |
| [MinIO](https://min.io/)                     | High performance object storage                               |
| [Kubernetes](https://kubernetes.io/)         | Container orchestration platform                              |
| [Minikube](https://minikube.sigs.k8s.io/)    | Local Kubernetes implementation for development               |
| [Helm](https://helm.sh/)                     | Package manager for Kubernetes                                |
| [Docker](https://www.docker.com/)            | Container platform                                            |
| [Cursor Editor](https://cursor.sh/)          | AI-powered code editor used for development                   |

## Prerequisites

- Rust (nightly)
- Protobuf compiler (protoc)
- Docker, k8s, minikube cluster and helm for local development
- PostgreSQL instance database for SQLx compile-time query checking
- [Just](https://github.com/casey/just) command runner

## Getting Started

1. Run `just build` to build skyvault and push container image to minikube.
2. Run `just deploy` to start everything in k8s.
3. Run `just smoke` to run some simple smoke tests against this.

## Security

See our [Security Policy](SECURITY.md) for reporting security vulnerabilities.

## License

This project is licensed under the terms in the [LICENSE](LICENSE) file.
