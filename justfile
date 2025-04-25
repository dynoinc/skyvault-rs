default: check

check:
    cargo fmt --all
    cargo clippy -- -D warnings
    cargo test -- --nocapture

deploy:
    podman build -t localhost/skyvault:dev .
    podman save --output ./target/myapp.tar localhost/skyvault:dev
    kind load image-archive ./target/myapp.tar --name kind-cluster
    helm upgrade --install skyvault-dev ./charts/skyvault
