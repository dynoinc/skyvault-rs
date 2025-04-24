default: check

check:
    cargo fmt --all
    cargo clippy -- -D warnings
    cargo test -- --nocapture

deploy:
    podman build -t skyvault:latest .
    podman save -o ./target/skyvault.tar localhost/skyvault:latest
    minikube image load ./target/skyvault.tar
    helm upgrade --install skyvault-dev ./charts/skyvault --namespace default --create-namespace
