default: check

check:
    cargo fmt --all
    cargo clippy -- -D warnings
    cargo test -- --nocapture

build:
    podman build -t localhost/skyvault:dev .
    podman save --output ./target/myapp.tar localhost/skyvault:dev
    kind load image-archive ./target/myapp.tar --name kind-cluster
    kubectl delete pod -l app.kubernetes.io/component=skyvault

deploy:
    helm upgrade --install skyvault-dev ./charts/skyvault

pgshell:
    kubectl exec -it $(kubectl get pods -l app.kubernetes.io/component=postgres -o jsonpath="{.items[0].metadata.name}") -- psql -U postgres -d skyvault
