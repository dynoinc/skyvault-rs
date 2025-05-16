default: check

sqlx:
    sqlx database reset -f

check:
    cargo +nightly-2025-05-14 fmt --all
    cargo clippy -- -D warnings
    RUST_BACKTRACE=1 cargo test

    helm lint charts/skyvault

    cd smoke-tests && uv run ruff check .
    cd smoke-tests && uv run ruff format .
    cd smoke-tests && uv run python3 -m grpc_tools.protoc -I../proto --python_out=. --pyi_out=. --grpc_python_out=. ../proto/skyvault/v1/skyvault.proto

build:
    cargo sqlx prepare
    docker build -t localhost/skyvault:dev .
    docker save --output ./target/myapp.tar localhost/skyvault:dev
    minikube image load ./target/myapp.tar
    kubectl delete pod -l app.kubernetes.io/component=skyvault

deploy:
    helm upgrade --install dev ./charts/skyvault

pgshell:
    kubectl exec -it $(kubectl get pods -l app.kubernetes.io/component=postgres -o jsonpath="{.items[0].metadata.name}") -- psql -U postgres -d skyvault

smoke:
    cd smoke-tests && uv run pytest
