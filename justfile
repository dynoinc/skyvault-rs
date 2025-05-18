default: check

sqlx:
    sqlx database reset -f

check:
    buf generate
    cargo fmt --all
    cargo check
    cargo clippy -- -D warnings
    RUST_BACKTRACE=1 cargo test

    helm lint charts/skyvault

    for dir in smoke-tests skycli; do pushd $dir; uv run ruff format .; popd; done
    for dir in smoke-tests skycli; do pushd $dir; uv run ruff check .; popd; done

build:
    cargo sqlx prepare
    docker build -t localhost/skyvault:dev .
    docker save --output ./target/myapp.tar localhost/skyvault:dev
    minikube image load ./target/myapp.tar
    kubectl delete pod -l app.kubernetes.io/component=skyvault

deploy:
    helm upgrade --install dev ./charts/skyvault --set deployments.dev.enabled=true

pgshell:
    kubectl exec -it $(kubectl get pods -l app.kubernetes.io/component=postgres -o jsonpath="{.items[0].metadata.name}") -- psql -U postgres -d skyvault

cli *args:
    cd skycli && uv run python main.py {{args}}

smoke *args:
    cd smoke-tests && uv run pytest {{args}}
