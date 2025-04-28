default: check

check:
    cargo fmt --all
    cargo clippy -- -D warnings
    RUST_BACKTRACE=1 cargo test

build:
    cargo sqlx prepare
    docker build -t localhost/skyvault:dev .
    docker save --output ./target/myapp.tar localhost/skyvault:dev
    minikube image load ./target/myapp.tar
    kubectl delete pod -l app.kubernetes.io/component=skyvault

deploy:
    helm upgrade --install skyvault-dev ./charts/skyvault

pgshell:
    kubectl exec -it $(kubectl get pods -l app.kubernetes.io/component=postgres -o jsonpath="{.items[0].metadata.name}") -- psql -U postgres -d skyvault

smoke:
    cd smoke-tests && uv run build.py
    cd smoke-tests && uv run tests.py
