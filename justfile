set dotenv-load

default: check

sqlx:
    sqlx database reset -f

check:
    cargo fmt --all
    cargo check --all-targets
    cargo clippy --all-targets -- -D warnings
    RUST_BACKTRACE=1 cargo test

    helm lint charts/skyvault

build:
    cargo sqlx prepare
    docker build -t localhost/skyvault:dev .
    docker save --output ./target/myapp.tar localhost/skyvault:dev
    minikube image load ./target/myapp.tar

    kubectl delete pod -l app.kubernetes.io/component=skyvault-reader
    kubectl delete pod -l app.kubernetes.io/component=skyvault-writer
    kubectl delete pod -l app.kubernetes.io/component=skyvault-orchestrator
    kubectl delete pod -l app.kubernetes.io/component=skyvault-cache

deploy:
    helm upgrade --install dev ./charts/skyvault \
        --set deployments.reader.enabled=true \
        --set deployments.writer.enabled=true \
        --set deployments.orchestrator.enabled=true \
        --set deployments.cache.enabled=true \
        --set common.env.SENTRY_DSN=$SENTRY_DSN

diff:
    helm diff upgrade --install dev ./charts/skyvault \
        --set deployments.reader.enabled=true \
        --set deployments.writer.enabled=true \
        --set deployments.orchestrator.enabled=true \
        --set deployments.cache.enabled=true \
        --set common.env.SENTRY_DSN=$SENTRY_DSN

pgshell:
    kubectl exec -it $(kubectl get pods -l app.kubernetes.io/component=postgres -o jsonpath="{.items[0].metadata.name}") -- psql -U postgres -d skyvault

smoke:
    RUST_BACKTRACE=1 cargo test --test smoke_tests -- --ignored --nocapture --test-threads=1

reset-db-dry:
    kubectl get configmap skyvault-reset-db-dry-job-spec -o jsonpath='{.data.job\.yaml}' | kubectl create -f -

reset-db:
    kubectl get configmap skyvault-reset-db-job-spec -o jsonpath='{.data.job\.yaml}' | kubectl create -f -