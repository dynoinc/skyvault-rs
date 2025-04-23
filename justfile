# Format, lint, and test the codebase
check:
    cargo fmt --all
    cargo clippy -- -D warnings
    cargo test -- --nocapture
