default: check

check:
    cargo fmt --all
    cargo clippy -- -D warnings
    cargo test -- --nocapture
