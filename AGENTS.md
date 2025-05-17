# Repo Guidelines for Contributors and Agents

These instructions apply to the entire repository. Follow them whenever you create
pull requests or update the code.

## Formatting and Linting

- If you modify any `.proto` files under `proto/`, run `buf generate` to update
  the generated code in `gen/`.
- Format Rust code before committing:
  ```bash
  cargo fmt --all
  ```
- Run the linter before committing:
  ```bash
  cargo clippy -- -D warnings
  ```
- When editing Python code in `smoke-tests/` or `skycli/`, format and lint each
  directory with ruff:
  ```bash
  uv run ruff format .
  uv run ruff check .
  ```
- For Helm chart changes, ensure `helm lint charts/skyvault` succeeds.

## Tests

Run the full test suite locally before opening a pull request:
```bash
RUST_BACKTRACE=1 cargo test
```

## Commit Style

Use the [Conventional Commits](https://www.conventionalcommits.org/) style for
commit messages, for example `feat: add new feature` or `fix: correct issue`.

## Pull Requests

Include a short summary of your changes in the PR description and mention that
you ran the formatting, lint, and test commands listed above.
