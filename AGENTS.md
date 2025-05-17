# Repo Guidelines for Contributors and Agents

These instructions apply to the entire repository. Follow them whenever you create
pull requests or update the code.

## Formatting and Linting

- Format Rust code before committing:
  ```bash
  cargo fmt --all
  ```
- Run the linter before committing:
  ```bash
  cargo clippy -- -D warnings
  ```

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
