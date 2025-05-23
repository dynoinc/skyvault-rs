# Skyvault CLI and Testing Tools

This package provides both a command-line interface and testing tools for interacting with the Skyvault service.

## Features

- Command-line interface for interacting with Skyvault services (orchestrator, writer, reader)
- Smoke tests for validating Skyvault service functionality
- Helper functions for common Skyvault operations

## Installation

The package requires Python 3.13 or later. Install using your preferred Python package manager:

```bash
# Using uv (recommended)
uv pip install .

# Using pip
pip install .
```

## Usage

### CLI

The CLI provides direct access to Skyvault services:

```bash
# Get help
python -m skycli --help

# Example: Create a table
python -m skycli orchestrator create_table --config.table_name=my_table
```

### Running Tests

The package includes smoke tests to validate Skyvault service functionality:

```bash
# Run all tests
pytest

# Run only smoke tests
pytest -m smoke

# Run tests with detailed output
RUST_BACKTRACE=1 pytest -v
```

## Development

This package uses `uv` for dependency management and `ruff` for linting.

### Setup Development Environment

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies including dev tools
uv pip install -e ".[dev]"
```

### Code Style

The codebase follows PEP 8 guidelines and uses `ruff` for linting:

```bash
# Run linter
ruff check .
```
