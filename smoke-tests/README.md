# SkyVault Smoke Tests

This directory contains smoke tests for the SkyVault service using pytest.

## Running Tests

```bash
# From the project root
just smoke
```

## Test Descriptions

1. **Simple Write and Read Test**
   - Writes a key-value pair to the database
   - Verifies the key can be read back with the correct value

2. **Write, Compact, Read Test**
   - Writes key-value pairs to the database
   - Triggers WAL compaction
   - Verifies all keys are still readable after compaction

## Adding New Tests

To add new tests:

1. Add your test functions to `test_basic.py`
2. Use the `stubs` fixture to get access to the gRPC stubs
3. Tag your tests with appropriate markers (e.g., `@pytest.mark.smoke`) 