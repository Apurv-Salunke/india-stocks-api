# Test Suite for India Stocks API

This directory contains the test suite for the India Stocks API project.

## Test Files

- `test_base.py` - Tests for the `BaseBroker` abstract class
- `test_constant.py` - Tests for enum classes (`OrderType`, `TransactionType`, `ProductType`, etc.)
- `test_common.py` - Tests for utility functions in `internal.utils.common`
- `test_domains.py` - Tests for instrument domain classes (`Equity`, `Index`, `Future`, `Option`)
- `test_httpx_client.py` - Tests for HTTP client utilities
- `conftest.py` - Shared pytest fixtures and configuration

## Running Tests

### Install Dependencies

First, install the development dependencies:

```bash
pip install -r requirements-dev.txt
```

### Run All Tests

```bash
pytest tests/
```

### Run Specific Test File

```bash
pytest tests/test_base.py
```

### Run with Coverage

```bash
pytest tests/ --cov=india_stocks_api --cov-report=html
```

### Run with Verbose Output

```bash
pytest tests/ -v
```

## Test Structure

Each test file follows pytest conventions:
- Test classes are prefixed with `Test`
- Test methods are prefixed with `test_`
- Tests use descriptive names that explain what they're testing

## Fixtures

Shared fixtures are available in `conftest.py`:
- `temp_dir` - Temporary directory for file operations
- `sample_order_details` - Sample order data for testing
- `sample_positions` - Sample positions data for testing
