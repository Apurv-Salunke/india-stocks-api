# Testing Strategy

This document covers the test organization, writing tests, and running the test suite.

---

## Test Organization

```
tests/
├── __init__.py
├── conftest.py           # Shared fixtures
├── unit/                 # Fast, isolated tests
│   ├── __init__.py
│   ├── test_angel_authenticate.py
│   ├── test_auth_api.py
│   ├── test_context_session.py
│   └── ...
├── integration/          # Live broker tests
│   ├── __init__.py
│   ├── test_angel_auth_live.py
│   ├── test_data_methods_live.py
│   └── test_streaming_live.py
└── scripts/              # Manual/exploratory tests
    ├── test_trading.py
    └── ...
```

---

## Test Types

### Unit Tests

Fast, isolated tests that don't require network access or credentials.

**Characteristics:**
- Mock external dependencies
- Run in milliseconds
- Deterministic (no randomness)
- No side effects

**Location:** `tests/unit/`

**Run:**
```bash
poetry run pytest tests/unit/ -v
```

### Integration Tests

Tests that require live broker credentials and network access.

**Characteristics:**
- Use real broker APIs
- Require credentials in environment
- May be slow
- May have rate limit constraints

**Location:** `tests/integration/`

**Run:**
```bash
# Requires .env with broker credentials
poetry run pytest tests/integration/ -v
```

### Scripts

Manual tests for development and debugging.

**Location:** `tests/scripts/`

**Run:**
```bash
poetry run python tests/scripts/test_trading.py
```

---

## Running Tests

### All Unit Tests

```bash
poetry run pytest tests/unit/ -v
```

### Single Test File

```bash
poetry run pytest tests/unit/test_auth_api.py -v
```

### Single Test Function

```bash
poetry run pytest tests/unit/test_auth_api.py::test_authenticate_success -v
```

### Exclude Integration Tests

```bash
poetry run pytest -m "not integration"
```

### With Coverage

```bash
poetry run pytest tests/unit/ --cov=india_stocks_api --cov-report=html
```

---

## Pytest Markers

### Integration Marker

Tests requiring live credentials are marked with `@pytest.mark.integration`:

```python
# tests/integration/test_angel_auth_live.py

import pytest

@pytest.mark.integration
def test_live_authentication():
    """Requires ANGEL_* env vars."""
    broker = AngelOne(...)
    assert broker.authenticate()
```

### Skip Conditions

```python
import pytest
import os

@pytest.mark.skipif(
    not os.getenv("ANGEL_API_KEY"),
    reason="Requires Angel One credentials"
)
def test_with_credentials():
    pass
```

---

## Fixtures

### Shared Fixtures (conftest.py)

```python
# tests/conftest.py

import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_broker():
    """Create a mock broker for testing."""
    broker = MagicMock()
    broker.authenticate.return_value = True
    return broker

@pytest.fixture
def sample_equity():
    """Create a test equity instrument."""
    from india_stocks_api.instruments import Equity
    return Equity("TESTSTOCK", exchange="NSE")

@pytest.fixture
def sample_quote_response():
    """Create a sample QuoteResponse."""
    from india_stocks_api.responses import QuoteResponse
    return QuoteResponse(
        ltp=100.0,
        bid=99.5,
        ask=100.5,
        open=98.0,
        high=102.0,
        low=97.0,
        prev_close=99.0,
        volume=1000000,
        oi=0
    )
```

### Environment Fixtures

```python
# tests/conftest.py

import os
import pytest

@pytest.fixture
def live_broker():
    """Create authenticated broker for integration tests."""
    from india_stocks_api.brokers import AngelOne
    
    api_key = os.getenv("ANGEL_API_KEY")
    client_code = os.getenv("ANGEL_CLIENT_ID")
    password = os.getenv("ANGEL_PIN")
    totp_key = os.getenv("ANGEL_TOTP_SECRET")
    
    if not all([api_key, client_code, password, totp_key]):
        pytest.skip("Missing credentials")
    
    broker = AngelOne(
        api_key=api_key,
        client_code=client_code,
        password=password,
        totp_key=totp_key
    )
    broker.authenticate()
    return broker
```

---

## Writing Unit Tests

### Testing Response Mapping

```python
# tests/unit/test_data_responses.py

from india_stocks_api.brokers.angel import AngelOne
from india_stocks_api.responses import QuoteResponse

def test_map_quote_response():
    """Test raw API response mapping to canonical QuoteResponse."""
    broker = AngelOne.__new__(AngelOne)  # Skip __init__
    
    raw_response = {
        "ltp": "2845.50",
        "bid": "2845.35",
        "ask": "2845.65",
        "open": "2830.00",
        "high": "2860.10",
        "low": "2825.00",
        "close": "2840.00",
        "volume": "12345678",
        "oi": "0",
    }
    
    result = broker._map_quote_response(raw_response)
    
    assert isinstance(result, QuoteResponse)
    assert result.ltp == 2845.50
    assert result.bid == 2845.35
    assert result.volume == 12345678
```

### Testing with Mocks

```python
# tests/unit/test_angel_authenticate.py

from unittest.mock import patch, MagicMock
import pytest
from india_stocks_api.brokers import AngelOne
from india_stocks_api import AuthenticationError

def test_authenticate_success():
    """Test successful authentication."""
    with patch('india_stocks_api.brokers.angel.authenticate_broker') as mock_auth:
        mock_auth.return_value = ("jwt_token", "feed_token", None)
        
        broker = AngelOne(
            api_key="test_key",
            client_code="test_client",
            password="1234",
            totp_key="TESTTOTPKEY"
        )
        
        result = broker.authenticate()
        
        assert result is True
        mock_auth.assert_called_once()

def test_authenticate_failure():
    """Test authentication failure handling."""
    with patch('india_stocks_api.brokers.angel.authenticate_broker') as mock_auth:
        mock_auth.return_value = (None, None, "Invalid credentials")
        
        broker = AngelOne(
            api_key="test_key",
            client_code="test_client",
            password="1234",
            totp_key="TESTTOTPKEY"
        )
        
        with pytest.raises(AuthenticationError):
            broker.authenticate()
```

### Testing Exceptions

```python
# tests/unit/test_exceptions.py

import pytest
from india_stocks_api import (
    ISAError,
    AuthenticationError,
    SessionExpiredError,
    ValidationError,
)
from india_stocks_api.exceptions import ErrorCode

def test_exception_hierarchy():
    """Verify exception inheritance."""
    assert issubclass(SessionExpiredError, AuthenticationError)
    assert issubclass(AuthenticationError, ISAError)

def test_error_code():
    """Verify error codes are attached."""
    e = ValidationError("Test", code=ErrorCode.INVALID_INSTRUMENT)
    assert e.code == ErrorCode.INVALID_INSTRUMENT
    assert e.code.value == 4002

def test_exception_str():
    """Verify exception string format."""
    e = ValidationError("Bad input", code=ErrorCode.VALIDATION_ERROR)
    assert "Bad input" in str(e)
    assert "VALIDATION_ERROR" in str(e)
```

---

## Writing Integration Tests

### Basic Integration Test

```python
# tests/integration/test_angel_auth_live.py

import pytest
from india_stocks_api.brokers import AngelOne

@pytest.mark.integration
def test_authentication(live_broker):
    """Verify live authentication works."""
    # live_broker fixture handles authentication
    assert live_broker is not None

@pytest.mark.integration
def test_get_quote(live_broker):
    """Verify quote fetching works."""
    from india_stocks_api.instruments import Equity
    
    quote = live_broker.get_quote(Equity("SBIN"))
    
    assert quote.ltp > 0
    assert quote.volume >= 0
```

### Rate Limit Handling

```python
# tests/integration/test_data_methods_live.py

import pytest
import time

@pytest.mark.integration
def test_multiple_quotes(live_broker):
    """Test multiple quote requests with rate limiting."""
    from india_stocks_api.instruments import Equity
    
    symbols = ["RELIANCE", "TCS", "INFY"]
    
    for symbol in symbols:
        quote = live_broker.get_quote(Equity(symbol))
        assert quote.ltp > 0
        time.sleep(1)  # Rate limit: 1 req/sec
```

---

## Mocking Best Practices

### Mock at the Right Level

```python
# Good: Mock internal API function
with patch('india_stocks_api.brokers.angel.place_order_api') as mock:
    mock.return_value = (True, {"status": "success"}, "12345")
    response = broker.place_order(...)

# Avoid: Mock too deep (httpx internals)
# Avoid: Mock too high (broker method itself)
```

### Use respx for HTTP Mocking

```python
import respx
import httpx

@respx.mock
def test_api_call():
    respx.get("https://api.broker.com/quote").mock(
        return_value=httpx.Response(200, json={"ltp": 100.0})
    )
    
    # Test code that makes HTTP request
```

---

## Test Data

### Use Realistic Data

```python
# Good: Realistic test data
SAMPLE_QUOTE = {
    "ltp": "2845.50",
    "bid": "2845.35",
    "ask": "2845.65",
    "volume": "12345678",
}

# Avoid: Arbitrary values
SAMPLE_QUOTE = {
    "ltp": "1",
    "volume": "1",
}
```

### Parameterized Tests

```python
import pytest

@pytest.mark.parametrize("exchange,expected", [
    ("NSE", "NSE"),
    ("BSE", "BSE"),
    ("NFO", "NFO"),
])
def test_exchange_handling(exchange, expected):
    from india_stocks_api.instruments import Equity
    
    equity = Equity("TEST", exchange=exchange)
    assert equity.exchange == expected
```

---

## CI Integration

### pytest.ini Configuration

```ini
# pytest.ini or pyproject.toml

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "integration: requires live broker credentials",
]
```

### CI Workflow

```yaml
# .github/workflows/ci.yml

- name: Run unit tests
  run: poetry run pytest tests/unit/ -v

# Integration tests run separately with secrets
- name: Run integration tests
  if: github.event_name == 'push'
  env:
    ANGEL_API_KEY: ${{ secrets.ANGEL_API_KEY }}
    # ...
  run: poetry run pytest tests/integration/ -v
```

---

## Common Pitfalls

### Avoid Test Pollution

```python
# Bad: Modifies global state
def test_set_token():
    from india_stocks_api.internal import context
    context.set_auth_token("test")
    # Affects other tests!

# Good: Clean up after test
def test_set_token():
    from india_stocks_api.internal import context
    original = context.get_auth_token()
    try:
        context.set_auth_token("test")
        # Test code
    finally:
        context.set_auth_token(original)
```

### Time-Dependent Tests

```python
# Bad: Depends on current time
def test_session_expiry():
    # May fail at different times of day
    pass

# Good: Mock time
from unittest.mock import patch
from datetime import datetime

def test_session_expiry():
    with patch('india_stocks_api.brokers.base.datetime') as mock_dt:
        mock_dt.now.return_value = datetime(2024, 1, 15, 12, 0, 0)
        # Test code
```

---

## Next Steps

- [Contributing](contributing.md) - Contribution workflow
- [Error Handling](error-handling.md) - Exception patterns
