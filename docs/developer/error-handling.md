# Error Handling

This document covers the exception hierarchy, error codes, and error handling patterns.

---

## Exception Hierarchy

```text
ISAError (base)
│
├── AuthenticationError
│   └── SessionExpiredError
│
├── NetworkError
│
├── RateLimitError
│
├── ValidationError
│
├── BrokerError
│   └── OrderError
│
├── DataIntegrityError
│
└── DownloadError
```

All exceptions inherit from `ISAError` and include:

- Human-readable message
- Standardized error code (`ErrorCode` enum)
- Optional details dictionary

---

## Error Codes

### Ranges

| Range | Category | Description |
|-------|----------|-------------|
| 1xxx | Authentication | Login, token, session errors |
| 2xxx | Network | Connection, timeout errors |
| 3xxx | Rate Limiting | API throttling |
| 4xxx | Validation | Invalid parameters |
| 5xxx | Broker Service | Broker-side errors |
| 6xxx | Data Integrity | Data validation failures |
| 9xxx | Unknown | Catch-all errors |

### Complete Code List

```python
class ErrorCode(IntEnum):
    # Authentication (1xxx)
    AUTH_FAILED = 1000
    SESSION_EXPIRED = 1001
    INVALID_CREDENTIALS = 1002
    TOKEN_INVALID = 1003
    
    # Network (2xxx)
    NETWORK_ERROR = 2000
    TIMEOUT = 2001
    CONNECTION_REFUSED = 2002
    CONNECTION_RESET = 2003
    DOWNLOAD_FAILED = 2004
    
    # Rate Limiting (3xxx)
    RATE_LIMITED = 3000
    
    # Validation (4xxx)
    INVALID_REQUEST = 4000
    VALIDATION_ERROR = 4001
    INVALID_INSTRUMENT = 4002
    INVALID_ORDER = 4003
    INSUFFICIENT_FUNDS = 4004
    INSUFFICIENT_QUANTITY = 4005
    
    # Broker Service (5xxx)
    BROKER_ERROR = 5000
    SERVICE_UNAVAILABLE = 5001
    ORDER_REJECTED = 5002
    ORDER_NOT_FOUND = 5003
    POSITION_NOT_FOUND = 5004
    
    # Data Integrity (6xxx)
    DATA_INTEGRITY_ERROR = 6000
    DUPLICATE_TOKEN = 6001
    MISSING_REQUIRED_FIELD = 6002
    ROW_COUNT_MISMATCH = 6003
    CHECKSUM_MISMATCH = 6004
    ATOMIC_OPERATION_FAILED = 6005
    
    # Unknown (9xxx)
    UNKNOWN = 9000
    INTERNAL_ERROR = 9001
```

---

## Exception Classes

### ISAError (Base)

```python
from india_stocks_api import ISAError

try:
    broker.place_order(...)
except ISAError as e:
    print(f"Message: {e}")
    print(f"Code: {e.code}")           # ErrorCode enum
    print(f"Code name: {e.code.name}") # e.g., "ORDER_REJECTED"
    print(f"Details: {e.details}")     # Optional dict
```

### AuthenticationError

Raised when authentication fails or credentials are invalid.

```python
from india_stocks_api import AuthenticationError

try:
    broker.authenticate()
except AuthenticationError as e:
    print(f"Auth failed: {e}")
    # e.code may be AUTH_FAILED, INVALID_CREDENTIALS, TOKEN_INVALID
```

### SessionExpiredError

Subclass of `AuthenticationError`. Raised when session has expired.

```python
from india_stocks_api import SessionExpiredError

try:
    broker.get_quote(...)
except SessionExpiredError:
    # Re-authenticate and retry
    broker.authenticate()
    broker.get_quote(...)
```

### NetworkError

Raised on connection failures, timeouts, etc.

```python
from india_stocks_api import NetworkError

try:
    broker.get_quote(...)
except NetworkError as e:
    print(f"Network issue: {e}")
    # e.code may be NETWORK_ERROR, TIMEOUT, CONNECTION_REFUSED
```

### RateLimitError

Raised when API rate limits are exceeded.

```python
from india_stocks_api import RateLimitError
import time

try:
    broker.get_quote(...)
except RateLimitError:
    time.sleep(1)  # Back off
    broker.get_quote(...)  # Retry
```

### ValidationError

Raised for invalid request parameters.

```python
from india_stocks_api import ValidationError

try:
    broker.place_order(
        instrument=Index("NIFTY 50"),  # Can't trade index!
        ...
    )
except ValidationError as e:
    print(f"Invalid request: {e}")
    # e.code may be INVALID_INSTRUMENT, INVALID_ORDER
```

### BrokerError

Raised when the broker service returns an error.

```python
from india_stocks_api import BrokerError

try:
    broker.place_order(...)
except BrokerError as e:
    print(f"Broker rejected: {e}")
    if e.details:
        print(f"Broker error code: {e.details.get('errorcode')}")
```

### DataIntegrityError

Raised during instrument database operations.

```python
from india_stocks_api.exceptions import DataIntegrityError

try:
    broker._download_master_contract()
except DataIntegrityError as e:
    print(f"Data validation failed: {e}")
```

### DownloadError

Raised when downloading master contract data fails.

```python
from india_stocks_api.exceptions import DownloadError

try:
    broker._download_master_contract()
except DownloadError as e:
    print(f"Download failed: {e}")
```

---

## Error Details

The `details` dictionary provides additional context:

```python
try:
    broker.place_order(...)
except BrokerError as e:
    # e.details may contain:
    # {
    #     "raw": {...},           # Raw broker response
    #     "errorcode": "AG8001",  # Broker-specific error code
    #     "order_id": "123456",   # Context-specific info
    # }
```

---

## Error Handling Patterns

### Comprehensive Handler

```python
import logging
import time
from india_stocks_api import (
    ISAError,
    AuthenticationError,
    SessionExpiredError,
    ValidationError,
    BrokerError,
    NetworkError,
    RateLimitError,
)

logger = logging.getLogger(__name__)

def safe_operation(broker, func, *args, **kwargs):
    """Execute with comprehensive error handling."""
    try:
        return func(*args, **kwargs)
    
    except SessionExpiredError:
        # Auto-recover from expired session
        broker.authenticate()
        return func(*args, **kwargs)
    
    except RateLimitError:
        # Back off and retry
        time.sleep(1)
        return func(*args, **kwargs)
    
    except NetworkError as e:
        # Log and maybe retry
        logger.warning(f"Network error: {e}")
        raise
    
    except ValidationError as e:
        # Developer error - fix the code
        logger.error(f"Invalid parameters: {e}")
        raise
    
    except BrokerError as e:
        # Business logic error - may need user intervention
        logger.error(f"Broker error: {e}")
        raise
    
    except AuthenticationError as e:
        # Credential issue - need user action
        logger.error(f"Auth error: {e}")
        raise
```

### Retry with Backoff

```python
import time
from india_stocks_api import NetworkError, RateLimitError

def with_retry(func, max_retries=3, backoff=1.0):
    """Retry on transient errors."""
    last_error = None
    
    for attempt in range(max_retries):
        try:
            return func()
        except (NetworkError, RateLimitError) as e:
            last_error = e
            wait = backoff * (2 ** attempt)  # Exponential backoff
            time.sleep(wait)
    
    raise last_error
```

### Error Classification

```python
def classify_error(e: ISAError) -> str:
    """Classify error for handling strategy."""
    code = e.code
    
    if 1000 <= code < 2000:
        return "auth"      # Re-authenticate
    elif 2000 <= code < 3000:
        return "network"   # Retry with backoff
    elif 3000 <= code < 4000:
        return "rate"      # Back off
    elif 4000 <= code < 5000:
        return "validation"  # Fix parameters
    elif 5000 <= code < 6000:
        return "broker"    # Check broker status
    else:
        return "unknown"   # Log and alert
```

---

## Creating Exceptions (For Contributors)

### Raising with Context

```python
from india_stocks_api.exceptions import BrokerError, ErrorCode

def place_order_api(data, token):
    response = make_request(...)
    
    if not response.get("success"):
        raise BrokerError(
            f"Order failed: {response.get('message')}",
            code=ErrorCode.ORDER_REJECTED,
            details={
                "raw": response,
                "errorcode": response.get("errorcode"),
                "symbol": data.get("symbol"),
            }
        )
```

### Chaining Exceptions

```python
try:
    result = external_api_call()
except ExternalError as e:
    raise NetworkError(
        f"API call failed: {e}",
        code=ErrorCode.NETWORK_ERROR,
        details={"original": str(e)}
    ) from e
```

---

## Logging Errors

### Structured Error Logging

```python
import logging
import json

logger = logging.getLogger("india_stocks_api")

def log_error(e: ISAError, context: dict = None):
    """Log error with structured data."""
    log_data = {
        "error_type": type(e).__name__,
        "message": str(e),
        "code": e.code.name,
        "code_value": e.code.value,
    }
    
    if e.details:
        log_data["details"] = e.details
    
    if context:
        log_data["context"] = context
    
    logger.error(json.dumps(log_data))
```

---

## Testing Error Handling

### Unit Tests

```python
import pytest
from india_stocks_api import AuthenticationError, ValidationError

def test_authentication_error():
    with pytest.raises(AuthenticationError) as exc_info:
        raise AuthenticationError("Test error")
    
    assert "Test error" in str(exc_info.value)

def test_error_code():
    from india_stocks_api.exceptions import ErrorCode
    
    e = ValidationError("Invalid", code=ErrorCode.INVALID_INSTRUMENT)
    assert e.code == ErrorCode.INVALID_INSTRUMENT
```

---

## Next Steps

- [Testing Strategy](testing.md) - Test organization
- [Contributing](contributing.md) - Contribution workflow
