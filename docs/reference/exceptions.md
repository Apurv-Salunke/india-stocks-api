# Exceptions Reference

All exceptions inherit from `ISAError` and include standardized error codes.

---

## Import

```python
from india_stocks_api import (
    ISAError,
    AuthenticationError,
    SessionExpiredError,
    NetworkError,
    RateLimitError,
    ValidationError,
    BrokerError,
    OrderError,
    DataIntegrityError,
    DownloadError,
)
from india_stocks_api.exceptions import ErrorCode
```

---

## Exception Hierarchy

```text
Exception
└── ISAError                    # Base exception
    ├── AuthenticationError     # Auth failures
    │   └── SessionExpiredError # Session expired
    ├── NetworkError           # Connection issues
    ├── RateLimitError         # Rate limits
    ├── ValidationError        # Invalid input
    ├── BrokerError            # Broker service errors
    ├── OrderError             # Order failures
    ├── DataIntegrityError     # Data validation
    └── DownloadError          # Download failures
```

---

## ISAError (Base)

Base exception for all india-stocks-api errors.

```python
class ISAError(Exception):
    code: ErrorCode
    details: dict[str, Any] | None
```

### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `code` | `ErrorCode` | Standardized error code |
| `details` | `dict \| None` | Additional context (broker response) |

### Example

```python
try:
    broker.place_order(...)
except ISAError as e:
    print(f"Error: {e}")
    print(f"Code: {e.code.name} ({e.code.value})")
    if e.details:
        print(f"Broker code: {e.details.get('errorcode')}")
```

---

## AuthenticationError

Raised when authentication fails or is missing.

```python
class AuthenticationError(ISAError):
    pass
```

**Default code:** `ErrorCode.AUTH_FAILED` (1000)

### When Raised

- Invalid credentials
- TOTP validation failed
- Missing authentication (calling methods before `authenticate()`)

### Example

```python
try:
    broker.authenticate()
except AuthenticationError as e:
    if e.code == ErrorCode.INVALID_CREDENTIALS:
        print("Check your API key and password")
    else:
        print(f"Auth failed: {e}")
```

---

## SessionExpiredError

Raised when the broker session has expired (typically past midnight IST).

```python
class SessionExpiredError(AuthenticationError):
    pass
```

**Code:** `ErrorCode.SESSION_EXPIRED` (1001)

### When Raised

- Session token expired (past midnight IST)
- Token invalidated by broker

### Example

```python
try:
    broker.get_quote(Equity("RELIANCE"))
except SessionExpiredError:
    # Re-authenticate and retry
    broker.authenticate()
    quote = broker.get_quote(Equity("RELIANCE"))
```

---

## NetworkError

Raised on transport/connection failures.

```python
class NetworkError(ISAError):
    pass
```

**Default code:** `ErrorCode.NETWORK_ERROR` (2000)

### When Raised

- Connection timeout
- DNS resolution failed
- Connection refused
- Connection reset

### Example

```python
try:
    broker.authenticate()
except NetworkError as e:
    if e.code == ErrorCode.TIMEOUT:
        print("Request timed out, retrying...")
    else:
        print(f"Network error: {e}")
```

---

## RateLimitError

Raised when API rate limits are exceeded.

```python
class RateLimitError(ISAError):
    pass
```

**Code:** `ErrorCode.RATE_LIMITED` (3000)

### When Raised

- Too many requests per second
- Exceeded daily API quota

### Example

```python
import time

try:
    for symbol in symbols:
        broker.get_quote(Equity(symbol))
except RateLimitError:
    time.sleep(1)  # Back off and retry
```

---

## ValidationError

Raised for invalid request parameters.

```python
class ValidationError(ISAError):
    pass
```

**Default code:** `ErrorCode.VALIDATION_ERROR` (4001)

### When Raised

- Invalid instrument (symbol not found)
- Invalid order parameters
- Missing required fields
- Invalid date format

### Example

```python
try:
    broker.place_order(
        instrument=Equity("INVALID_SYMBOL"),
        transaction_type=TransactionType.BUY,
        quantity=10
    )
except ValidationError as e:
    if e.code == ErrorCode.INVALID_INSTRUMENT:
        print("Symbol not found")
    elif e.code == ErrorCode.INVALID_ORDER:
        print("Invalid order parameters")
```

---

## BrokerError

Raised when the broker service returns an error.

```python
class BrokerError(ISAError):
    pass
```

**Default code:** `ErrorCode.BROKER_ERROR` (5000)

### When Raised

- Broker API returned error
- Service temporarily unavailable
- Internal broker error

### Example

```python
try:
    broker.place_order(...)
except BrokerError as e:
    if e.code == ErrorCode.SERVICE_UNAVAILABLE:
        print("Broker service is down")
    else:
        print(f"Broker error: {e}")
```

---

## OrderError

Raised when an order operation fails.

```python
class OrderError(ISAError):
    pass
```

**Default code:** `ErrorCode.ORDER_REJECTED` (5002)

### When Raised

- Order rejected by exchange
- Order not found (for modify/cancel)
- Insufficient margin

### Example

```python
try:
    broker.place_order(
        instrument=Equity("RELIANCE"),
        transaction_type=TransactionType.BUY,
        quantity=1000000  # Large quantity
    )
except OrderError as e:
    if e.code == ErrorCode.INSUFFICIENT_FUNDS:
        print("Not enough margin")
    elif e.code == ErrorCode.ORDER_REJECTED:
        print(f"Rejected: {str(e)}")
```

---

## DataIntegrityError

Raised when data integrity validation fails.

```python
class DataIntegrityError(ISAError):
    pass
```

**Default code:** `ErrorCode.VALIDATION_ERROR` (4001)

### When Raised

- Duplicate tokens in instrument data
- Missing required fields in data
- Row count mismatch after database insertion
- Invalid data format

### Example

```python
try:
    # Internally during master contract download
    pass
except DataIntegrityError as e:
    print(f"Data corruption detected: {e}")
```

---

## DownloadError

Raised when downloading master contract data fails.

```python
class DownloadError(ISAError):
    pass
```

**Default code:** `ErrorCode.NETWORK_ERROR` (2000)

### When Raised

- Failed to download master contract
- Invalid response format from broker
- Checksum validation failed

---

## ErrorCode Enum

Standardized error codes organized by category.

```python
class ErrorCode(IntEnum):
    # Authentication (1xxx)
    AUTH_FAILED = 1000
    SESSION_EXPIRED = 1001
    INVALID_CREDENTIALS = 1002
    TOKEN_INVALID = 1003

    # Network/Transport (2xxx)
    NETWORK_ERROR = 2000
    TIMEOUT = 2001
    CONNECTION_REFUSED = 2002
    CONNECTION_RESET = 2003
    DOWNLOAD_FAILED = 2004

    # Rate Limiting (3xxx)
    RATE_LIMITED = 3000

    # Invalid Request (4xxx)
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

    # Unknown/Internal (9xxx)
    UNKNOWN = 9000
    INTERNAL_ERROR = 9001
```

### Code Ranges

| Range | Category | Description |
|-------|----------|-------------|
| 1xxx | Authentication | Login, session, credentials |
| 2xxx | Network | HTTP, WebSocket, timeouts |
| 3xxx | Rate Limiting | API throttling |
| 4xxx | Validation | Invalid input, parameters |
| 5xxx | Broker | Broker service errors |
| 6xxx | Data | Data integrity issues |
| 9xxx | Unknown | Uncategorized errors |

---

## Error Handling Patterns

### Catch Specific First

```python
try:
    broker.place_order(...)
except SessionExpiredError:
    broker.authenticate()
    # Retry
except AuthenticationError:
    print("Check credentials")
except OrderError as e:
    print(f"Order failed: {e}")
except ISAError as e:
    print(f"Unexpected error: {e}")
```

### Check Error Code

```python
try:
    broker.get_quote(instrument)
except ISAError as e:
    if e.code == ErrorCode.INVALID_INSTRUMENT:
        print("Symbol not found")
    elif e.code == ErrorCode.SESSION_EXPIRED:
        broker.authenticate()
    elif e.code.value >= 5000:
        print("Broker-side issue")
```

### Access Broker Details

```python
except ISAError as e:
    if e.details:
        broker_code = e.details.get("errorcode")
        broker_msg = e.details.get("message")
        print(f"Broker error {broker_code}: {broker_msg}")
```

---

## See Also

- [Error Handling (Developer)](../developer/error-handling.md) - Creating custom exceptions
- [Production Guide](../user/production-guide.md) - Error recovery patterns
