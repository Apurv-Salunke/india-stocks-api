# Architecture Overview

This document explains the internal architecture of the SDK for developers and contributors.

---

## System Design

### High-Level Architecture

``` text
┌─────────────────────────────────────────────────────────────┐
│                        User Code                            │
│  broker.place_order(Equity("RELIANCE"), TransactionType.BUY)│
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Broker Adapter Layer                     │
│                   (brokers/angel.py)                        │
│  • Translates domain objects → broker format                │
│  • Maps canonical enums → broker-specific values            │
│  • Normalizes responses → canonical dataclasses             │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Context Shim                           │
│                  (internal/context.py)                      │
│  • Provides auth tokens                                     │
│  • Manages HTTP client                                      │
│  • Handles symbol resolution                                │
│  • Persists sessions                                        │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   Ported Broker Internals                   │
│               (internal/angel/api/*, etc.)                  │
│  • Actual HTTP requests to broker                           │
│  • Raw response parsing                                     │
│  • WebSocket handling                                       │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                      Broker API                             │
│             (HTTP/WebSocket endpoints)                      │
└─────────────────────────────────────────────────────────────┘
```

---

## The Shim Pattern

### Problem Statement

The SDK reuses battle-tested code from OpenAlgo's broker implementations. However, OpenAlgo code depends on:

- Flask session for authentication
- Flask-SQLAlchemy for database
- Centralized utils for HTTP
- Specific logging patterns

### Solution: Context Shim

The shim (`internal/context.py`) provides replacement functions that the ported code expects:

| OpenAlgo Dependency | Shim Replacement |
|---------------------|------------------|
| `database.token_db.get_br_symbol` | `context.get_br_symbol()` |
| `utils.httpx_client.get_client` | `context.get_httpx_client()` |
| Flask session auth | `context.get_auth_token()` |
| Flask database | `context.get_instruments_db_path()` |

### Benefits

1. **Minimal Changes**: Ported code requires few modifications
2. **Upgradeability**: Easy to sync with OpenAlgo updates
3. **Isolation**: Broker logic stays tested and stable
4. **Standalone**: No Flask or server dependencies

---

## Module Responsibilities

### `/brokers/`

Public-facing broker adapters.

| File | Responsibility |
|------|----------------|
| `base.py` | Abstract base class, auto-registration, auto-provisioning |
| `angel.py` | Angel One implementation (~940 lines) |

### `/instruments/`

Domain objects and instrument resolution.

| File | Responsibility |
|------|----------------|
| `models.py` | `Equity`, `Future`, `Option`, `Index` dataclasses |
| `resolver.py` | Symbol → Token resolution logic |
| `database.py` | SQLite database access |

### `/internal/`

Hidden implementation details. Not public API.

| Directory | Responsibility |
|-----------|----------------|
| `context.py` | Shim for dependency injection |
| `angel/api/` | HTTP API calls (auth, orders, data) |
| `angel/database/` | Master contract download |
| `angel/mapping/` | Value transformations |
| `angel/streaming/` | WebSocket adapter |

### `/responses.py`

Canonical response dataclasses returned by all broker methods.

### `/constants.py`

Type-safe enums for order types, product types, etc.

### `/exceptions.py`

Exception hierarchy with error codes.

---

## Data Flow Examples

### Order Placement

```text
1. User: broker.place_order(Equity("RELIANCE"), TransactionType.BUY, 1)

2. AngelOne.place_order():
   a. _resolve_instrument(Equity("RELIANCE"))
      → Queries SQLite: symbol="RELIANCE", exchange="NSE"
      → Returns: {symbol: "RELIANCE-EQ", token: "2885", exchange: "NSE"}
   
   b. _require_auth()
      → Checks context.get_auth_token()
      → Validates session expiry
      → Returns JWT token
   
   c. Build broker payload:
      {symbol: "RELIANCE-EQ", exchange: "NSE", action: "BUY", ...}
   
   d. Call internal API:
      place_order_api(data, jwt_token)
      → HTTP POST to Angel API
      → Returns raw response
   
   e. Map to canonical:
      OrderResponse(order_id="...", status="success", message="...")

3. Return OrderResponse to user
```

### Quote Fetching

```text
1. User: broker.get_quote(Equity("SBIN"))

2. AngelOne.get_quote():
   a. _resolve_instrument(Equity("SBIN"))
      → Returns: {symbol: "SBIN-EQ", token: "3045", exchange: "NSE"}
   
   b. _require_auth()
      → Returns JWT token
   
   c. Create BrokerData instance with token
   
   d. Call internal API:
      BrokerData.get_quotes(symbol, exchange)
      → HTTP GET to Angel API
      → Returns raw JSON
   
   e. Map to canonical:
      QuoteResponse(ltp=825.40, bid=825.35, ask=825.45, ...)

3. Return QuoteResponse to user
```

---

## Auto-Provisioning

### BrokerMeta Metaclass

The `BrokerMeta` metaclass intercepts broker instantiation:

```python
class BrokerMeta(ABCMeta):
    def __call__(cls, *args, **kwargs):
        # Create instance
        instance = super().__call__(*args, **kwargs)
        
        # Auto-provision instruments
        instance._ensure_instruments_ready()
        
        return instance
```

### Database Freshness Check

```python
def _is_db_stale(db_path) -> bool:
    if not db_path.exists():
        return True
    
    mtime = db_path.stat().st_mtime
    file_date = datetime.fromtimestamp(mtime).date()
    
    return file_date < date.today()
```

### Download Flow

1. Check if `_cache/instruments.db` exists
2. Check file modification date
3. If missing or stale, call `_download_master_contract()`
4. Download raw data from broker
5. Transform and populate SQLite database

---

## Session Management

### Session Storage

Sessions stored in `_cache/sessions.json`:

```json
{
  "angel": {
    "access_token": "jwt_token_here",
    "feed_token": "feed_token_here",
    "api_key": "xxx",
    "client_code": "xxx",
    "authenticated_at": "2024-01-15T09:30:00",
    "expires_at": "2024-01-16T00:00:00"
  }
}
```

### What Is NOT Stored

- Password (PIN)
- TOTP secret
- Any sensitive credentials

### Session Lifecycle

```text
authenticate()
    ↓
Generate TOTP → Send to broker → Receive tokens
    ↓
Store in memory (context.set_auth_token)
    ↓
Persist to _cache/sessions.json
    ↓
Valid until midnight IST
    ↓
On expiry: SessionExpiredError → Re-authenticate
```

---

## Instrument Resolution

### Resolution Process

```text
Input: Equity("RELIANCE")
    ↓
Build query: symbol="RELIANCE", exchange="NSE"
    ↓
Query SQLite:
  SELECT symbol, token, tradingsymbol, exchange
  FROM instruments
  WHERE symbol='RELIANCE' AND exchange='NSE'
    ↓
Return: {
  symbol: "RELIANCE-EQ",
  token: "2885",
  tradingsymbol: "RELIANCE-EQ",
  exchange: "NSE"
}
```

### F&O Resolution

For options/futures, additional fields are matched:

```text
Input: Option("NIFTY", date(2024,12,26), 22000, OptionType.CE)
    ↓
Query with: symbol, expiry, strike, option_type
    ↓
Returns unique contract token
```

---

## WebSocket Streaming

### Architecture

```text
┌───────────────────────────────────────────────────────┐
│                   AngelOne Adapter                    │
│  • subscribe() - buffer subscriptions                 │
│  • start_streaming() - connect WebSocket              │
│  • on_tick callback - user receives ticks             │
└─────────────────────┬─────────────────────────────────┘
                      │
                      ▼
┌───────────────────────────────────────────────────────┐
│                SmartWebSocketV2                       │
│  (internal/angel/streaming/)                          │
│  • Connection management                              │
│  • Automatic reconnection                             │
│  • Binary frame parsing                               │
└─────────────────────┬─────────────────────────────────┘
                      │
                      ▼
┌───────────────────────────────────────────────────────┐
│              Angel One WebSocket Server               │
└───────────────────────────────────────────────────────┘
```

### Subscription Buffering

Subscriptions can be added before connection is established:

```python
# These are buffered
broker.subscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)
broker.subscribe([Equity("TCS")], mode=StreamMode.LTP)

# On connect, buffered subscriptions are sent
broker.start_streaming()
```

---

## Error Handling Architecture

### Exception Hierarchy

```text
ISAError (base)
├── AuthenticationError
│   └── SessionExpiredError
├── NetworkError
├── RateLimitError
├── ValidationError
├── BrokerError
│   └── OrderError
├── DataIntegrityError
└── DownloadError
```

### Error Code Ranges

| Range | Category |
|-------|----------|
| 1xxx | Authentication |
| 2xxx | Network/Transport |
| 3xxx | Rate Limiting |
| 4xxx | Validation |
| 5xxx | Broker Service |
| 6xxx | Data Integrity |
| 9xxx | Unknown/Internal |

---

## Next Steps

- [Broker Adapter Guide](broker-adapter-guide.md) - Adding new brokers
- [Instrument System](instrument-system.md) - Database internals
- [Session & Auth](session-auth.md) - Token management details
