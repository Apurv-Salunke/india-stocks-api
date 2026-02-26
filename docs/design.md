# Indian Stocks API - Technical Design Document

> **Package Name**: `india_stocks_api`
>
> > **Version**: 2.0
> > **Last Updated**: January 2026

---

## 1. Overview

### 1.1 Objective

A standalone, PyPI-installable Python package for programmatic trading on Indian stock exchanges. The package wraps broker APIs (Zerodha, Angel One, Fyers, etc.) with a unified, type-safe interface.

### 1.2 Goals

- **Type Safety**: All inputs use Enums to prevent string typos.
- **Domain-Driven**: Trade using `Equity`, `Future`, `Option` objects—not cryptic symbol strings.
- **Broker Agnostic**: Write once, switch brokers by changing one line.
- **Standalone**: No Flask, UI, or external server dependencies.

### 1.3 Non-Goals (Out of Scope for v1.0)

- WebSocket streaming (future enhancement)
- Multi-account management
- Backtesting engine

---

## 2. Architecture

### 2.1 Package Structure

```text
india_stocks_api/
├── __init__.py              # Public exports
├── constants.py             # Enums (OrderType, TransactionType, etc.)
├── instruments/             # Domain objects & DB
│   ├── models.py            # Equity, Future, Option, Index classes
│   ├── resolver.py          # Token resolution logic
│   └── database.py          # SQLite master contract DB
├── brokers/                 # Broker-specific adapters
│   ├── base.py              # BaseBroker abstract class
│   ├── zerodha.py           # Zerodha implementation
│   ├── angel.py             # Angel One implementation
│   └── ...
└── internal/                # Ported OpenAlgo code (hidden)
    ├── context.py           # Shim for dependency injection
    └── zerodha/             # Raw broker logic
```

### 2.2 The "Shim" Pattern

The package reuses battle-tested code from OpenAlgo's `broker/` directory. To decouple from OpenAlgo's Flask dependencies, we use a "Shim" module that replaces internal imports.

| Original Import                   | Shimmed Replacement                                  |
| --------------------------------- | ---------------------------------------------------- |
| `database.token_db.get_br_symbol` | `india_stocks_api.internal.context.get_br_symbol`    |
| `utils.httpx_client.get_client`   | `india_stocks_api.internal.context.get_httpx_client` |

This allows minimal changes to the ported code while maintaining upgradeability.

---

## 3. Domain Objects

### 3.1 Instrument Hierarchy

```python
from dataclasses import dataclass
from datetime import date
from enum import Enum

class OptionType(str, Enum):
    CE = "CE"
    PE = "PE"

@dataclass(slots=True, frozen=True)
class Equity:
    symbol: str                    # "RELIANCE"
    exchange: str = "NSE"

@dataclass(slots=True, frozen=True)
class Index:
    """Non-tradable. Used for quotes only."""
    symbol: str                    # "NIFTY 50"
    exchange: str = "NSE"

@dataclass(slots=True, frozen=True)
class Future:
    symbol: str                    # "NIFTY"
    expiry: date
    exchange: str = "NFO"

@dataclass(slots=True, frozen=True)
class Option:
    symbol: str                    # "BANKNIFTY"
    expiry: date
    strike: float
    opt_type: OptionType
    exchange: str = "NFO"
```

### 3.2 Design Rationale

- **Enforced Fields**: `Option` requires `strike` and `expiry`. The constructor fails if missing.
- **Memory Efficient**: `slots=True` reduces RAM by ~40% for 100K+ instruments.
- **Hashable**: `frozen=True` allows use in sets/dicts.

---

## 4. Enums & Type Safety

```python
from enum import Enum

class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"
    SLM = "SL-M"

class TransactionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class ProductType(str, Enum):
    INTRADAY = "MIS"
    DELIVERY = "CNC"
    CARRYFORWARD = "NRML"
```

**Internal Translation**: Each broker adapter maps these standard Enums to broker-specific strings.

---

## 5. Broker Adapters

### 5.1 Architecture: Thin Wrappers Over Ported Code

The broker adapters are **thin wrappers** that delegate to the actual OpenAlgo code (ported into `internal/`). We do NOT rewrite the broker logic—we reuse it.

```text
User Code
    ↓
brokers/zerodha.py      ← Thin Wrapper (Domain Objects → dict)
    ↓
internal/zerodha/api/   ← Ported OpenAlgo Code (unchanged logic)
    ↓
internal/context.py     ← Shim (replaces database/utils imports)
```

### 5.2 Base Class with Auto-Registration

```python
# brokers/base.py
class BaseBroker:
    _registry = {}

    def __init_subclass__(cls, broker_name: str = None, **kwargs):
        super().__init_subclass__(**kwargs)
        if broker_name:
            BaseBroker._registry[broker_name] = cls

    # Abstract methods - all brokers must implement
    def place_order(self, instrument, transaction_type, quantity, **kwargs): ...
    def get_quote(self, instrument): ...
    def get_positions(self): ...
```

### 5.3 Zerodha Adapter (Thin Wrapper Example)

```python
# brokers/zerodha.py
from .base import BaseBroker

# Import PORTED OpenAlgo code (lives in internal/)
from ..internal.zerodha.api.order_api import place_order_api, get_positions
from ..internal import context


class Zerodha(BaseBroker, broker_name="zerodha"):

    def __init__(self, api_key: str, access_token: str):
        self.api_key = api_key
        self.access_token = access_token

        # Configure the Shim so ported code uses OUR credentials
        context.set_auth_token(access_token)
        context.set_api_key(api_key)

    def place_order(self, instrument, transaction_type, quantity, **kwargs):
        # 1. Resolve Domain Object → token info
        token_info = self._resolve_instrument(instrument)

        # 2. Build dict in format OpenAlgo code expects
        data = {
            "symbol": token_info["symbol"],
            "exchange": token_info["exchange"],
            "action": transaction_type.value,
            "quantity": str(quantity),
            "pricetype": kwargs.get("order_type", "MARKET"),
            "product": kwargs.get("product_type", "MIS"),
            "price": str(kwargs.get("price", 0)),
        }

        # 3. Call the ACTUAL ported OpenAlgo code
        res, response, orderid = place_order_api(data, self.access_token)

        return {"order_id": orderid, "response": response}

    # Zerodha-specific method (not in BaseBroker)
    def place_gtt(self, instrument, trigger_price, **kwargs):
        from ..internal.zerodha.api.gtt_api import place_gtt_api
        # ... delegate to ported code
```

### 5.4 How the Shim Works

The ported code in `internal/zerodha/api/order_api.py` originally had:

```python
# ORIGINAL (in OpenAlgo)
from database.auth_db import get_auth_token
from utils.httpx_client import get_httpx_client
```

After porting, imports are replaced to:

```python
# PORTED (in india_stocks_api/internal/)
from india_stocks_api.internal.context import get_auth_token
from india_stocks_api.internal.context import get_httpx_client
```

The `context.py` Shim provides these functions, returning credentials we set during `__init__`.

### 5.5 Capability Visibility

- `Zerodha` class has `place_gtt()` method (Zerodha supports GTT).
- `AngelOne` class does **not** have `place_gtt()` (Angel doesn't support it).
- IDE autocomplete shows only valid methods per broker.

---

## 6. API Design

### 6.1 Place Order

```python
def place_order(
    self,
    instrument: Equity | Future | Option,  # NOT Index
    transaction_type: TransactionType,
    quantity: int,
    order_type: OrderType = OrderType.MARKET,
    product_type: ProductType = ProductType.INTRADAY,
    price: float = 0.0,
    trigger_price: float = 0.0,
    **kwargs
) -> OrderResponse
```

**Validation**:

- If `instrument` is `Index`, raises `TypeError`.
- Resolves token from internal DB using instrument attributes.

### 6.2 Usage Examples

```python
from india_stocks_api import Zerodha, Equity, Option, OptionType, TransactionType
from datetime import date

# Initialize broker
client = Zerodha(api_key="...", access_token="...")

# Trade Equity
client.place_order(
    Equity("RELIANCE"),
    TransactionType.BUY,
    quantity=10
)

# Trade Option
nifty_call = Option("NIFTY", date(2024, 1, 25), 21500, OptionType.CE)
client.place_order(nifty_call, TransactionType.SELL, quantity=50)
```

---

## 7. Instrument Resolution

### 7.1 The Master Database

A local SQLite database stores all tradable instruments with pre-computed metadata.

| Column          | Type    | Purpose                        |
| --------------- | ------- | ------------------------------ |
| `token`         | String  | Broker-specific token ID       |
| `symbol`        | String  | Standardized name (NSE naming) |
| `expiry_date`   | Date    | ISO format `YYYY-MM-DD`        |
| `strike_price`  | Float   | Strike for options             |
| `option_type`   | String  | CE/PE                          |
| `is_weekly`     | Boolean | Pre-computed flag              |
| `tradingsymbol` | String  | Broker-specific symbol for API |

### 7.2 Resolution Flow

1. User creates `Option("NIFTY", date(2024,1,25), 21500, OptionType.CE)`
2. Resolver queries: `SELECT token, tradingsymbol FROM instruments WHERE symbol='NIFTY' AND expiry='2024-01-25' AND strike=21500 AND opt_type='CE'`
3. Returns the broker-specific token for the API call.

### 7.3 Edge Case: Ambiguity

If multiple contracts match (rare), the library raises `AmbiguousContractError` instead of guessing.

---

## 8. Upstream Sync (Automation)

### 8.1 Goal

Keep broker logic updated when OpenAlgo releases new versions.

### 8.2 GitHub Action Workflow

- **Trigger**: Daily schedule or on new OpenAlgo release.
- **Process**:
  1. Clone upstream OpenAlgo.
  2. Run extraction script (copies `broker/` with Shim replacements).
  3. If changes detected, create a Pull Request.
- **Human Review**: Developer reviews PR, runs tests, then merges.

---

## 9. Python Patterns Used

| Feature             | Pattern                 | Benefit                          |
| ------------------- | ----------------------- | -------------------------------- |
| Broker Registration | `__init_subclass__`     | Zero-boilerplate broker addition |
| Instrument Types    | `dataclass(slots=True)` | Memory efficient, hashable       |
| Resolution Logic    | `singledispatchmethod`  | Clean type-based dispatch        |
| API Contract        | `Protocol`              | Duck typing with IDE support     |
| Session Management  | Context Manager         | Auto-cleanup of connections      |

---

## 10. Future Enhancements (v2.0+)

### 10.1 Database Normalization

**Current (v1.0)**: Flat table optimized for fast token resolution.

**Future Consideration**: Normalized schema for richer queries.

```text
underlyings(id, name, exchange, lot_size, tick_size)
    └── expiries(id, underlying_id, expiry_date, is_weekly, is_monthly)
           └── contracts(token, expiry_id, strike, opt_type, tradingsymbol)
```

**When to Normalize**:

- Building option chain UI features
- Adding "smart expiry" aliases (`expiry="current_week"`)
- Supporting analytics ("show all available strikes")

**Pragmatic Middle Ground**: Add a secondary `underlyings` table for metadata (lot size, tick size) while keeping contracts flat.

### 10.2 Other Planned Features

| Feature             | Priority | Notes                                      |
| ------------------- | -------- | ------------------------------------------ |
| WebSocket Streaming | High     | Real-time tick data via callbacks          |
| Async API           | Medium   | `place_order_async()` for concurrency      |
| Multi-Account       | Low      | Single client managing multiple accounts   |
| Rate Limiting       | Medium   | Built-in throttling per broker limits      |
| Retry Logic         | Medium   | Exponential backoff for transient failures |
| Dry-Run Mode        | Low      | `paper=True` for logging without execution |

---

## 11. Summary

**Indian Stocks API** provides:

1. **Domain Objects**: `Equity`, `Future`, `Option`, `Index`
2. **Type-Safe Enums**: `OrderType`, `TransactionType`, `ProductType`
3. **Broker Adapters**: `Zerodha`, `AngelOne`, etc. with capability-specific methods
4. **Instrument-First API**: Pass objects directly to `place_order()`, not strings
5. **Fast Resolution**: SQLite-backed O(1) token lookup
6. **Maintainable**: Automated sync with upstream OpenAlgo

---

<!-- End of Design Document -->
