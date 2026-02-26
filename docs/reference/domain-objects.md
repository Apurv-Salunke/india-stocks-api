# Domain Objects Reference

Domain objects represent tradable financial instruments. All are immutable dataclasses with `slots=True` for memory efficiency.

---

## Import

```python
from india_stocks_api.instruments import Equity, Future, Option, Index
# or
from india_stocks_api import Equity, Future, Option, Index
```

---

## Equity

Represents a tradable stock or ETF.

```python
@dataclass(slots=True, frozen=True)
class Equity:
    symbol: str
    exchange: str = "NSE"
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `symbol` | `str` | (required) | Trading symbol (e.g., `"RELIANCE"`) |
| `exchange` | `str` | `"NSE"` | Exchange (`"NSE"` or `"BSE"`) |

### Usage

```python
from india_stocks_api import Equity

# NSE equity (default)
reliance = Equity("RELIANCE")

# BSE equity
sbin_bse = Equity("SBIN", exchange="BSE")

# Using in order
broker.place_order(
    instrument=Equity("TCS"),
    transaction_type=TransactionType.BUY,
    quantity=10
)
```

---

## Index

Represents a non-tradable market index. Used only for quotes and charts.

```python
@dataclass(slots=True, frozen=True)
class Index:
    symbol: str
    exchange: str = "NSE"
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `symbol` | `str` | (required) | Index symbol (e.g., `"NIFTY 50"`) |
| `exchange` | `str` | `"NSE"` | Exchange |

### Usage

```python
from india_stocks_api import Index

# Get Nifty 50 quote
nifty = Index("NIFTY 50")
quote = broker.get_quote(nifty)

# Bank Nifty
bank_nifty = Index("NIFTY BANK")
history = broker.get_history(bank_nifty, "2024-01-01", "2024-01-31", CandleInterval.ONE_DAY)
```

### Common Index Symbols

| Index | Symbol |
|-------|--------|
| Nifty 50 | `"NIFTY 50"` |
| Bank Nifty | `"NIFTY BANK"` |
| Nifty IT | `"NIFTY IT"` |
| Nifty Midcap | `"NIFTY MIDCAP 100"` |
| Sensex | `"SENSEX"` (BSE) |

---

## Future

Represents a futures contract on an underlying.

```python
@dataclass(slots=True, frozen=True)
class Future:
    symbol: str
    expiry: date
    exchange: str = "NFO"
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `symbol` | `str` | (required) | Underlying symbol (e.g., `"NIFTY"`, `"BANKNIFTY"`) |
| `expiry` | `date` | (required) | Contract expiry date |
| `exchange` | `str` | `"NFO"` | Exchange (typically `"NFO"` for NSE F&O) |

### Usage

```python
from datetime import date
from india_stocks_api import Future, TransactionType, ProductType

# Nifty future expiring Jan 25, 2024
nifty_fut = Future("NIFTY", date(2024, 1, 25))

# Get quote
quote = broker.get_quote(nifty_fut)

# Trade futures
broker.place_order(
    instrument=nifty_fut,
    transaction_type=TransactionType.BUY,
    quantity=50,  # Nifty lot size
    product_type=ProductType.CARRYFORWARD
)
```

### Finding Expiry Dates

Check exchange circulars or use known expiry patterns:
- **Weekly expiry**: Every Thursday (or previous trading day if holiday)
- **Monthly expiry**: Last Thursday of the month

---

## Option

Represents an options contract.

```python
@dataclass(slots=True, frozen=True)
class Option:
    symbol: str
    expiry: date
    strike: float
    opt_type: OptionType
    exchange: str = "NFO"
```

### Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `symbol` | `str` | (required) | Underlying symbol |
| `expiry` | `date` | (required) | Contract expiry date |
| `strike` | `float` | (required) | Strike price |
| `opt_type` | `OptionType` | (required) | `CE` (Call) or `PE` (Put) |
| `exchange` | `str` | `"NFO"` | Exchange |

### OptionType Enum

```python
from india_stocks_api import OptionType

OptionType.CE  # Call option
OptionType.PE  # Put option
```

### Usage

```python
from datetime import date
from india_stocks_api import Option, OptionType, TransactionType

# Bank Nifty 48000 Call expiring Jan 25, 2024
banknifty_ce = Option(
    symbol="BANKNIFTY",
    expiry=date(2024, 1, 25),
    strike=48000.0,
    opt_type=OptionType.CE
)

# Nifty 21500 Put
nifty_pe = Option(
    symbol="NIFTY",
    expiry=date(2024, 1, 25),
    strike=21500.0,
    opt_type=OptionType.PE
)

# Get option quote
quote = broker.get_quote(banknifty_ce)
print(f"Premium: {quote.ltp}, OI: {quote.oi}")

# Buy option
broker.place_order(
    instrument=banknifty_ce,
    transaction_type=TransactionType.BUY,
    quantity=15  # Bank Nifty lot size
)
```

---

## Type Alias

The library defines a union type for method signatures:

```python
Instrument = Equity | Future | Option | Index
```

---

## Resolution

Domain objects are automatically resolved to broker-specific tokens and trading symbols via the instrument database. You don't need to know internal broker tokens.

```python
# Just use the domain object
broker.get_quote(Equity("RELIANCE"))  # Resolved internally

# The resolver handles:
# - Symbol → Trading symbol mapping
# - Symbol → Broker token mapping
# - Exchange validation
```

---

## Best Practices

### 1. Reuse Domain Objects

```python
# Good: Create once, reuse
reliance = Equity("RELIANCE")
quote = broker.get_quote(reliance)
broker.place_order(instrument=reliance, ...)

# Avoid: Creating repeatedly
quote = broker.get_quote(Equity("RELIANCE"))
broker.place_order(instrument=Equity("RELIANCE"), ...)
```

### 2. Use Constants for Expiries

```python
# Good: Define expiries clearly
WEEKLY_EXPIRY = date(2024, 1, 25)
MONTHLY_EXPIRY = date(2024, 1, 25)

nifty_weekly = Option("NIFTY", WEEKLY_EXPIRY, 21500.0, OptionType.CE)
```

### 3. Validate Before Trading

```python
# Get quote to verify instrument exists
try:
    quote = broker.get_quote(option)
except ValidationError:
    print("Invalid instrument - check expiry/strike")
```

---

## See Also

- [Enums](enums.md) - OptionType, TransactionType, etc.
- [Broker Methods](broker-methods.md) - Using instruments in API calls
