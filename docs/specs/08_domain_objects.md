# Domain-Specific Objects: Equity, Future, Option, Index

Instead of a generic `Instrument` class, we will define specialized classes for each asset class. This enforces valid inputs at the object construction level.

## 1. Class Hierarchy

```python
from abc import ABC
from datetime import date
from decimal import Decimal
from enum import Enum

class SecType(str, Enum):
    EQ = "EQ"
    FUT = "FUT"
    OPT = "OPT"
    IDX = "IDX"  # New type for Indices

class Instrument(ABC):
    """Base class for all financial instruments"""
    symbol: str  # The Standardized OpenAlgo Symbol (e.g., "NIFTY", "RELIANCE")
    exchange: str
    currency: str = "INR"

class Equity(Instrument):
    """Represents a simple Tradable Stock or ETF"""
    def __init__(self, symbol: str, exchange: str = "NSE"):
        self.symbol = symbol  # e.g., "RELIANCE"
        self.exchange = exchange
        self.sec_type = SecType.EQ

class Index(Instrument):
    """
    Represents a Non-Tradable Market Index (e.g., NIFTY 50).
    Used for getting quotes/charts, NOT for placing orders.
    """
    def __init__(self, symbol: str, exchange: str = "NSE"):
        self.symbol = symbol  # e.g., "NIFTY 50"
        self.exchange = exchange
        self.sec_type = SecType.IDX

class Future(Instrument):
    """Represents a Futures Contract"""
    def __init__(self, symbol: str, expiry: date, exchange: str = "NFO"):
        self.symbol = symbol  # e.g., "NIFTY"
        self.expiry = expiry
        self.exchange = exchange
        self.sec_type = SecType.FUT

class Option(Instrument):
    """Represents an Options Contract"""
    def __init__(self, symbol: str, expiry: date, strike: float, opt_type: OptionType, exchange: str = "NFO"):
        self.symbol = symbol  # e.g., "BANKNIFTY"
        self.expiry = expiry
        self.strike = strike
        self.opt_type = opt_type # CE/PE
        self.exchange = exchange
        self.sec_type = SecType.OPT
```

## 2. Handling Ambiguity & Conflicts

### Solution for Point 1 (Expiry Ambiguity)

To mitigate the risk of ambiguous expiries (e.g., Weekly vs Monthly on same day):

- The `Future` and `Option` classes will strictly rely on `expiry` **Date** matching.
- The internal resolution engine (`token_db`) scans all master contracts for that underlying.
- **Validation**: If multiple contracts match the same Description (Symbol + Date + Strike + Type), the library **must raise an `AmbiguousContractError`** rather than guessing. This forces the user to be more specific or check their data, preventing accidental trades.

### Solution for Point 3 (Symbol Mismatch)

- **Standardization**: The `symbol` input always expects the **OpenAlgo Standard Name** (which mirrors NSE naming).
- **Mapping**: The internal SQLite database contains a `brsymbol` column. Finding the right token is a 2-step process:
  1.  User Input ("M&M") -> Match OpenAlgo Symbol column ("M&M") -> Get internal Token.
  2.  Use Token -> Look up `brsymbol` (Broker specific, e.g., "MM") -> Send to API.
- This creates a "Firewall" where the user only speaks OpenAlgo/NSE dialect, and the system translates.

### Solution for Point 4 (Index vs Spot)

- Introduced `Index` class.
- `place_order()` will type-check the input. If `isinstance(instr, Index)`, it raises `TypeError("Cannot place order on non-tradable Index. Use Future or Option.")`.

### Solution for Point 5 (Multi-Exchange)

- `Instrument` objects carry their own `exchange` attribute.
- `Equity("IDEA", exchange="BSE")` and `Equity("IDEA", exchange="NSE")` resolve to different tokens internally.
- The resolution query strictly filters by `exchange`, preventing cross-exchange token collisions.

## 3. Usage in API

```python
# 1. Trading a Stock
reli = Equity("RELIANCE")
client.place_order(reli, TransactionType.BUY, qty=10)

# 2. Getting Index Data (Safe)
nifty = Index("NIFTY 50")
quote = client.get_quote(nifty) # Allowed
# client.place_order(nifty, ...) # Raises TypeError

# 3. Trading a Future
nifty_fut = Future("NIFTY", expiry=date(2023, 12, 28))
client.place_order(nifty_fut, TransactionType.SELL, qty=50)
```
