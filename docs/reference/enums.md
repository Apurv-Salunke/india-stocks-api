# Enums Reference

Type-safe enumerations for order parameters, intervals, and streaming modes.

---

## Import

```python
from india_stocks_api import (
    OrderType,
    TransactionType,
    ProductType,
    OrderValidity,
    OptionType,
    CandleInterval,
    StreamMode,
    GTTStatus,
)
```

---

## OrderType

Order execution type.

```python
class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"        # Stop Loss Limit
    SLM = "SL-M"     # Stop Loss Market
```

| Value | Description |
|-------|-------------|
| `MARKET` | Execute at current market price |
| `LIMIT` | Execute only at specified price or better |
| `SL` | Stop loss limit - triggers limit order at trigger price |
| `SLM` | Stop loss market - triggers market order at trigger price |

### Usage

```python
from india_stocks_api import OrderType

# Market order (default)
broker.place_order(instrument, TransactionType.BUY, 10)

# Limit order
broker.place_order(
    instrument,
    TransactionType.BUY,
    10,
    order_type=OrderType.LIMIT,
    price=2850.00
)

# Stop loss order
broker.place_order(
    instrument,
    TransactionType.SELL,
    10,
    order_type=OrderType.SL,
    trigger_price=2800.00,
    price=2795.00
)
```

---

## TransactionType

Buy or sell direction.

```python
class TransactionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
```

| Value | Description |
|-------|-------------|
| `BUY` | Buy / Long entry |
| `SELL` | Sell / Short entry or exit |

### Usage

```python
from india_stocks_api import TransactionType

# Buy order
broker.place_order(instrument, TransactionType.BUY, 10)

# Sell order
broker.place_order(instrument, TransactionType.SELL, 10)
```

---

## ProductType

Order product/margin type.

```python
class ProductType(str, Enum):
    INTRADAY = "MIS"       # Margin Intraday Squareoff
    DELIVERY = "CNC"       # Cash N Carry (Equity Delivery)
    CARRYFORWARD = "NRML"  # Normal (F&O Carry Forward)
    COVER_ORDER = "CO"     # Cover Order
    BRACKET_ORDER = "BO"   # Bracket Order
```

| Value | Internal | Description |
|-------|----------|-------------|
| `INTRADAY` | `MIS` | Day trading, auto squared-off at market close |
| `DELIVERY` | `CNC` | Equity delivery, held in demat account |
| `CARRYFORWARD` | `NRML` | F&O positions, carry forward overnight |
| `COVER_ORDER` | `CO` | Intraday with mandatory stop loss |
| `BRACKET_ORDER` | `BO` | Intraday with target and stop loss |

### Usage

```python
from india_stocks_api import ProductType

# Intraday (default)
broker.place_order(instrument, TransactionType.BUY, 10)

# Delivery (equity)
broker.place_order(
    instrument,
    TransactionType.BUY,
    10,
    product_type=ProductType.DELIVERY
)

# Carry forward (F&O)
broker.place_order(
    future_instrument,
    TransactionType.BUY,
    50,
    product_type=ProductType.CARRYFORWARD
)
```

---

## OrderValidity

Order time-in-force.

```python
class OrderValidity(str, Enum):
    DAY = "DAY"
    IOC = "IOC"   # Immediate or Cancel
    EOS = "EOS"   # End of Session
```

| Value | Description |
|-------|-------------|
| `DAY` | Valid until market close |
| `IOC` | Execute immediately, cancel unfilled portion |
| `EOS` | Valid until end of trading session |

### Usage

```python
from india_stocks_api import OrderValidity

# Day order (default)
broker.place_order(instrument, TransactionType.BUY, 10)

# IOC order
broker.place_order(
    instrument,
    TransactionType.BUY,
    10,
    validity=OrderValidity.IOC
)
```

---

## OptionType

Option contract type (Call or Put).

```python
class OptionType(str, Enum):
    CE = "CE"  # Call Option
    PE = "PE"  # Put Option
```

| Value | Description |
|-------|-------------|
| `CE` | Call option - right to buy |
| `PE` | Put option - right to sell |

### Usage

```python
from india_stocks_api import Option, OptionType
from datetime import date

# Call option
call = Option("NIFTY", date(2024, 1, 25), 21500.0, OptionType.CE)

# Put option
put = Option("NIFTY", date(2024, 1, 25), 21500.0, OptionType.PE)
```

---

## CandleInterval

Time intervals for historical data.

```python
class CandleInterval(str, Enum):
    ONE_MINUTE = "1m"
    THREE_MINUTE = "3m"
    FIVE_MINUTE = "5m"
    TEN_MINUTE = "10m"
    FIFTEEN_MINUTE = "15m"
    THIRTY_MINUTE = "30m"
    ONE_HOUR = "1h"
    ONE_DAY = "D"
```

| Value | Internal | Description |
|-------|----------|-------------|
| `ONE_MINUTE` | `1m` | 1-minute candles |
| `THREE_MINUTE` | `3m` | 3-minute candles |
| `FIVE_MINUTE` | `5m` | 5-minute candles |
| `TEN_MINUTE` | `10m` | 10-minute candles |
| `FIFTEEN_MINUTE` | `15m` | 15-minute candles |
| `THIRTY_MINUTE` | `30m` | 30-minute candles |
| `ONE_HOUR` | `1h` | 1-hour candles |
| `ONE_DAY` | `D` | Daily candles |

### Usage

```python
from india_stocks_api import CandleInterval

# Daily candles
history = broker.get_history(
    instrument,
    "2024-01-01",
    "2024-01-31",
    CandleInterval.ONE_DAY
)

# 15-minute candles
intraday = broker.get_history(
    instrument,
    "2024-01-15",
    "2024-01-15",
    CandleInterval.FIFTEEN_MINUTE
)
```

---

## StreamMode

WebSocket streaming data modes.

```python
class StreamMode(int, Enum):
    LTP = 1         # Last Traded Price only
    QUOTE = 2       # LTP + OHLC + Volume
    SNAP_QUOTE = 3  # Quote + Best 5 Bid/Ask
    DEPTH = 4       # Full 20-level Market Depth (NSE CM only)
```

| Value | Level | Description |
|-------|-------|-------------|
| `LTP` | 1 | Minimal - just price |
| `QUOTE` | 2 | Basic quote data |
| `SNAP_QUOTE` | 3 | Quote + top 5 order book |
| `DEPTH` | 4 | Full 20-level depth (NSE Cash only) |

### Usage

```python
from india_stocks_api import StreamMode

# LTP only (lowest bandwidth)
broker.start_streaming(
    instruments=[Equity("RELIANCE")],
    mode=StreamMode.LTP,
    on_tick=handle_tick
)

# Full depth
broker.start_streaming(
    instruments=[Equity("SBIN")],
    mode=StreamMode.DEPTH,
    on_tick=handle_tick
)
```

### Bandwidth Considerations

| Mode | Data Volume | Use Case |
|------|-------------|----------|
| `LTP` | Lowest | Price alerts, watchlists |
| `QUOTE` | Medium | Intraday charts |
| `SNAP_QUOTE` | Higher | Scalping, quick decisions |
| `DEPTH` | Highest | Order flow analysis |

---

## GTTStatus

GTT (Good Till Triggered) rule status.

```python
class GTTStatus(str, Enum):
    NEW = "NEW"
    ACTIVE = "ACTIVE"
    TRIGGERED = "SENTTOEXCHANGE"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"
    FORALL = "FORALL"  # Filter for listing
```

| Value | Description |
|-------|-------------|
| `NEW` | Newly created rule |
| `ACTIVE` | Rule is active and monitoring |
| `TRIGGERED` | Trigger condition met, sent to exchange |
| `CANCELLED` | Manually cancelled |
| `EXPIRED` | Rule expired (past validity period) |
| `REJECTED` | Rejected by broker/exchange |
| `FORALL` | Special filter to retrieve all rules |

### Usage

```python
from india_stocks_api import GTTStatus

# Get active rules only
active_rules = broker.get_gtt_list(status=GTTStatus.ACTIVE)

# Get all rules
all_rules = broker.get_gtt_list(status=GTTStatus.FORALL)
```

---

## String Values

All enums inherit from `str` (except `StreamMode` which is `int`), so they can be used directly in string contexts:

```python
order_type = OrderType.LIMIT
print(f"Order type: {order_type}")  # "Order type: LIMIT"

# Comparison works with strings
if order_type == "LIMIT":
    print("Limit order")
```

---

## See Also

- [Domain Objects](domain-objects.md) - Using OptionType with Option
- [Broker Methods](broker-methods.md) - Using enums in API calls
