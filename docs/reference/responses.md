# Response Objects Reference

Canonical response dataclasses returned by broker methods. All are immutable (`frozen=True`) and memory-efficient (`slots=True`).

---

## Import

```python
from india_stocks_api import (
    QuoteResponse,
    DepthResponse,
    DepthLevel,
    HistoryResponse,
    Candle,
    FundsResponse,
    ProfileResponse,
    OrderResponse,
    Order,
    Position,
    Holding,
    Trade,
    WebSocketTick,
    StreamDepthLevel,
    GTTRule,
    GTTResponse,
)
```

---

## QuoteResponse

Current market quote for an instrument.

```python
@dataclass(slots=True, frozen=True)
class QuoteResponse:
    bid: float
    ask: float
    open: float
    high: float
    low: float
    ltp: float
    prev_close: float
    volume: int
    oi: int
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `bid` | `float` | Best bid price |
| `ask` | `float` | Best ask price |
| `open` | `float` | Day's open price |
| `high` | `float` | Day's high price |
| `low` | `float` | Day's low price |
| `ltp` | `float` | Last traded price |
| `prev_close` | `float` | Previous day's close |
| `volume` | `int` | Total traded volume |
| `oi` | `int` | Open interest (F&O only) |

### Example

```python
quote = broker.get_quote(Equity("RELIANCE"))
print(f"LTP: ₹{quote.ltp:,.2f}")
print(f"Day Range: ₹{quote.low:,.2f} - ₹{quote.high:,.2f}")
print(f"Change: {((quote.ltp / quote.prev_close) - 1) * 100:.2f}%")
```

---

## DepthResponse

Market depth (Level 2 order book data).

```python
@dataclass(slots=True, frozen=True)
class DepthResponse:
    bids: tuple[DepthLevel, ...]
    asks: tuple[DepthLevel, ...]
    high: float
    low: float
    ltp: float
    ltq: int
    open: float
    prev_close: float
    volume: int
    oi: int
    total_buy_qty: int
    total_sell_qty: int
```

### DepthLevel

```python
@dataclass(slots=True, frozen=True)
class DepthLevel:
    price: float
    quantity: int
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `bids` | `tuple[DepthLevel, ...]` | Bid levels (best first) |
| `asks` | `tuple[DepthLevel, ...]` | Ask levels (best first) |
| `total_buy_qty` | `int` | Total bid quantity |
| `total_sell_qty` | `int` | Total ask quantity |
| `ltq` | `int` | Last traded quantity |
| *(plus quote fields)* | | Same as QuoteResponse |

### Example

```python
depth = broker.get_depth(Equity("SBIN"))

print("Top 5 Bids:")
for level in depth.bids[:5]:
    print(f"  ₹{level.price:,.2f} x {level.quantity:,}")

print("Top 5 Asks:")
for level in depth.asks[:5]:
    print(f"  ₹{level.price:,.2f} x {level.quantity:,}")
```

---

## HistoryResponse

Historical OHLCV candle data.

```python
@dataclass(slots=True, frozen=True)
class HistoryResponse:
    symbol: str
    exchange: str
    interval: str
    candles: tuple[Candle, ...]
    
    def to_dataframe(self) -> pd.DataFrame
```

### Candle

```python
@dataclass(slots=True, frozen=True)
class Candle:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    oi: int
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | `str` | Resolved trading symbol |
| `exchange` | `str` | Exchange code |
| `interval` | `str` | Candle interval |
| `candles` | `tuple[Candle, ...]` | OHLCV data |

### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `to_dataframe()` | `pd.DataFrame` | Convert to pandas DataFrame |

### Example

```python
history = broker.get_history(
    Equity("INFY"),
    "2024-01-01",
    "2024-01-31",
    CandleInterval.ONE_DAY
)

# Use as DataFrame
df = history.to_dataframe()
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
print(df.tail())

# Or iterate candles
for candle in history.candles[-5:]:
    print(f"{candle.timestamp}: O={candle.open} H={candle.high} L={candle.low} C={candle.close}")
```

---

## FundsResponse

Account funds and margin information.

```python
@dataclass(slots=True, frozen=True)
class FundsResponse:
    available_cash: float
    collateral: float
    m2m_realized: float
    m2m_unrealized: float
    utilized_debits: float
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `available_cash` | `float` | Available cash for trading |
| `collateral` | `float` | Collateral margin |
| `m2m_realized` | `float` | Realized mark-to-market |
| `m2m_unrealized` | `float` | Unrealized mark-to-market |
| `utilized_debits` | `float` | Margin utilized |

### Example

```python
funds = broker.get_funds()
print(f"Available: ₹{funds.available_cash:,.2f}")
print(f"Collateral: ₹{funds.collateral:,.2f}")
print(f"Unrealized P&L: ₹{funds.m2m_unrealized:,.2f}")
```

---

## ProfileResponse

User profile information.

```python
@dataclass(slots=True, frozen=True)
class ProfileResponse:
    client_code: str | None
    name: str | None
    exchanges: tuple[str, ...]
    products: tuple[str, ...]
    email: str | None
    mobile: str | None
    raw: dict[str, Any]
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `client_code` | `str \| None` | Broker client code |
| `name` | `str \| None` | Account holder name |
| `exchanges` | `tuple[str, ...]` | Enabled exchanges |
| `products` | `tuple[str, ...]` | Enabled products |
| `email` | `str \| None` | Registered email |
| `mobile` | `str \| None` | Registered mobile |
| `raw` | `dict` | Full broker response |

---

## OrderResponse

Response for order create/modify/cancel operations.

```python
@dataclass(slots=True, frozen=True)
class OrderResponse:
    order_id: str | None
    status: str
    message: str
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `order_id` | `str \| None` | Broker order ID (if successful) |
| `status` | `str` | `"success"`, `"failed"`, or `"error"` |
| `message` | `str` | Status message or error details |

### Example

```python
response = broker.place_order(
    instrument=Equity("RELIANCE"),
    transaction_type=TransactionType.BUY,
    quantity=10
)

if response.status == "success":
    print(f"Order placed: {response.order_id}")
else:
    print(f"Failed: {response.message}")
```

---

## Order

Order details from order book.

```python
@dataclass(slots=True, frozen=True)
class Order:
    order_id: str
    symbol: str
    exchange: str
    transaction_type: str
    order_type: str
    product_type: str
    quantity: int
    price: float
    trigger_price: float
    average_price: float
    status: str
    timestamp: str
    raw: dict[str, Any]
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `order_id` | `str` | Unique order identifier |
| `symbol` | `str` | Trading symbol |
| `exchange` | `str` | Exchange code |
| `transaction_type` | `str` | `"BUY"` or `"SELL"` |
| `order_type` | `str` | `"MARKET"`, `"LIMIT"`, etc. |
| `product_type` | `str` | `"MIS"`, `"CNC"`, `"NRML"` |
| `quantity` | `int` | Order quantity |
| `price` | `float` | Limit price |
| `trigger_price` | `float` | Trigger price (SL orders) |
| `average_price` | `float` | Fill price |
| `status` | `str` | `"open"`, `"complete"`, `"rejected"`, `"cancelled"` |
| `timestamp` | `str` | Order timestamp |
| `raw` | `dict` | Full broker response |

### Example

```python
orders = broker.get_orders()
for order in orders:
    print(f"{order.order_id}: {order.symbol} {order.transaction_type} "
          f"{order.quantity} @ {order.price} [{order.status}]")
```

---

## Position

Open position information.

```python
@dataclass(slots=True, frozen=True)
class Position:
    symbol: str
    exchange: str
    product_type: str
    quantity: int
    average_price: float
    ltp: float
    pnl: float
    raw: dict[str, Any]
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | `str` | Trading symbol |
| `exchange` | `str` | Exchange code |
| `product_type` | `str` | Product type |
| `quantity` | `int` | Position quantity (negative for short) |
| `average_price` | `float` | Entry average price |
| `ltp` | `float` | Last traded price |
| `pnl` | `float` | Unrealized P&L |
| `raw` | `dict` | Full broker response |

### Example

```python
positions = broker.get_positions()
total_pnl = sum(p.pnl for p in positions)
print(f"Total Unrealized P&L: ₹{total_pnl:,.2f}")
```

---

## Holding

Long-term delivery holding.

```python
@dataclass(slots=True, frozen=True)
class Holding:
    symbol: str
    exchange: str
    quantity: int
    average_price: float
    ltp: float
    pnl: float
    pnl_percent: float
    raw: dict[str, Any]
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `pnl_percent` | `float` | P&L percentage |
| *(others)* | | Similar to Position |

### Example

```python
holdings = broker.get_holdings()
for h in holdings:
    print(f"{h.symbol}: {h.quantity} @ ₹{h.average_price:,.2f} "
          f"(P&L: {h.pnl_percent:+.2f}%)")
```

---

## Trade

Executed trade record.

```python
@dataclass(slots=True, frozen=True)
class Trade:
    order_id: str
    symbol: str
    exchange: str
    transaction_type: str
    quantity: int
    price: float
    trade_value: float
    timestamp: str
    raw: dict[str, Any]
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `trade_value` | `float` | Total trade value |
| `timestamp` | `str` | Trade execution time |
| *(others)* | | Similar to Order |

---

## WebSocketTick

Real-time tick from WebSocket stream.

```python
@dataclass(slots=True, frozen=True)
class WebSocketTick:
    symbol: str
    exchange: str
    token: str
    mode: str
    exchange_type: int
    timestamp: int
    ltp: float
    ltq: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    oi: int
    bid: float
    ask: float
    total_buy_quantity: float
    total_sell_quantity: float
    bids: tuple[StreamDepthLevel, ...]
    asks: tuple[StreamDepthLevel, ...]
```

### StreamDepthLevel

```python
@dataclass(slots=True, frozen=True)
class StreamDepthLevel:
    price: float
    quantity: int
    orders: int
```

### Example

```python
def on_tick(tick: WebSocketTick):
    print(f"{tick.symbol}: {tick.ltp} ({tick.volume:,})")

broker.start_streaming(
    instruments=[Equity("RELIANCE")],
    on_tick=on_tick
)
```

---

## GTTRule

GTT (Good Till Triggered) rule details.

```python
@dataclass(slots=True, frozen=True)
class GTTRule:
    rule_id: str
    symbol: str
    exchange: str
    transaction_type: str
    product_type: str
    quantity: int
    price: float
    trigger_price: float
    status: str
    created_at: str | None
    updated_at: str | None
    expires_at: str | None
    raw: dict[str, Any]
```

---

## GTTResponse

Response for GTT operations.

```python
@dataclass(slots=True, frozen=True)
class GTTResponse:
    rule_id: str | None
    status: str
    message: str
```

---

## Raw Data Access

All response objects include a `raw` field (where applicable) containing the complete broker response for advanced use cases:

```python
order = broker.get_order_details("123456")
# Access broker-specific fields
print(order.raw)
```

---

## See Also

- [Broker Methods](broker-methods.md) - Methods returning these objects
- [Domain Objects](domain-objects.md) - Input instrument types
