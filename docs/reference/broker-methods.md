# Broker Methods Reference

Complete method reference for broker adapters.

---

## BaseBroker

Abstract base class defining the common interface for all broker implementations.

### Factory Method

```python
@classmethod
def create(cls, broker_name: str, **kwargs) -> BaseBroker
```

Create a broker instance by name.

| Parameter | Type | Description |
|-----------|------|-------------|
| `broker_name` | `str` | Broker identifier (e.g., `"angel"`) |
| `**kwargs` | | Broker-specific credentials |

**Returns:** `BaseBroker` subclass instance

**Raises:** `ValidationError` if broker_name is unknown

**Example:**
```python
broker = BaseBroker.create("angel",
    api_key="xxx",
    client_code="xxx",
    password="1234",
    totp_key="xxx"
)
```

---

## Authentication

### authenticate()

```python
@abstractmethod
def authenticate(self) -> bool
```

Authenticate with the broker. Required before any trading operations.

**Returns:** `True` if authentication succeeds

**Raises:**
- `AuthenticationError` - Invalid credentials
- `NetworkError` - Connection failed

**Example:**
```python
broker = AngelOne(...)
broker.authenticate()
```

---

## Market Data

### get_quote()

```python
@abstractmethod
def get_quote(
    self,
    instrument: Equity | Future | Option | Index
) -> QuoteResponse
```

Get current market quote for an instrument.

| Parameter | Type | Description |
|-----------|------|-------------|
| `instrument` | `Equity \| Future \| Option \| Index` | Target instrument |

**Returns:** [`QuoteResponse`](responses.md#quoteresponse)

**Raises:**
- `AuthenticationError` - Not authenticated
- `SessionExpiredError` - Session expired
- `ValidationError` - Invalid instrument

**Example:**
```python
quote = broker.get_quote(Equity("RELIANCE"))
print(f"LTP: {quote.ltp}")
```

### get_depth()

```python
@abstractmethod
def get_depth(
    self,
    instrument: Equity | Future | Option | Index
) -> DepthResponse
```

Get market depth (Level 2 order book data).

| Parameter | Type | Description |
|-----------|------|-------------|
| `instrument` | `Equity \| Future \| Option \| Index` | Target instrument |

**Returns:** [`DepthResponse`](responses.md#depthresponse)

**Raises:**
- `AuthenticationError` - Not authenticated
- `SessionExpiredError` - Session expired

**Example:**
```python
depth = broker.get_depth(Equity("SBIN"))
for bid in depth.bids[:5]:
    print(f"Bid: {bid.price} x {bid.quantity}")
```

### get_history()

```python
@abstractmethod
def get_history(
    self,
    instrument: Equity | Future | Option | Index,
    start_date: str,
    end_date: str,
    interval: CandleInterval
) -> HistoryResponse
```

Get historical OHLCV candle data.

| Parameter | Type | Description |
|-----------|------|-------------|
| `instrument` | `Equity \| Future \| Option \| Index` | Target instrument |
| `start_date` | `str` | Start date in `YYYY-MM-DD` format |
| `end_date` | `str` | End date in `YYYY-MM-DD` format |
| `interval` | `CandleInterval` | Candle interval (e.g., `ONE_MINUTE`, `ONE_DAY`) |

**Returns:** [`HistoryResponse`](responses.md#historyresponse) with candle data

**Raises:**
- `AuthenticationError` - Not authenticated
- `ValidationError` - Invalid date range or interval

**Example:**
```python
from india_stocks_api import CandleInterval

history = broker.get_history(
    Equity("INFY"),
    "2024-01-01",
    "2024-01-31",
    CandleInterval.ONE_DAY
)

# Convert to DataFrame
df = history.to_dataframe()
```

---

## Order Management

### place_order()

```python
@abstractmethod
def place_order(
    self,
    instrument: Equity | Future | Option,
    transaction_type: TransactionType,
    quantity: int,
    order_type: OrderType = OrderType.MARKET,
    product_type: ProductType = ProductType.INTRADAY,
    price: float = 0.0,
    trigger_price: float = 0.0,
    validity: OrderValidity = OrderValidity.DAY,
    **kwargs
) -> OrderResponse
```

Place a new order.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `instrument` | `Equity \| Future \| Option` | (required) | Instrument to trade |
| `transaction_type` | `TransactionType` | (required) | `BUY` or `SELL` |
| `quantity` | `int` | (required) | Order quantity |
| `order_type` | `OrderType` | `MARKET` | `MARKET`, `LIMIT`, `SL`, `SLM` |
| `product_type` | `ProductType` | `INTRADAY` | `INTRADAY`, `DELIVERY`, `CARRYFORWARD` |
| `price` | `float` | `0.0` | Limit price (for `LIMIT`/`SL` orders) |
| `trigger_price` | `float` | `0.0` | Trigger price (for `SL`/`SLM` orders) |
| `validity` | `OrderValidity` | `DAY` | `DAY`, `IOC` |

**Returns:** [`OrderResponse`](responses.md#orderresponse)

**Raises:**
- `AuthenticationError` - Not authenticated
- `OrderError` - Order rejected by broker
- `ValidationError` - Invalid parameters

**Example:**
```python
from india_stocks_api import TransactionType, OrderType

# Market order
response = broker.place_order(
    instrument=Equity("RELIANCE"),
    transaction_type=TransactionType.BUY,
    quantity=10
)

# Limit order
response = broker.place_order(
    instrument=Equity("TCS"),
    transaction_type=TransactionType.BUY,
    quantity=5,
    order_type=OrderType.LIMIT,
    price=3500.00
)
```

### modify_order()

```python
@abstractmethod
def modify_order(
    self,
    order_id: str,
    price: float = 0.0,
    trigger_price: float = 0.0,
    quantity: int = 0
) -> OrderResponse
```

Modify an existing pending order.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `order_id` | `str` | (required) | Order ID to modify |
| `price` | `float` | `0.0` | New limit price (0 = unchanged) |
| `trigger_price` | `float` | `0.0` | New trigger price (0 = unchanged) |
| `quantity` | `int` | `0` | New quantity (0 = unchanged) |

**Returns:** [`OrderResponse`](responses.md#orderresponse)

**Raises:**
- `OrderError` - Order not found or not modifiable

**Example:**
```python
response = broker.modify_order(
    order_id="123456789",
    price=2850.00
)
```

### cancel_order()

```python
@abstractmethod
def cancel_order(self, order_id: str) -> OrderResponse
```

Cancel a pending order.

| Parameter | Type | Description |
|-----------|------|-------------|
| `order_id` | `str` | Order ID to cancel |

**Returns:** [`OrderResponse`](responses.md#orderresponse)

**Example:**
```python
response = broker.cancel_order("123456789")
```

### get_orders()

```python
@abstractmethod
def get_orders(self) -> list[Order]
```

Get all orders for the current trading day.

**Returns:** List of [`Order`](responses.md#order) objects

**Example:**
```python
orders = broker.get_orders()
for order in orders:
    print(f"{order.order_id}: {order.symbol} {order.status}")
```

### get_order_details()

```python
@abstractmethod
def get_order_details(self, order_id: str) -> Order
```

Get details for a specific order.

| Parameter | Type | Description |
|-----------|------|-------------|
| `order_id` | `str` | Order ID to look up |

**Returns:** [`Order`](responses.md#order) object

### get_executed_orders()

```python
def get_executed_orders(self) -> list[Order]
```

Get only filled/executed orders (convenience method).

**Returns:** List of [`Order`](responses.md#order) with status `"complete"`

### get_pending_orders()

```python
def get_pending_orders(self) -> list[Order]
```

Get only pending/open orders (convenience method).

**Returns:** List of [`Order`](responses.md#order) with status `"open"` or `"trigger pending"`

---

## Portfolio & Account

### get_positions()

```python
@abstractmethod
def get_positions(self) -> list[Position]
```

Get current open positions.

**Returns:** List of [`Position`](responses.md#position) objects

**Example:**
```python
positions = broker.get_positions()
for pos in positions:
    print(f"{pos.symbol}: {pos.quantity} @ {pos.average_price}, PnL: {pos.pnl}")
```

### get_holdings()

```python
@abstractmethod
def get_holdings(self) -> list[Holding]
```

Get long-term delivery holdings.

**Returns:** List of [`Holding`](responses.md#holding) objects

**Example:**
```python
holdings = broker.get_holdings()
total_value = sum(h.ltp * h.quantity for h in holdings)
```

### get_trades()

```python
@abstractmethod
def get_trades(self) -> list[Trade]
```

Get executed trades for the current day.

**Returns:** List of [`Trade`](responses.md#trade) objects

### get_funds()

```python
@abstractmethod
def get_funds(self) -> FundsResponse
```

Get account funds and margin information.

**Returns:** [`FundsResponse`](responses.md#fundsresponse)

**Example:**
```python
funds = broker.get_funds()
print(f"Available: ₹{funds.available_cash:,.2f}")
```

### get_profile()

```python
@abstractmethod
def get_profile(self) -> ProfileResponse
```

Get user profile information.

**Returns:** [`ProfileResponse`](responses.md#profileresponse)

---

## Streaming (Broker-Specific)

### start_streaming()

```python
def start_streaming(
    self,
    instruments: list[Equity | Future | Option | Index],
    mode: StreamMode = StreamMode.LTP,
    on_tick: Callable[[WebSocketTick], None] | None = None,
    on_error: Callable[[Exception], None] | None = None,
    on_close: Callable[[], None] | None = None
) -> None
```

Start WebSocket streaming for live tick data.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `instruments` | `list[...]` | (required) | Instruments to subscribe |
| `mode` | `StreamMode` | `LTP` | Streaming mode |
| `on_tick` | `Callable` | `None` | Tick callback |
| `on_error` | `Callable` | `None` | Error callback |
| `on_close` | `Callable` | `None` | Disconnect callback |

**Example:**
```python
def handle_tick(tick):
    print(f"{tick.symbol}: {tick.ltp}")

broker.start_streaming(
    instruments=[Equity("RELIANCE"), Equity("TCS")],
    mode=StreamMode.QUOTE,
    on_tick=handle_tick
)
```

### stop_streaming()

```python
def stop_streaming(self) -> None
```

Stop WebSocket streaming and disconnect.

---

## GTT Orders (Broker-Specific)

GTT (Good Till Triggered) orders are supported on Angel One. See [GTT Orders](../user/orders-trading.md#gtt-orders) for details.

### create_gtt()

```python
def create_gtt(
    self,
    instrument: Equity | Future | Option,
    transaction_type: TransactionType,
    quantity: int,
    trigger_price: float,
    price: float,
    product_type: ProductType = ProductType.DELIVERY,
    time_period: int = 365
) -> GTTResponse
```

### modify_gtt()

```python
def modify_gtt(
    self,
    rule_id: int,
    instrument: Equity | Future | Option,
    quantity: int,
    trigger_price: float,
    price: float
) -> GTTResponse
```

### cancel_gtt()

```python
def cancel_gtt(
    self,
    rule_id: int,
    instrument: Equity | Future | Option
) -> GTTResponse
```

### get_gtt_list()

```python
def get_gtt_list(status: GTTStatus | None = None) -> list[GTTRule]
```

### get_gtt_details()

```python
def get_gtt_details(rule_id: int) -> GTTRule
```

---

## See Also

- [Domain Objects](domain-objects.md) - Equity, Future, Option, Index
- [Enums](enums.md) - OrderType, TransactionType, etc.
- [Responses](responses.md) - Response dataclasses
- [Exceptions](exceptions.md) - Error classes
