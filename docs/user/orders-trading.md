# Orders & Trading

This guide covers order placement, lifecycle management, and GTT orders.

---

## Order Placement

### Market Order

Executes immediately at the best available price.

```python
from india_stocks_api.instruments import Equity
from india_stocks_api.constants import TransactionType, OrderType, ProductType

response = broker.place_order(
    instrument=Equity("SBIN"),
    transaction_type=TransactionType.BUY,
    quantity=1,
    order_type=OrderType.MARKET,
    product_type=ProductType.INTRADAY
)

print(f"Order ID: {response.order_id}")
print(f"Status: {response.status}")
```

### Limit Order

Executes only at your specified price or better.

```python
response = broker.place_order(
    instrument=Equity("RELIANCE"),
    transaction_type=TransactionType.BUY,
    quantity=1,
    order_type=OrderType.LIMIT,
    product_type=ProductType.DELIVERY,
    price=2800.0  # Your limit price
)
```

### Stop-Loss Order (SL)

A limit order that triggers when the stop price is hit.

```python
response = broker.place_order(
    instrument=Equity("RELIANCE"),
    transaction_type=TransactionType.SELL,
    quantity=1,
    order_type=OrderType.SL,
    product_type=ProductType.INTRADAY,
    trigger_price=2750.0,  # Triggers when price falls to this
    price=2745.0           # Limit price after trigger
)
```

### Stop-Loss Market Order (SL-M)

A market order that triggers when the stop price is hit.

```python
response = broker.place_order(
    instrument=Equity("RELIANCE"),
    transaction_type=TransactionType.SELL,
    quantity=1,
    order_type=OrderType.SLM,
    product_type=ProductType.INTRADAY,
    trigger_price=2750.0  # Executes at market when hit
)
```

---

## Order Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `instrument` | Equity/Future/Option | Yes | What to trade |
| `transaction_type` | TransactionType | Yes | BUY or SELL |
| `quantity` | int | Yes | Number of shares/lots |
| `order_type` | OrderType | No | MARKET, LIMIT, SL, SLM (default: MARKET) |
| `product_type` | ProductType | No | INTRADAY, DELIVERY, CARRYFORWARD (default: INTRADAY) |
| `price` | float | No | Required for LIMIT and SL orders |
| `trigger_price` | float | No | Required for SL and SLM orders |
| `validity` | OrderValidity | No | DAY or IOC (default: DAY) |

---

## Product Types

### INTRADAY (MIS)

Position must be closed by market end. Higher leverage, lower margin.

```python
broker.place_order(
    instrument=Equity("SBIN"),
    transaction_type=TransactionType.BUY,
    quantity=10,
    product_type=ProductType.INTRADAY  # Auto-squared off at 3:15 PM
)
```

### DELIVERY (CNC)

For holding overnight. Full margin required.

```python
broker.place_order(
    instrument=Equity("RELIANCE"),
    transaction_type=TransactionType.BUY,
    quantity=5,
    product_type=ProductType.DELIVERY  # Held in your demat
)
```

### CARRYFORWARD (NRML)

For F&O positions you want to hold overnight.

```python
from india_stocks_api.instruments import Future
from datetime import date

broker.place_order(
    instrument=Future("NIFTY", expiry=date(2024, 12, 26)),
    transaction_type=TransactionType.BUY,
    quantity=50,  # 1 lot
    product_type=ProductType.CARRYFORWARD
)
```

---

## F&O Orders

### Futures

```python
from india_stocks_api.instruments import Future
from datetime import date

# Buy NIFTY future
broker.place_order(
    instrument=Future("NIFTY", expiry=date(2024, 12, 26)),
    transaction_type=TransactionType.BUY,
    quantity=50,
    order_type=OrderType.LIMIT,
    product_type=ProductType.CARRYFORWARD,
    price=22500.0
)
```

### Options

```python
from india_stocks_api.instruments import Option
from india_stocks_api.constants import OptionType
from datetime import date

# Buy BANKNIFTY Call
broker.place_order(
    instrument=Option(
        symbol="BANKNIFTY",
        expiry=date(2024, 12, 26),
        strike=52000.0,
        opt_type=OptionType.CE
    ),
    transaction_type=TransactionType.BUY,
    quantity=15,  # 1 lot
    order_type=OrderType.LIMIT,
    product_type=ProductType.CARRYFORWARD,
    price=250.0
)
```

---

## Order Management

### View Orders

```python
orders = broker.get_orders()

for order in orders:
    print(f"{order.order_id}: {order.symbol} {order.transaction_type}")
    print(f"  Status: {order.status}")
    print(f"  Qty: {order.quantity} @ {order.average_price}")
```

### Order Fields

| Field | Description |
|-------|-------------|
| `order_id` | Unique order identifier |
| `symbol` | Trading symbol |
| `exchange` | Exchange (NSE, NFO, etc.) |
| `transaction_type` | BUY or SELL |
| `order_type` | MARKET, LIMIT, SL, SL-M |
| `product_type` | MIS, CNC, NRML |
| `quantity` | Order quantity |
| `price` | Limit price |
| `trigger_price` | Trigger price (SL orders) |
| `average_price` | Execution price |
| `status` | open, complete, cancelled, rejected |
| `timestamp` | Order time |

### Get Order Details

```python
order = broker.get_order_details(order_id="240115000012345")
print(f"Status: {order.status}")
print(f"Filled: {order.average_price}")
```

---

## Modify Orders

Modify pending orders (price, quantity, trigger).

```python
response = broker.modify_order(
    order_id="240115000012345",
    price=2810.0,        # New limit price
    quantity=2           # New quantity
)

print(f"Modified: {response.status}")
```

### Modifiable Fields

- `price` - New limit price
- `trigger_price` - New trigger price
- `quantity` - New quantity

> **Note**: You cannot modify order type or transaction type. Cancel and place a new order instead.

---

## Cancel Orders

Cancel pending orders.

```python
response = broker.cancel_order(order_id="240115000012345")
print(f"Cancelled: {response.status}")
```

### Cancel All (Not Available)

Cancelling all orders at once is not currently supported. Iterate manually:

```python
orders = broker.get_orders()

for order in orders:
    if order.status == "open":
        broker.cancel_order(order.order_id)
```

---

## GTT Orders (Good Till Triggered)

GTT orders remain active until triggered, expired, or cancelled.

### Create GTT

```python
from india_stocks_api.instruments import Equity
from india_stocks_api.constants import TransactionType, ProductType

# Get current price
quote = broker.get_quote(Equity("SBIN"))
current_price = quote.ltp

# Set trigger 5% below current
trigger_price = round(current_price * 0.95, 2)
limit_price = round(trigger_price + 5, 2)

response = broker.create_gtt(
    instrument=Equity("SBIN"),
    transaction_type=TransactionType.BUY,
    quantity=10,
    trigger_price=trigger_price,
    price=limit_price,
    product_type=ProductType.DELIVERY,
    time_period=365  # Days until expiry
)

print(f"GTT Rule ID: {response.rule_id}")
```

### List GTT Rules

```python
from india_stocks_api.constants import GTTStatus

# Get all active rules
rules = broker.get_gtt_list(status=GTTStatus.ACTIVE)

for rule in rules:
    print(f"{rule.rule_id}: {rule.symbol}")
    print(f"  Trigger: {rule.trigger_price}, Price: {rule.price}")
    print(f"  Status: {rule.status}")
```

### GTT Statuses

| Status | Description |
|--------|-------------|
| `NEW` | Newly created |
| `ACTIVE` | Active and monitoring |
| `TRIGGERED` | Condition met, order sent |
| `CANCELLED` | User cancelled |
| `EXPIRED` | Validity period ended |
| `REJECTED` | Rejected by exchange |
| `FORALL` | Filter: get all statuses |

### Modify GTT

```python
response = broker.modify_gtt(
    rule_id=123456,
    instrument=Equity("SBIN"),
    quantity=15,
    trigger_price=780.0,
    price=785.0
)
```

### Cancel GTT

```python
response = broker.cancel_gtt(
    rule_id=123456,
    instrument=Equity("SBIN")
)
```

---

## Executed Trades

View filled orders:

```python
trades = broker.get_trades()

for trade in trades:
    print(f"{trade.order_id}: {trade.symbol}")
    print(f"  {trade.transaction_type} {trade.quantity} @ {trade.price}")
    print(f"  Value: {trade.trade_value}")
    print(f"  Time: {trade.timestamp}")
```

---

## Error Handling

### Common Order Errors

```python
from india_stocks_api import ValidationError, BrokerError

try:
    response = broker.place_order(
        instrument=Equity("RELIANCE"),
        transaction_type=TransactionType.BUY,
        quantity=1,
        order_type=OrderType.LIMIT,
        product_type=ProductType.INTRADAY,
        price=2800.0
    )
except ValidationError as e:
    # Invalid parameters
    print(f"Validation error: {e}")
except BrokerError as e:
    # Broker rejected order
    print(f"Broker error: {e}")
    if e.details:
        print(f"Broker code: {e.details.get('errorcode')}")
```

### Common Error Scenarios

| Error | Cause | Solution |
|-------|-------|----------|
| Insufficient margin | Not enough funds | Add funds or reduce quantity |
| Invalid price | Price outside circuit limits | Check circuit limits |
| Market closed | Order outside trading hours | Wait for market open |
| Symbol not found | Invalid instrument | Verify symbol and expiry |

---

## Order Validation

The SDK validates orders before sending:

```python
from india_stocks_api.instruments import Index

# This raises ValidationError - cannot trade Index
try:
    broker.place_order(
        instrument=Index("NIFTY 50"),  # Not tradable!
        transaction_type=TransactionType.BUY,
        quantity=1
    )
except ValidationError as e:
    print(f"Cannot trade index: {e}")
```

---

## Next Steps

- [Portfolio & Account](portfolio-account.md) - Positions, holdings, funds
- [Production Guide](production-guide.md) - Error recovery patterns
