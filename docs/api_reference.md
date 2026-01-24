# API Reference

## Broker Initialization

The library uses a modular factory pattern. You can either instantiate a broker class directly or use the `BaseBroker.create()` method.

### Angel One

```python
from india_stocks_api.brokers import AngelOne

broker = AngelOne(
    api_key="your_api_key",
    client_code="your_client_code",
    password="your_pin",
    totp_key="your_totp_seed"
)

if broker.authenticate():
    print("Logged in!")
```

---

## Domain Objects (Instruments)

Always use Domain Objects to represent what you want to trade. This avoids "String Hell".

### Equity

```python
from india_stocks_api.instruments import Equity
reliance = Equity("RELIANCE") # Defaults to NSE
```

### Future

```python
from datetime import date
from india_stocks_api.instruments import Future
nifty_fut = Future("NIFTY", expiry=date(2024, 12, 26))
```

### Option

```python
from india_stocks_api.instruments import Option
from india_stocks_api.constants import OptionType
banknifty_call = Option("BANKNIFTY", expiry=date(2024, 12, 26), strike=52000, opt_type=OptionType.CE)
```

---

## Trading Operations

### Place Order

```python
from india_stocks_api.constants import TransactionType, OrderType, ProductType

order = broker.place_order(
    instrument=reliance,
    transaction_type=TransactionType.BUY,
    quantity=1,
    order_type=OrderType.MARKET,
    product_type=ProductType.INTRADAY
)
print(f"Order ID: {order['order_id']}")
```

### GTT (Good-Till-Triggered)

```python
gtt_order = broker.create_gtt(
    instrument=reliance,
    transaction_type=TransactionType.BUY,
    quantity=10,
    trigger_price=2450.0,
    price=2455.0
)
```

---

## Data Fetching

### Real-time Quotes

```python
quote = broker.get_quote(reliance)
print(f"LTP: {quote['ltp']}")
```

### Historical Data (Candles)

```python
from india_stocks_api.constants import CandleInterval

history = broker.get_history(
    instrument=reliance,
    start_date="2024-12-01",
    end_date="2024-12-20",
    interval=CandleInterval.FIVE_MINUTE
)
# Returns a Pandas DataFrame
print(history.head())
```

---

## Account Info

### Positions & Holdings

```python
positions = broker.get_positions()
holdings = broker.get_holdings()
```

### Funds & Margin

```python
funds = broker.get_funds()
print(f"Available Cash: {funds['availablecash']}")
```
