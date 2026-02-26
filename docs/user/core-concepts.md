# Core Concepts

This guide explains the fundamental concepts you need to understand when using the SDK.

---

## Domain Objects

Domain objects represent tradable financial instruments. Instead of dealing with broker-specific token strings like `"2885"` or cryptic symbols like `"NIFTY24DEC22000CE"`, you work with intuitive Python objects.

### Equity

Represents stocks and ETFs traded on NSE or BSE.

```python
from india_stocks_api.instruments import Equity

reliance = Equity("RELIANCE")           # NSE by default
reliance_bse = Equity("RELIANCE", exchange="BSE")
```

### Future

Represents futures contracts on indices or stocks.

```python
from india_stocks_api.instruments import Future
from datetime import date

nifty_fut = Future("NIFTY", expiry=date(2024, 12, 26))
reliance_fut = Future("RELIANCE", expiry=date(2024, 12, 26))
```

### Option

Represents options contracts with strike price and type.

```python
from india_stocks_api.instruments import Option
from india_stocks_api.constants import OptionType
from datetime import date

nifty_call = Option(
    symbol="NIFTY",
    expiry=date(2024, 12, 26),
    strike=22000.0,
    opt_type=OptionType.CE
)

nifty_put = Option(
    symbol="NIFTY",
    expiry=date(2024, 12, 26),
    strike=22000.0,
    opt_type=OptionType.PE
)
```

### Index

Represents market indices. Used for data fetching only—cannot be traded directly.

```python
from india_stocks_api.instruments import Index

nifty = Index("NIFTY 50")
banknifty = Index("NIFTY BANK")
```

> **Note**: Attempting to place an order on an `Index` will raise a `ValidationError`.

---

## Instrument Resolution

When you create a domain object, the SDK automatically resolves it to the broker's internal token.

```python
# You write:
reliance = Equity("RELIANCE")
broker.get_quote(reliance)

# SDK internally:
# 1. Looks up "RELIANCE" on "NSE" in instrument database
# 2. Finds broker token (e.g., "2885")
# 3. Sends token to broker API
# 4. Returns normalized response
```

The instrument database is downloaded automatically when you create a broker instance (if it's missing or outdated).

---

## Canonical Responses

All broker methods return standardized response objects. The same code works across brokers without changes.

### Quote Response

```python
quote = broker.get_quote(Equity("RELIANCE"))

# QuoteResponse fields:
quote.ltp         # Last traded price
quote.bid         # Best bid price
quote.ask         # Best ask price
quote.open        # Day's open price
quote.high        # Day's high price
quote.low         # Day's low price
quote.prev_close  # Previous day's close
quote.volume      # Traded volume
quote.oi          # Open interest (for F&O)
```

### Order Response

```python
response = broker.place_order(...)

# OrderResponse fields:
response.order_id  # Broker-assigned order ID
response.status    # "success" or "error"
response.message   # Human-readable message
```

### Position

```python
positions = broker.get_positions()

for pos in positions:
    pos.symbol         # Trading symbol
    pos.exchange       # Exchange (NSE, NFO, etc.)
    pos.product_type   # MIS, CNC, NRML
    pos.quantity       # Net quantity
    pos.average_price  # Average entry price
    pos.ltp            # Last traded price
    pos.pnl            # Profit/loss
```

### Holding

```python
holdings = broker.get_holdings()

for holding in holdings:
    holding.symbol         # Trading symbol
    holding.exchange       # Exchange
    holding.quantity       # Number of shares
    holding.average_price  # Average buy price
    holding.ltp            # Current price
    holding.pnl            # Profit/loss
    holding.pnl_percent    # P&L percentage
```

---

## Type-Safe Enums

The SDK uses enums instead of strings to prevent errors and enable IDE autocompletion.

### Order Types

```python
from india_stocks_api.constants import OrderType

OrderType.MARKET  # Execute at market price
OrderType.LIMIT   # Execute at specified price or better
OrderType.SL      # Stop-loss limit order
OrderType.SLM     # Stop-loss market order
```

### Transaction Types

```python
from india_stocks_api.constants import TransactionType

TransactionType.BUY   # Buy order
TransactionType.SELL  # Sell order
```

### Product Types

```python
from india_stocks_api.constants import ProductType

ProductType.INTRADAY     # MIS - Squared off by end of day
ProductType.DELIVERY     # CNC - Delivery/holding
ProductType.CARRYFORWARD # NRML - Carry forward (F&O)
```

### Candle Intervals

```python
from india_stocks_api.constants import CandleInterval

CandleInterval.ONE_MINUTE      # 1m
CandleInterval.FIVE_MINUTE     # 5m
CandleInterval.FIFTEEN_MINUTE  # 15m
CandleInterval.ONE_HOUR        # 1h
CandleInterval.ONE_DAY         # D
```

### Stream Modes

```python
from india_stocks_api.constants import StreamMode

StreamMode.LTP         # Last traded price only
StreamMode.QUOTE       # LTP + OHLC + Volume
StreamMode.SNAP_QUOTE  # Quote + Best 5 Bid/Ask
StreamMode.DEPTH       # Full 20-level depth (NSE CM only)
```

---

## Session Lifecycle

### Authentication Flow

```text
authenticate()
    ↓
TOTP generated automatically
    ↓
Credentials sent to broker
    ↓
JWT + Feed tokens returned
    ↓
Session persisted to _cache/sessions.json
    ↓
Session valid until midnight IST
```

### Session Expiry

Sessions expire at midnight IST. Calling any method after expiry raises `SessionExpiredError`.

```python
from india_stocks_api import SessionExpiredError

try:
    quote = broker.get_quote(Equity("RELIANCE"))
except SessionExpiredError:
    broker.authenticate()  # Re-authenticate
    quote = broker.get_quote(Equity("RELIANCE"))
```

### Session Persistence

Session tokens are stored in `_cache/sessions.json`. This file:

- Contains access token, feed token, client code
- Does **not** contain passwords or TOTP secrets
- Has restricted permissions (chmod 600)

---

## Broker Abstraction

The SDK provides a common interface across all brokers.

### Same Code, Different Brokers

```python
# Angel One
from india_stocks_api.brokers import AngelOne
broker = AngelOne(api_key, client_code, password, totp_key)

# Zerodha (when available)
# from india_stocks_api.brokers import Zerodha
# broker = Zerodha(api_key, api_secret, request_token)

# Same operations work regardless of broker
broker.authenticate()
quote = broker.get_quote(Equity("RELIANCE"))
```

### Factory Pattern

You can also create brokers dynamically:

```python
from india_stocks_api.brokers import BaseBroker

broker = BaseBroker.create(
    "angel",
    api_key=api_key,
    client_code=client_code,
    password=password,
    totp_key=totp_key
)
```

---

## Error Handling Model

All SDK exceptions inherit from `ISAError` and include:

- Human-readable message
- Standardized error code
- Optional details (broker response, etc.)

```python
from india_stocks_api import AuthenticationError, ValidationError, BrokerError

try:
    broker.place_order(...)
except ValidationError as e:
    print(f"Invalid request: {e}")
    print(f"Error code: {e.code}")
except BrokerError as e:
    print(f"Broker rejected: {e}")
    print(f"Broker code: {e.details.get('errorcode')}")
```

See [Production Guide](production-guide.md) for error handling patterns.

---

## Auto-Provisioning

The SDK automatically manages the instrument database.

### When Database Is Downloaded

- First time a broker instance is created
- When existing database is older than today

### What Gets Downloaded

- Complete list of tradable instruments from the broker
- Stored in `_cache/instruments.db` (SQLite)
- Takes ~5-10 seconds on first run

### Manual Download (Rarely Needed)

```python
# Database is downloaded automatically, but you can force it:
broker._download_master_contract()
```

---

## Next Steps

- [Market Data](market-data.md) - Fetch quotes, depth, and historical data
- [Orders & Trading](orders-trading.md) - Place, modify, and cancel orders
- [Streaming](streaming.md) - Real-time tick data
