# Indian Stock API

## Why This Library Exists

Trading with Indian brokers is painful. Here are the problems this library solves:

### The Problems

- **Symbol Hell**: Each broker uses different symbols (RELIANCE vs RELIANCE-EQ vs RELIANCE.NS)
- **Token Management**: You need to manually find and manage broker-specific tokens
- **Broker Lock-in**: Switching brokers means rewriting your entire trading code
- **Complex APIs**: Each broker has different API structures and authentication methods
- **Request/Response Formats**: Different JSON structures, field names, and data formats
- **Data Inconsistency**: Same instrument, different data formats across brokers

### The Solution

This library provides a **unified interface** that:

- **No Symbol Management**: Use standard symbols (RELIANCE, BANKNIFTY) - we handle broker mapping
- **No Token Hassle**: We automatically resolve broker-specific tokens
- **Easy Broker Switching**: Change brokers with just one line of code
- **Consistent API**: Same methods work across all brokers
- **Standardized Data**: Uniform data format regardless of broker
- **Unified Request/Response**: Same JSON structure across all brokers

## Overview

The Indian Stock API is a Python package for trading Indian stocks, futures, options, commodities, and currencies. Connect to multiple Indian brokers, fetch real-time data, place orders, and manage your trading account - **without worrying about broker-specific details**.

## Features

- **Unified API** - Same methods work across all brokers
- **No Symbol Management** - Use standard symbols, we handle broker mapping
- **No Token Hassle** - Automatic token resolution for all instruments
- **Easy Broker Switching** - Change brokers with one line of code
- **Standardized Data** - Uniform request/response format
- **Login & Authentication** to multiple Indian brokers
- **Fetch Candles** - OHLCV data for any timeframe
- **Place Orders** - Buy/Sell stocks, futures, options
- **Real-time Data** - Live prices and market depth
- **Portfolio Management** - View positions and P&L
- **Multiple Brokers** - AngelOne, Zerodha, Upstox, and more
- **All Exchanges** - NSE, BSE, MCX, NCDEX

## Installation

```bash
pip install india-stocks-api
```

## Quick Start

### Modular Broker API (v2.0)

```python
from india_stocks_api.brokers import AngelOne
from india_stocks_api.constants import OrderType, TransactionType, ProductType

# Initialize AngelOne
broker = AngelOne(
    api_key="your_api_key",
    client_code="your_client_code",
    password="your_password",
    totp_key="your_totp_seed"
)

# Authenticate
if broker.authenticate():
    # Place a Market Order for Reliance
    from india_stocks_api.instruments.models import Equity
    reliance = Equity("RELIANCE")

    order = broker.place_order(
        instrument=reliance,
        transaction_type=TransactionType.BUY,
        quantity=1,
        order_type=OrderType.MARKET,
        product_type=ProductType.INTRADAY
    )
    print(f"Order Success: {order['order_id']}")
```

### Powerful Instrument Resolution

No more hunting for tokens! Create `Equity`, `Future`, or `Option` objects and the library handles the rest.

```python
from india_stocks_api.instruments.models import Future, Option
from india_stocks_api.constants import OptionType
from datetime import date

# Resolve a Future
nifty_fut = Future("NIFTY", expiry=date(2024, 12, 26))

# Resolve an Option
banknifty_call = Option("BANKNIFTY", expiry=date(2024, 12, 26), strike=52000, opt_type=OptionType.CE)

# Fetch History
history = broker.get_history(nifty_fut, start_date="2024-12-01", end_date="2024-12-20", interval="5m")
```

### GTT (Good-Till-Triggered) Support

```python
# Create a GTT Buy Rule
broker.create_gtt(
    instrument=reliance,
    transaction_type=TransactionType.BUY,
    quantity=10,
    trigger_price=2450.0,
    price=2455.0
)
```

### Get Live Prices

```python
# Get current price
price = angelone.get_quote("RELIANCE", "NSE")
print(f"RELIANCE: {price['last_price']}")

# Get multiple quotes
quotes = angelone.get_quotes([
    {"symbol": "RELIANCE", "exchange": "NSE"},
    {"symbol": "TCS", "exchange": "NSE"},
    {"symbol": "INFY", "exchange": "NSE"}
])

for quote in quotes:
    print(f"{quote['symbol']}: {quote['last_price']}")
```

### View Portfolio

```python
# Get positions
positions = angelone.get_positions()
print("Current Positions:")
for position in positions:
    print(f"{position['symbol']}: {position['quantity']} @ {position['average_price']}")

# Get holdings
holdings = angelone.get_holdings()
print("Holdings:")
for holding in holdings:
    print(f"{holding['symbol']}: {holding['quantity']} shares")

# Get P&L
pnl = angelone.get_pnl()
print(f"Total P&L: {pnl['total_pnl']}")
```

## Advanced Trading

### Futures & Options Trading

```python
# Buy BANKNIFTY future
fno_order = angelone.place_order(
    symbol="BANKNIFTY28OCT25FUT",
    exchange="NSE",
    transaction_type="BUY",
    quantity=25,  # Lot size
    order_type="MARKET",
    product="MIS"
)

# Buy BANKNIFTY option
option_order = angelone.place_order(
    symbol="BANKNIFTY28OCT2545000CE",
    exchange="NSE",
    transaction_type="BUY",
    quantity=25,
    order_type="LIMIT",
    price=150.00,
    product="MIS"
)
```

### Commodity Trading

```python
# Buy GOLD commodity
gold_order = angelone.place_order(
    symbol="GOLD",
    exchange="MCX",
    transaction_type="BUY",
    quantity=1,  # 1 kg
    order_type="MARKET",
    product="INTRADAY"
)
```

### Algorithmic Trading Example

```python
import time
from datetime import datetime

# Simple moving average strategy
def trading_strategy():
    while True:
        # Get current price
        price = angelone.get_quote("RELIANCE", "NSE")
        current_price = price['last_price']

        # Get 20-period SMA
        candles = angelone.get_candles("RELIANCE", "NSE", "1minute", limit=20)
        sma_20 = sum(c['close'] for c in candles) / len(candles)

        # Trading logic
        if current_price > sma_20 * 1.01:  # Price 1% above SMA
            angelone.place_order("RELIANCE", "NSE", "SELL", 10, "MARKET", "INTRADAY")
            print(f"Sold RELIANCE at {current_price}")
        elif current_price < sma_20 * 0.99:  # Price 1% below SMA
            angelone.place_order("RELIANCE", "NSE", "BUY", 10, "MARKET", "INTRADAY")
            print(f"Bought RELIANCE at {current_price}")

        time.sleep(60)  # Check every minute

# Run strategy
trading_strategy()
```

## Error Handling

```python
try:
    order = angelone.place_order("RELIANCE", "NSE", "BUY", 10, "MARKET", "INTRADAY")
    print(f"Order successful: {order['order_id']}")
except Exception as e:
    print(f"Order failed: {e}")
    # Handle insufficient funds, invalid symbol, etc.
```

## Supported Brokers

- **AngelOne** - Full support for all features
- **Zerodha** - Coming soon
- **Upstox** - Coming soon
- **ICICI Direct** - Coming soon
- **5Paisa** - Coming soon

## Supported Exchanges

- **NSE** - National Stock Exchange
- **BSE** - Bombay Stock Exchange
- **MCX** - Multi Commodity Exchange
- **NCDEX** - National Commodity & Derivatives Exchange

## Common Use Cases

### Day Trading

```python
# Quick day trading setup - works with any broker
broker = brokers.AngelOne()  # or brokers.Zerodha()
broker.login("api_key", "username", "password")

# Get intraday candles - same code for all brokers
candles = broker.get_candles("RELIANCE", "NSE", "5minute", limit=100)

# Place quick order - unified API
order = broker.place_order("RELIANCE", "NSE", "BUY", 10, "MARKET", "INTRADAY")
```

### Swing Trading

```python
# Swing trading with daily candles - broker agnostic
daily_candles = broker.get_candles("RELIANCE", "NSE", "1day", limit=50)

# Place delivery order - same method for all brokers
order = broker.place_order("RELIANCE", "NSE", "BUY", 100, "LIMIT", "DELIVERY", price=2400)
```

### Options Trading

```python
# Buy call option - standard symbol, we handle broker mapping
call_order = broker.place_order("BANKNIFTY28OCT2545000CE", "NSE", "BUY", 25, "MARKET", "MIS")

# Buy put option - same API across brokers
put_order = broker.place_order("BANKNIFTY28OCT2545000PE", "NSE", "BUY", 25, "MARKET", "MIS")
```

### Multi-Broker Strategy

```python
# Run same strategy across multiple brokers
brokers_list = [brokers.AngelOne(), brokers.Zerodha()]

for broker in brokers_list:
    broker.login("credentials...")

    # Same trading logic works for all brokers
    candles = broker.get_candles("RELIANCE", "NSE", "1minute", limit=20)
    if candles[-1]['close'] > candles[-2]['close']:
        broker.place_order("RELIANCE", "NSE", "BUY", 10, "MARKET", "INTRADAY")
```

## Documentation

### For End Users

- **[Quick Start Guide](#quick-start)** - Get started in minutes
- **[API Reference](docs/api-reference.md)** - Complete API documentation with examples
- **[Migration Guide](docs/migration-guide.md)** - Upgrade instructions and troubleshooting

### For Developers

- **[Architecture Overview](docs/architecture.md)** - System design and components
- **[Broker Integration Guide](docs/broker-integration-guide.md)** - Add support for new brokers
- **[Database Schema](docs/database-schema.md)** - Database structure and relationships
- **[Design Philosophy](docs/philosophy.md)** - Core principles and design decisions

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Version

Current version: 1.0.0

## Contact

For any inquiries, please contact [me](salunke.apurv7@gmail.com) or open an issue in the repository.
