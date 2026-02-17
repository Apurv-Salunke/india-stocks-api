# India Stocks API v2.0

> **A unified, type-safe Python library for algorithmic trading on Indian stock exchanges**

## Why This Library Exists

Trading with Indian brokers is painful. Here are the problems v2.0 solves:

### The Problems with Traditional Broker APIs

| Problem                | Traditional Approach                                         | India Stocks API v2.0                              |
| ---------------------- | ------------------------------------------------------------ | -------------------------------------------------- |
| **Symbol Hell**        | Each broker uses different symbols (RELIANCE vs RELIANCE-EQ) | ✅ Use standard symbols - we handle broker mapping |
| **Token Management**   | Manually find and manage broker-specific tokens              | ✅ Automatic token resolution                      |
| **Database Setup**     | Download and populate instrument databases manually          | ✅ **Zero-config auto-provisioning**               |
| **Broker Lock-in**     | Rewrite code when switching brokers                          | ✅ Change brokers with one line                    |
| **Complex APIs**       | Different authentication methods per broker                  | ✅ Unified interface                               |
| **Type Safety**        | String-based parameters prone to typos                       | ✅ **Type-safe Enums and Domain Objects**          |
| **Data Inconsistency** | Different formats across brokers                             | ✅ Standardized responses                          |

### What's New in v2.0

🎉 **Transparent Auto-Provisioning**

- Instruments database downloads automatically on first use
- No manual setup, no configuration files
- Fresh data every day (staleness detection)

🎯 **Domain-Driven Design**

- Use `Equity("RELIANCE")`, `Future("NIFTY", expiry)`, `Option(...)`
- Type-safe enums: `OrderType.LIMIT`, `TransactionType.BUY`
- IDE autocomplete for all methods

🚀 **Zero-Config Experience**

```python
broker = AngelOne(api_key, client_code, password, totp)
# ✨ That's it! Instruments DB auto-downloads if needed
```

## Installation

```bash
pip install india-stocks-api
```

**Requirements**: Python 3.10+

## Quick Start

### Basic Trading

```python
from india_stocks_api.brokers import AngelOne
from india_stocks_api.instruments import Equity
from india_stocks_api.constants import TransactionType, OrderType, ProductType

# Initialize broker (instruments DB auto-downloads on first run)
broker = AngelOne(
    api_key="your_api_key",
    client_code="your_client_code",
    password="your_password",
    totp_key="your_totp_seed"
)

# Authenticate
if broker.authenticate():
    # Create instrument object (type-safe, no strings!)
    reliance = Equity("RELIANCE")

    # Place order
    order = broker.place_order(
        instrument=reliance,
        transaction_type=TransactionType.BUY,
        quantity=1,
        order_type=OrderType.MARKET,
        product_type=ProductType.INTRADAY
    )
    print(f"✅ Order placed: {order['order_id']}")
```

### Futures & Options Trading

```python
from india_stocks_api.instruments import Future, Option
from india_stocks_api.constants import OptionType
from datetime import date

# Define instruments using domain objects
nifty_future = Future("NIFTY", expiry=date(2024, 12, 26))
banknifty_call = Option(
    "BANKNIFTY",
    expiry=date(2024, 12, 26),
    strike=52000,
    opt_type=OptionType.CE
)

# Place F&O orders
broker.place_order(
    instrument=nifty_future,
    transaction_type=TransactionType.BUY,
    quantity=50,  # Lot size
    order_type=OrderType.LIMIT,
    price=23500.0,
    product_type=ProductType.INTRADAY
)
```

### Historical Data

```python
from india_stocks_api.constants import CandleInterval

# Fetch historical candles (Pandas DataFrame)
history = broker.get_history(
    instrument=Equity("TCS"),
    start_date="2024-12-01",
    end_date="2024-12-20",
    interval=CandleInterval.FIVE_MINUTE
)

print(history.head())
#    timestamp    open    high     low   close   volume
# 0  2024-12-01  3450.0  3475.0  3440.0  3470.0  125000
```

### GTT (Good-Till-Triggered) Orders

```python
# Set a GTT trigger (Angel One specific feature)
broker.create_gtt(
    instrument=Equity("RELIANCE"),
    transaction_type=TransactionType.BUY,
    quantity=10,
    trigger_price=2450.0,
    price=2455.0
)
```

## Advanced Features

### Real-time Market Data

```python
# Live price
quote = broker.get_quote(Equity("RELIANCE"))
print(f"LTP: {quote['ltp']}, Change: {quote['change_percent']}%")

# Market depth (Level 2)
depth = broker.get_depth(Equity("INFY"))
print(f"Best Bid: {depth['bids'][0]['price']} x {depth['bids'][0]['quantity']}")
```

### Portfolio Management

```python
# View positions
positions = broker.get_positions()
for pos in positions:
    print(f"{pos['symbol']}: {pos['quantity']} @ {pos['avg_price']}")

# View holdings
holdings = broker.get_holdings()

# Check funds
funds = broker.get_funds()
print(f"Available: ₹{funds['availablecash']:,.2f}")
```

### Order Management

```python
# Modify order
broker.modify_order(
    order_id="241226000123456",
    order_type=OrderType.LIMIT,
    price=2460.0,
    quantity=20
)

# Cancel order
broker.cancel_order("241226000123456")

# Get order book
orders = broker.get_orders()

# Get trade book
trades = broker.get_trades()
```

## Algorithmic Trading Example

```python
import time
from datetime import datetime

def momentum_strategy():
    """Simple momentum-based intraday strategy"""

    symbol = Equity("RELIANCE")

    while True:
        # Get current price
        quote = broker.get_quote(symbol)
        ltp = quote['ltp']

        # Get recent candles
        history = broker.get_history(
            instrument=symbol,
            start_date=datetime.now().strftime("%Y-%m-%d"),
            end_date=datetime.now().strftime("%Y-%m-%d"),
            interval=CandleInterval.ONE_MINUTE
        )

        # Calculate 20-period SMA
        if len(history) >= 20:
            sma_20 = history['close'].tail(20).mean()

            # Buy signal: Price > SMA * 1.01
            if ltp > sma_20 * 1.01:
                broker.place_order(
                    instrument=symbol,
                    transaction_type=TransactionType.BUY,
                    quantity=10,
                    order_type=OrderType.MARKET,
                    product_type=ProductType.INTRADAY
                )
                print(f"📈 BUY at {ltp}")

            # Sell signal: Price < SMA * 0.99
            elif ltp < sma_20 * 0.99:
                broker.place_order(
                    instrument=symbol,
                    transaction_type=TransactionType.SELL,
                    quantity=10,
                    order_type=OrderType.MARKET,
                    product_type=ProductType.INTRADAY
                )
                print(f"📉 SELL at {ltp}")

        time.sleep(60)  # Check every minute

# Run strategy
momentum_strategy()
```

## Architecture Highlights

### 🎯 Domain Objects (Type-Safe)

No more string parameters! Use strongly-typed objects:

```python
from india_stocks_api.instruments import Equity, Future, Option, Index
from india_stocks_api.constants import OptionType

# These are dataclasses with IDE autocomplete
equity = Equity("RELIANCE", exchange="NSE")
future = Future("NIFTY", expiry=date(2024, 12, 26), exchange="NFO")
option = Option("BANKNIFTY", date(2024, 12, 26), 52000, OptionType.CE)
index = Index("NIFTY 50")  # For quotes only, not tradeable
```

### 🔄 Auto-Provisioning (Zero Config)

On first broker instantiation:

1. ✅ Checks if `instruments.db` exists and is fresh (today's date)
2. ✅ Downloads master contract from broker if stale/missing (~200K instruments)
3. ✅ Populates SQLite database with normalized symbols
4. ✅ All this happens **transparently** in the background

Future instantiations: ⚡ Instant (uses cached DB)

### 🏗️ Modular Architecture

```
india_stocks_api/
├── brokers/         # Broker adapters (AngelOne, Zerodha, etc.)
├── instruments/     # Domain objects & database
├── constants.py     # Type-safe enums
└── internal/        # Ported OpenAlgo code (battle-tested)
```

## Supported Brokers

| Broker       | Status          | Features                               |
| ------------ | --------------- | -------------------------------------- |
| **AngelOne** | ✅ Full Support | Orders, GTT, Streaming, History, Depth |
| **Zerodha**  | 🚧 In Progress  | -                                      |
| **Upstox**   | 📋 Planned      | -                                      |
| **Fyers**    | 📋 Planned      | -                                      |

## Supported Exchanges

- **NSE** - National Stock Exchange (Equities, Indices)
- **BSE** - Bombay Stock Exchange
- **NFO** - NSE F&O (Futures & Options)
- **BFO** - BSE F&O
- **CDS** - Currency Derivatives
- **MCX** - Multi Commodity Exchange

## Documentation

### Getting Started

- 📖 [Quick Start](#quick-start) - Get trading in 5 minutes
- 🎓 [API Reference](docs/api_reference.md) - Complete method documentation
- 🔄 [Migration Guide](docs/migration-guide.md) - Upgrading from v1.x

### Advanced

- 🏗️ [Architecture](docs/design.md) - System design & patterns
- 🔌 [Broker Integration](docs/broker-integration-guide.md) - Add new brokers
- 💾 [Database Schema](docs/database-schema.md) - Instrument storage
- 🧪 [Testing Guide](tests/) - Run tests locally

## Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/Apurv-Salunke/india-stocks-api.git
cd india-stocks-api

# Install with Poetry
poetry install

# Run tests
poetry run pytest

# Auto-provision DB test
poetry run python tests/test_auto_provisioning.py
```

### Running Tests

```bash
# Basic import test
python tests/verify_import.py

# Resolution test
python tests/test_resolution.py

# Full trading workflow (requires credentials)
ANGEL_API_KEY=xxx ANGEL_CLIENT_ID=xxx ANGEL_PIN=xxx ANGEL_TOTP_SECRET=xxx \
python tests/test_trading.py
```

## Contributing

We welcome contributions! Whether it's:

- 🐛 Bug fixes
- ✨ New broker integrations
- 📚 Documentation improvements
- 🧪 Test coverage

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Roadmap

### v2.1 (Q1 2026)

- [ ] Zerodha broker adapter
- [ ] WebSocket streaming for real-time data
- [ ] Async API support (`asyncio`)

### v2.2 (Q2 2026)

- [ ] Upstox and Fyers adapters
- [ ] Multi-account management
- [ ] Built-in rate limiting

### v3.0 (Q3 2026)

- [ ] Backtesting engine integration
- [ ] Strategy builder framework
- [ ] Paper trading mode

## FAQ

**Q: Do I need to download instrument CSVs manually?**
A: No! V2.0 auto-downloads and updates instruments on first use.

**Q: Can I use the same code for multiple brokers?**
A: Yes! Just change `AngelOne(...)` to `Zerodha(...)`. Same API.

**Q: How is this different from OpenAlgo?**
A: We port OpenAlgo's battle-tested broker logic but provide a **standalone library** (no Flask, no UI dependencies).

**Q: Is the instruments database huge?**
A: ~56MB for 200K+ instruments. Downloads once, updates daily.

## License

MIT License - See [LICENSE](LICENSE) for details.

## Version

Current version: **2.0.0**

## Credits

- Built on top of [OpenAlgo](https://github.com/marketcalls/openalgo)'s excellent broker integrations
- Special thanks to the OpenAlgo community for battle-tested code

## Contact

- **Author**: Apurv Salunke
- **Email**: salunke.apurv7@gmail.com
- **GitHub**: [@Apurv-Salunke](https://github.com/Apurv-Salunke)
- **Issues**: [GitHub Issues](https://github.com/Apurv-Salunke/india-stocks-api/issues)

---

⭐ **Star this repo** if you find it useful!
