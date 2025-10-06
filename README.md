# 🚀 India Stocks API - Unified Broker Interface

**A single Python API for 24+ Indian stock brokers**

Built on top of OpenAlgo's battle-tested broker implementations, providing a unified, clean interface for all your trading needs.

## ✨ Features

- 🎯 **Single API** for 24+ brokers - same code works everywhere
- 🔥 **Production-tested** - uses OpenAlgo's proven implementations
- 📦 **Easy to use** - simple, Pythonic interface
- 🚀 **Fast** - HTTP/2 connection pooling, efficient caching
- 📊 **Complete** - orders, positions, historical data, live quotes
- 🛡️ **Type-safe** - full type hints support
- 🔄 **Up-to-date** - easy to sync with OpenAlgo updates

## 📦 Installation

```bash
pip install india-stocks-api
```

## 🎯 Quick Start

```python
from india_stocks_api.brokers import Broker
from datetime import datetime, timedelta

# Create broker instance (works with any broker!)
broker = Broker("angelone")

# Authenticate
broker.authenticate({
    "user_id": "A1234",
    "pin": "1234",
    "totp_secret": "BASE32SECRET",
    "api_key": "your_key"
})

# Place order
order = broker.place_order(
    symbol="RELIANCE",
    exchange="NSE",
    side="BUY",
    quantity=10,
    order_type="MARKET"
)
print(f"Order placed: {order['order_id']}")

# Get positions
positions = broker.get_positions()
for pos in positions:
    print(f"{pos['symbol']}: {pos['netqty']}")

# Get historical data
end = datetime.now()
start = end - timedelta(days=7)
candles = broker.get_historical_data("RELIANCE", "NSE", "1h", start, end)

# Get live quotes
quote = broker.get_quotes("RELIANCE", "NSE")
print(f"LTP: {quote['ltp']}, Volume: {quote['volume']}")
```

## 🏦 Supported Brokers (24+)

| Broker | Code | Status | Token Service Tested |
|--------|------|--------|---------------------|
| **AngelOne** | `angelone` or `angel` | ✅ Ready | ✅ Tested (135,806 instruments) |
| **Zerodha** | `zerodha` | ✅ Ready | ⏳ Auth Required |
| **Upstox** | `upstox` | ✅ Ready | ✅ Tested (96,670 instruments) |
| **Dhan** | `dhan` | ✅ Ready | ⏳ Pending |
| **Fyers** | `fyers` | ✅ Ready | ⏳ Pending |
| **Groww** | `groww` | ✅ Ready | ⏳ Pending |
| **Kotak** | `kotak` | ✅ Ready | ⏳ Auth Required |
| **AliceBlue** | `aliceblue` | ✅ Ready | ⏳ Pending |
| **Flattrade** | `flattrade` | ✅ Ready | ⏳ Pending |
| **Shoonya** | `shoonya` | ✅ Ready | ⏳ Pending |
| **5Paisa** | `5paisa` or `fivepaisa` | ✅ Ready | ⏳ Pending |
| **Firstock** | `firstock` | ✅ Ready | ⏳ Pending |
| **CompositeEdge** | `compositedge` | ✅ Ready | ⏳ Pending |
| **DefinEdge** | `definedge` | ✅ Ready | ⏳ Pending |
| **iBulls** | `ibulls` | ✅ Ready | ⏳ Pending |
| **IIFL** | `iifl` | ✅ Ready | ⏳ Pending |
| **IndMoney** | `indmoney` | ✅ Ready | ⏳ Pending |
| **Paytm** | `paytm` | ✅ Ready | ⏳ Pending |
| **Pocketful** | `pocketful` | ✅ Ready | ⏳ Pending |
| **TradJini** | `tradejini` | ✅ Ready | ⏳ Pending |
| **Wisdom** | `wisdom` | ✅ Ready | ⏳ Pending |
| **Zebu** | `zebu` | ✅ Ready | ⏳ Pending |
| **5PaisaXTS** | `fivepaisaxts` | ✅ Ready | ⏳ Pending |
| **Dhan Sandbox** | `dhan_sandbox` | ✅ Ready | ⏳ Pending |

### Token Service Status Legend:
- ✅ **Tested** - Token download and storage working
- ⏳ **Auth Required** - Needs authentication credentials to test
- ⏳ **Pending** - Not yet tested (no auth required)

**Switching brokers?** Just change one line:
```python
# broker = Broker("angelone")
broker = Broker("zerodha")  # That's it! Same API.
```

## 📚 API Reference

### Authentication

```python
broker = Broker("angelone")

# Most brokers
broker.authenticate({
    "user_id": "your_id",
    "pin": "your_pin",
    "totp_secret": "your_secret",
    "api_key": "your_key"
})

# Zerodha (uses request token)
broker.authenticate({
    "request_token": "request_token_from_redirect",
    "api_key": "your_key",
    "api_secret": "your_secret"
})
```

### Orders

```python
# Market order
order = broker.place_order("RELIANCE", "NSE", "BUY", 10, "MARKET")

# Limit order
order = broker.place_order(
    symbol="RELIANCE",
    exchange="NSE",
    side="BUY",
    quantity=10,
    order_type="LIMIT",
    price=2500.00,
    product="CNC"  # Delivery
)

# Stop loss order
order = broker.place_order(
    symbol="RELIANCE",
    exchange="NSE",
    side="SELL",
    quantity=10,
    order_type="SL",
    price=2400.00,
    trigger_price=2450.00
)

# Cancel order
result = broker.cancel_order(order_id)

# Get order book
orders = broker.get_order_book()

# Get trade book
trades = broker.get_trade_book()
```

### Positions & Holdings

```python
# Get positions
positions = broker.get_positions()
for pos in positions:
    print(f"{pos['symbol']}: Qty={pos['netqty']}, PnL={pos['pnl']}")

# Get holdings
holdings = broker.get_holdings()
for holding in holdings:
    print(f"{holding['symbol']}: {holding['quantity']}")
```

### Market Data

```python
# Historical data
from datetime import datetime, timedelta

end = datetime.now()
start = end - timedelta(days=30)

candles = broker.get_historical_data(
    symbol="RELIANCE",
    exchange="NSE",
    interval="1h",  # 1m, 3m, 5m, 15m, 30m, 1h, D
    from_date=start,
    to_date=end
)

for candle in candles:
    print(f"{candle['timestamp']}: O={candle['open']}, C={candle['close']}")

# Live quotes
quote = broker.get_quotes("RELIANCE", "NSE")
print(f"""
LTP: {quote['ltp']}
Open: {quote['open']}
High: {quote['high']}
Low: {quote['low']}
Volume: {quote['volume']}
""")

# Market depth (L2 data)
depth = broker.get_market_depth("RELIANCE", "NSE")
print("Bids:", depth['bids'][:5])  # Top 5 bids
print("Asks:", depth['asks'][:5])  # Top 5 asks
```

### Error Handling

```python
from india_stocks_api.brokers import (
    Broker,
    BrokerError,
    AuthenticationError,
    OrderError,
    DataError
)

try:
    broker = Broker("angelone")
    broker.authenticate(credentials)
    order = broker.place_order("RELIANCE", "NSE", "BUY", 10)
except AuthenticationError as e:
    print(f"Auth failed: {e}")
except OrderError as e:
    print(f"Order failed: {e}")
except DataError as e:
    print(f"Data fetch failed: {e}")
except BrokerError as e:
    print(f"Broker error: {e}")
```

## 🏗️ Architecture

```
india-stocks-api/
├── brokers/           # 24+ broker implementations (from OpenAlgo)
│   ├── angel/
│   ├── zerodha/
│   ├── upstox/
│   └── ...
├── utils/             # Compatibility layer
│   ├── openalgo_compat.py  # Maps to your database
│   ├── httpx_client.py     # HTTP client
│   └── logging.py          # Logging
└── unified_api.py     # Single unified interface
```

### Design Philosophy

1. **Unified Interface** - Same API for all brokers
2. **Minimal Changes** - Uses OpenAlgo code as-is
3. **Easy Updates** - Sync with OpenAlgo when they update
4. **Your Database** - Integrates with your existing data
5. **Production Ready** - Battle-tested broker implementations

## 🔄 Updating Broker Implementations

When OpenAlgo updates their broker code:

```bash
# 1. Backup your changes (if any)
git stash

# 2. Copy updated broker folders from OpenAlgo
cp -r /path/to/openalgo/broker/* india_stocks_api/brokers/

# 3. Test
python -m pytest tests/

# 4. Done!
```

No need to rewrite everything - just sync the broker folders!

## 🧪 Testing

```bash
# Run tests
pytest tests/

# Test specific broker
pytest tests/test_angelone.py

# With coverage
pytest --cov=india_stocks_api tests/
```

## 📖 Examples

See [examples/](examples/) directory for more:
- `unified_api_example.py` - Complete usage examples
- `angelone_example.py` - AngelOne specific
- `zerodha_example.py` - Zerodha specific
- `multi_broker_example.py` - Using multiple brokers

## 🤝 Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

## 🙏 Credits

Built on top of [OpenAlgo](https://github.com/marketcalls/openalgo) by Rajandran R and team.
Their excellent broker implementations power this unified API.

## 📞 Support

- 📧 Email: support@example.com
- 💬 Discord: [Join our community](https://discord.gg/example)
- 🐛 Issues: [GitHub Issues](https://github.com/yourusername/india-stocks-api/issues)

## ⭐ Star History

If you find this useful, please star the repo!

---

**Made with ❤️ for the Indian trading community**
