# india-stocks-api Documentation

A unified, type-safe Python SDK for programmatic trading on Indian stock exchanges.

---

## Overview

`india-stocks-api` provides a single interface to trade across multiple Indian stockbrokers. Write your trading logic once, and switch brokers by changing a single line of code.

```python
from india_stocks_api.brokers import AngelOne
from india_stocks_api.instruments import Equity
from india_stocks_api.constants import TransactionType, OrderType

broker = AngelOne(api_key, client_code, password, totp_key)
broker.authenticate()

quote = broker.get_quote(Equity("RELIANCE"))
print(f"LTP: {quote.ltp}")
```

---

## Key Features

| Feature | Description |
|---------|-------------|
| **Broker Agnostic** | Unified interface across brokers. Switch brokers without changing logic. |
| **Type Safety** | Python enums for order types, product types, and transaction types. Catch errors at development time. |
| **Domain Objects** | Trade with `Equity`, `Future`, `Option` objects instead of broker-specific token strings. |
| **Canonical Responses** | Consistent response format regardless of broker. All quotes, orders, and positions return the same structure. |
| **Auto-Provisioned Instruments** | Instrument database is downloaded and normalized automatically. No manual symbol management. |
| **Real-time Streaming** | WebSocket streaming with automatic reconnection and subscription management. |

---

## Supported Brokers

| Broker | Status | Notes |
|--------|--------|-------|
| Angel One | ✅ Complete | Full trading, data, and streaming support |
| Zerodha | 🚧 In Progress | Under development |

---

## Documentation Sections

### For SDK Users

Getting started guides, usage patterns, and production deployment guidance.

| Section | Description |
|---------|-------------|
| [Getting Started](user/getting-started.md) | Installation, authentication, first trade |
| [Core Concepts](user/core-concepts.md) | Domain objects, broker abstraction, response models |
| [Market Data](user/market-data.md) | Quotes, depth, historical candles |
| [Orders & Trading](user/orders-trading.md) | Order placement, modification, cancellation, GTT |
| [Portfolio & Account](user/portfolio-account.md) | Positions, holdings, trades, funds |
| [Streaming](user/streaming.md) | Real-time tick data via WebSocket |
| [Broker Support](user/broker-support.md) | Broker-specific capabilities and limitations |
| [Production Guide](user/production-guide.md) | Session handling, error recovery, best practices |

### For SDK Developers

Architecture, extension guides, and contribution workflow.

| Section | Description |
|---------|-------------|
| [Architecture Overview](developer/architecture.md) | System design and module responsibilities |
| [Broker Adapter Guide](developer/broker-adapter-guide.md) | How to add a new broker |
| [Instrument System](developer/instrument-system.md) | Database lifecycle and symbol resolution |
| [Session & Auth](developer/session-auth.md) | Token management and persistence |
| [Error Handling](developer/error-handling.md) | Exception hierarchy and retry patterns |
| [Testing Strategy](developer/testing.md) | Unit vs integration testing |
| [Contributing](developer/contributing.md) | Development workflow and standards |

### API Reference

Complete reference for all public classes and methods.

| Section | Description |
|---------|-------------|
| [Broker Methods](reference/broker-methods.md) | All broker class methods |
| [Domain Objects](reference/domain-objects.md) | Equity, Future, Option, Index |
| [Enums](reference/enums.md) | OrderType, TransactionType, ProductType, etc. |
| [Response Models](reference/responses.md) | QuoteResponse, OrderResponse, Position, etc. |
| [Exceptions](reference/exceptions.md) | Error classes and codes |

---

## Quick Links

- **GitHub Repository**: [github.com/Apurv-Salunke/india-stocks-api](https://github.com/Apurv-Salunke/india-stocks-api)
- **PyPI Package**: `pip install india-stocks-api`
- **Python Support**: 3.10, 3.11, 3.12

---

## Requirements

- Python 3.10 or higher
- Broker API credentials (API key, client code, PIN, TOTP secret)
- Network access to broker APIs

---

## License

MIT License
