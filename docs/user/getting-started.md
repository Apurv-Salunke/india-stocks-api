# Getting Started

This guide walks you through installation, authentication, and your first trade.

---

## Installation

### From PyPI

```bash
pip install india-stocks-api
```

### From Source

```bash
git clone https://github.com/Apurv-Salunke/india-stocks-api.git
cd india-stocks-api
pip install .
```

### Development Installation

```bash
git clone https://github.com/Apurv-Salunke/india-stocks-api.git
cd india-stocks-api
poetry install
```

---

## Broker Credentials

Before using the SDK, you need credentials from your broker. For Angel One:

| Credential | Description | Where to Get |
|------------|-------------|--------------|
| `api_key` | Application API key | Angel One SmartAPI Dashboard |
| `client_code` | Your trading account ID | Your Angel One login ID |
| `password` | 4-digit MPIN | Set in Angel One app |
| `totp_key` | TOTP secret (base32) | Angel One app → Settings → TOTP |

### Environment Setup

Create a `.env` file in your project root:

```bash
ANGEL_API_KEY=your_api_key_here
ANGEL_CLIENT_ID=your_client_code
ANGEL_PIN=1234
ANGEL_TOTP_SECRET=your_totp_base32_secret
```

Load credentials in your code:

```python
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("ANGEL_API_KEY")
client_code = os.getenv("ANGEL_CLIENT_ID")
password = os.getenv("ANGEL_PIN")
totp_key = os.getenv("ANGEL_TOTP_SECRET")
```

> **Security**: Never commit `.env` files to version control. Add `.env` to your `.gitignore`.

---

## Authentication

### Basic Authentication

```python
from india_stocks_api.brokers import AngelOne

broker = AngelOne(
    api_key=api_key,
    client_code=client_code,
    password=password,
    totp_key=totp_key
)

# Authenticate with the broker
broker.authenticate()
print("Authenticated successfully")
```

### What Happens During Authentication

1. TOTP code is generated automatically from your `totp_key`
2. Credentials are sent to the broker API
3. JWT access token and feed token are returned
4. Tokens are stored in memory and persisted to `_cache/sessions.json`
5. Session expires at midnight IST

### Error Handling

```python
from india_stocks_api import AuthenticationError

try:
    broker.authenticate()
except AuthenticationError as e:
    print(f"Authentication failed: {e}")
    # Check credentials and try again
```

---

## Your First Quote

```python
from india_stocks_api.instruments import Equity

# Define the instrument
reliance = Equity("RELIANCE")

# Fetch real-time quote
quote = broker.get_quote(reliance)

print(f"Symbol: RELIANCE")
print(f"LTP: {quote.ltp}")
print(f"Bid: {quote.bid} | Ask: {quote.ask}")
print(f"Open: {quote.open} | High: {quote.high} | Low: {quote.low}")
print(f"Volume: {quote.volume}")
```

Output:
```
Symbol: RELIANCE
LTP: 2845.50
Bid: 2845.35 | Ask: 2845.65
Open: 2830.00 | High: 2860.10 | Low: 2825.00
Volume: 12345678
```

---

## Placing Your First Order

> **Warning**: The following code places a real order with real money. Use caution.

```python
from india_stocks_api.instruments import Equity
from india_stocks_api.constants import TransactionType, OrderType, ProductType

# Define instrument
sbin = Equity("SBIN")

# Place a market order
response = broker.place_order(
    instrument=sbin,
    transaction_type=TransactionType.BUY,
    quantity=1,
    order_type=OrderType.MARKET,
    product_type=ProductType.INTRADAY
)

print(f"Order ID: {response.order_id}")
print(f"Status: {response.status}")
print(f"Message: {response.message}")
```

Output:
```
Order ID: 240115000012345
Status: success
Message: Order placed successfully
```

---

## Streaming Real-Time Ticks

```python
from india_stocks_api.constants import StreamMode
from india_stocks_api.instruments import Equity

# Define callback for incoming ticks
def on_tick(tick):
    print(f"{tick.symbol}: LTP={tick.ltp}, Vol={tick.volume}")

# Set callbacks
broker.on_tick = on_tick

# Subscribe to instruments
broker.subscribe([Equity("RELIANCE"), Equity("SBIN")], mode=StreamMode.QUOTE)

# Start streaming (blocking call)
broker.start_streaming()
```

Output:
```
RELIANCE: LTP=2845.50, Vol=12345678
SBIN: LTP=825.40, Vol=9876543
RELIANCE: LTP=2845.55, Vol=12345700
...
```

> **Note**: `start_streaming()` is a blocking call. Run it in a separate thread if you need concurrent execution.

---

## Complete Example

```python
"""Complete example: authenticate, get quote, place order."""
import os
from dotenv import load_dotenv

from india_stocks_api.brokers import AngelOne
from india_stocks_api.instruments import Equity
from india_stocks_api.constants import TransactionType, OrderType, ProductType
from india_stocks_api import AuthenticationError, BrokerError

# Load credentials
load_dotenv()

def main():
    # Initialize broker
    broker = AngelOne(
        api_key=os.getenv("ANGEL_API_KEY"),
        client_code=os.getenv("ANGEL_CLIENT_ID"),
        password=os.getenv("ANGEL_PIN"),
        totp_key=os.getenv("ANGEL_TOTP_SECRET")
    )

    try:
        # Authenticate
        broker.authenticate()
        print("✓ Authenticated")

        # Get quote
        sbin = Equity("SBIN")
        quote = broker.get_quote(sbin)
        print(f"✓ SBIN LTP: {quote.ltp}")

        # Check funds
        funds = broker.get_funds()
        print(f"✓ Available cash: {funds.available_cash}")

        # Place order (uncomment to execute)
        # response = broker.place_order(
        #     instrument=sbin,
        #     transaction_type=TransactionType.BUY,
        #     quantity=1,
        #     order_type=OrderType.MARKET,
        #     product_type=ProductType.INTRADAY
        # )
        # print(f"✓ Order placed: {response.order_id}")

    except AuthenticationError as e:
        print(f"✗ Authentication failed: {e}")
    except BrokerError as e:
        print(f"✗ Broker error: {e}")

if __name__ == "__main__":
    main()
```

---

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `AuthenticationError: Invalid TOTP secret` | Incorrect TOTP key | Regenerate TOTP in broker app |
| `AuthenticationError: Authentication failed` | Wrong credentials | Verify API key, client code, PIN |
| `SessionExpiredError` | Session past midnight IST | Call `authenticate()` again |
| `ValidationError: instrument token not found` | Invalid symbol | Check symbol spelling and exchange |

---

## Next Steps

- [Core Concepts](core-concepts.md) - Understand domain objects and response models
- [Market Data](market-data.md) - Fetch quotes, depth, and historical data
- [Orders & Trading](orders-trading.md) - Place, modify, and cancel orders
