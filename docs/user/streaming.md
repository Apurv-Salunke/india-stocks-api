# Streaming

This guide covers real-time market data streaming via WebSocket.

---

## Overview

The SDK provides WebSocket streaming for real-time tick data. Key features:

- Real-time price updates
- Multiple streaming modes (LTP, Quote, Depth)
- Automatic reconnection
- Subscription management
- Buffered subscriptions (subscribe before connecting)

---

## Basic Streaming

### Setup Callbacks

```python
from india_stocks_api.brokers import AngelOne
from india_stocks_api.instruments import Equity
from india_stocks_api.constants import StreamMode
from india_stocks_api.responses import WebSocketTick

broker = AngelOne(api_key, client_code, password, totp_key)
broker.authenticate()

def on_tick(tick: WebSocketTick):
    print(f"{tick.symbol}: LTP={tick.ltp}, Vol={tick.volume}")

def on_open():
    print("Connected to WebSocket")

def on_error(error_type, error_msg):
    print(f"Error: {error_type} - {error_msg}")

def on_close():
    print("WebSocket disconnected")

# Assign callbacks
broker.on_tick = on_tick
broker.on_open = on_open
broker.on_error = on_error
broker.on_close = on_close
```

### Subscribe and Stream

```python
# Subscribe to instruments
broker.subscribe(
    instruments=[Equity("RELIANCE"), Equity("SBIN")],
    mode=StreamMode.QUOTE
)

# Start streaming (BLOCKING call)
broker.start_streaming()
```

> **Important**: `start_streaming()` blocks the current thread. Use threading for concurrent operations.

---

## Streaming Modes

| Mode | Data Included | Use Case |
|------|---------------|----------|
| `LTP` | Last traded price only | Lightweight monitoring |
| `QUOTE` | LTP + OHLC + Volume | Standard trading |
| `SNAP_QUOTE` | Quote + Best 5 Bid/Ask | Order book visibility |
| `DEPTH` | Full 20-level depth | Market making (NSE CM only) |

### Mode Selection

```python
from india_stocks_api.constants import StreamMode

# Lightweight - LTP only
broker.subscribe([Equity("RELIANCE")], mode=StreamMode.LTP)

# Standard - includes OHLC
broker.subscribe([Equity("SBIN")], mode=StreamMode.QUOTE)

# With order book
broker.subscribe([Equity("TCS")], mode=StreamMode.SNAP_QUOTE)

# Full depth (NSE only)
broker.subscribe([Equity("INFY")], mode=StreamMode.DEPTH)
```

---

## WebSocketTick Fields

| Field | Type | Available In |
|-------|------|--------------|
| `symbol` | str | All modes |
| `ltp` | float | All modes |
| `exchange` | str | All modes |
| `token` | str | All modes |
| `mode` | str | All modes |
| `timestamp` | int | All modes |
| `ltq` | int | Quote+ |
| `open` | float | Quote+ |
| `high` | float | Quote+ |
| `low` | float | Quote+ |
| `close` | float | Quote+ |
| `volume` | int | Quote+ |
| `oi` | int | Quote+ (F&O) |
| `bid` | float | Quote+ |
| `ask` | float | Quote+ |
| `total_buy_quantity` | float | Quote+ |
| `total_sell_quantity` | float | Quote+ |
| `bids` | tuple | Snap/Depth |
| `asks` | tuple | Snap/Depth |

### Depth Levels

For `SNAP_QUOTE` and `DEPTH` modes:

```python
def on_tick(tick: WebSocketTick):
    if tick.bids:
        print("Top 5 Bids:")
        for level in tick.bids[:5]:
            print(f"  {level.price} x {level.quantity} ({level.orders} orders)")
```

---

## Subscription Management

### Subscribe After Connection

You can subscribe both before and after connection:

```python
# Subscribe before connecting
broker.subscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)

# In on_open callback (after connection)
def on_open():
    broker.subscribe([Equity("TCS")], mode=StreamMode.QUOTE)
```

### Unsubscribe

```python
broker.unsubscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)
```

### Multiple Subscriptions

Subscribe to different instruments with different modes:

```python
# High-frequency instruments - full data
broker.subscribe(
    [Equity("RELIANCE"), Equity("SBIN")],
    mode=StreamMode.QUOTE
)

# Watchlist - just LTP
broker.subscribe(
    [Equity("TCS"), Equity("INFY"), Equity("HDFC")],
    mode=StreamMode.LTP
)
```

---

## Threading

Since `start_streaming()` is blocking, use threading for concurrent operations:

```python
import threading

def streaming_thread():
    broker.start_streaming()

# Start streaming in background
thread = threading.Thread(target=streaming_thread)
thread.daemon = True  # Exit when main thread exits
thread.start()

# Main thread continues
print("Streaming started in background")
# ... do other work ...
```

### Graceful Shutdown

```python
import signal
import sys

def handle_shutdown(signum, frame):
    print("Shutting down...")
    broker.stop_streaming()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# Start streaming
broker.start_streaming()
```

---

## Reconnection

The SDK handles reconnection automatically:

- Auto-reconnects on disconnection
- Resubscribes to all previous instruments
- Configurable retry attempts and delays

### Reconnection Behavior

1. Connection lost → attempt reconnect
2. After 3 failed attempts → stop retrying
3. On successful reconnect → resubscribe all instruments
4. Call `on_open()` callback on reconnect

---

## Practical Examples

### Live Price Monitor

```python
from india_stocks_api.brokers import AngelOne
from india_stocks_api.instruments import Equity
from india_stocks_api.constants import StreamMode

broker = AngelOne(api_key, client_code, password, totp_key)
broker.authenticate()

# Track prices
prices = {}

def on_tick(tick):
    prev = prices.get(tick.symbol, tick.ltp)
    change = tick.ltp - prev
    prices[tick.symbol] = tick.ltp
    
    arrow = "▲" if change > 0 else "▼" if change < 0 else "─"
    print(f"{tick.symbol}: {tick.ltp:>10.2f} {arrow} {abs(change):>6.2f}")

broker.on_tick = on_tick

watchlist = [Equity(s) for s in ["RELIANCE", "TCS", "INFY", "HDFC", "ICICIBANK"]]
broker.subscribe(watchlist, mode=StreamMode.LTP)
broker.start_streaming()
```

### Volume Alert

```python
VOLUME_THRESHOLD = 100000  # Alert on high volume

def on_tick(tick):
    if tick.ltq >= VOLUME_THRESHOLD:
        print(f"🔔 HIGH VOLUME: {tick.symbol} - {tick.ltq} @ {tick.ltp}")

broker.on_tick = on_tick
```

### Bid-Ask Spread Monitor

```python
def on_tick(tick):
    if tick.bid and tick.ask:
        spread = tick.ask - tick.bid
        spread_pct = (spread / tick.ltp) * 100
        print(f"{tick.symbol}: Spread = {spread:.2f} ({spread_pct:.3f}%)")

broker.on_tick = on_tick
broker.subscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)
```

### Save Ticks to File

```python
import json
from datetime import datetime

def on_tick(tick):
    data = {
        "timestamp": datetime.now().isoformat(),
        "symbol": tick.symbol,
        "ltp": tick.ltp,
        "volume": tick.volume
    }
    
    with open("ticks.jsonl", "a") as f:
        f.write(json.dumps(data) + "\n")

broker.on_tick = on_tick
```

### F&O Streaming

```python
from india_stocks_api.instruments import Future, Option
from india_stocks_api.constants import OptionType, StreamMode
from datetime import date, timedelta

# Stream futures and options for a near-month expiry
today = date.today()
expiry = today + timedelta(days=30)

# Stream futures and options
instruments = [
    Future("NIFTY", expiry=expiry),
    Option("BANKNIFTY", expiry, 52000.0, OptionType.CE),
    Option("BANKNIFTY", expiry, 52000.0, OptionType.PE),
]

def on_tick(tick):
    print(f"{tick.symbol}: LTP={tick.ltp}, OI={tick.oi}")

broker.on_tick = on_tick
broker.subscribe(instruments, mode=StreamMode.QUOTE)
broker.start_streaming()
```

---

## Error Handling

```python
def on_error(error_type, error_msg):
    if "connection" in error_msg.lower():
        print(f"Connection issue: {error_msg}")
    elif "authentication" in error_msg.lower():
        print(f"Auth issue: {error_msg}")
        # May need to re-authenticate
    else:
        print(f"Error [{error_type}]: {error_msg}")

broker.on_error = on_error
```

---

## Limitations

| Limitation | Details |
|------------|---------|
| Max instruments | ~200 per connection (broker limit) |
| Depth mode | NSE CM instruments only |
| Market hours | Streaming during market hours |
| Reconnection | Limited retry attempts |

---

## Next Steps

- [Broker Support](broker-support.md) - Broker-specific limitations
- [Production Guide](production-guide.md) - Production deployment
