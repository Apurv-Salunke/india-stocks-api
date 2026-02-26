# Market Data

This guide covers fetching real-time quotes, market depth, and historical candles.

---

## Real-Time Quotes

### Basic Quote

```python
from india_stocks_api.instruments import Equity

quote = broker.get_quote(Equity("RELIANCE"))

print(f"LTP: {quote.ltp}")
print(f"Bid: {quote.bid} | Ask: {quote.ask}")
print(f"Open: {quote.open} | High: {quote.high} | Low: {quote.low}")
print(f"Previous Close: {quote.prev_close}")
print(f"Volume: {quote.volume}")
```

### QuoteResponse Fields

| Field | Type | Description |
|-------|------|-------------|
| `ltp` | float | Last traded price |
| `bid` | float | Best bid price |
| `ask` | float | Best ask price |
| `open` | float | Day's opening price |
| `high` | float | Day's high |
| `low` | float | Day's low |
| `prev_close` | float | Previous day's close |
| `volume` | int | Total traded volume |
| `oi` | int | Open interest (F&O instruments) |

### Quotes for Different Instruments

```python
from india_stocks_api.instruments import Equity, Future, Option, Index
from india_stocks_api.constants import OptionType
from datetime import date

# Equity
quote = broker.get_quote(Equity("SBIN"))

# Index (non-tradable, data only)
nifty_quote = broker.get_quote(Index("NIFTY 50"))

# Future
fut_quote = broker.get_quote(Future("NIFTY", expiry=date(2024, 12, 26)))

# Option
opt_quote = broker.get_quote(Option(
    symbol="BANKNIFTY",
    expiry=date(2024, 12, 26),
    strike=52000.0,
    opt_type=OptionType.CE
))
```

---

## Market Depth

Market depth provides Level 2 order book data with bid/ask quantities at multiple price levels.

### Basic Depth

```python
depth = broker.get_depth(Equity("RELIANCE"))

print("Top Bids:")
for i, bid in enumerate(depth.bids[:5]):
    print(f"  {i+1}. {bid.price} x {bid.quantity}")

print("Top Asks:")
for i, ask in enumerate(depth.asks[:5]):
    print(f"  {i+1}. {ask.price} x {ask.quantity}")

print(f"Total Buy Qty: {depth.total_buy_qty}")
print(f"Total Sell Qty: {depth.total_sell_qty}")
```

Output:

```text
Top Bids:
  1. 2845.35 x 500
  2. 2845.30 x 1200
  3. 2845.25 x 800
  4. 2845.20 x 350
  5. 2845.15 x 1500
Top Asks:
  1. 2845.45 x 600
  2. 2845.50 x 900
  3. 2845.55 x 1500
  4. 2845.60 x 200
  5. 2845.65 x 750
Total Buy Qty: 45000
Total Sell Qty: 38000
```

### DepthResponse Fields

| Field | Type | Description |
|-------|------|-------------|
| `bids` | tuple[DepthLevel] | Bid levels (price, quantity) |
| `asks` | tuple[DepthLevel] | Ask levels (price, quantity) |
| `ltp` | float | Last traded price |
| `ltq` | int | Last traded quantity |
| `open` | float | Opening price |
| `high` | float | Day's high |
| `low` | float | Day's low |
| `prev_close` | float | Previous close |
| `volume` | int | Total volume |
| `oi` | int | Open interest |
| `total_buy_qty` | int | Total buy quantity |
| `total_sell_qty` | int | Total sell quantity |

---

## Historical Data

### Fetching Candles

```python
from india_stocks_api.constants import CandleInterval
from datetime import date, timedelta

# Define date range
end_date = date.today().strftime("%Y-%m-%d")
start_date = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")

# Fetch 5-minute candles
history = broker.get_history(
    instrument=Equity("RELIANCE"),
    start_date=start_date,
    end_date=end_date,
    interval=CandleInterval.FIVE_MINUTE
)

print(f"Fetched {len(history.candles)} candles")

# Access individual candles
for candle in history.candles[-5:]:  # Last 5 candles
    print(f"{candle.timestamp}: O={candle.open} H={candle.high} L={candle.low} C={candle.close} V={candle.volume}")
```

### Available Intervals

| Enum | Symbol | Description |
|------|--------|-------------|
| `ONE_MINUTE` | 1m | 1-minute candles |
| `THREE_MINUTE` | 3m | 3-minute candles |
| `FIVE_MINUTE` | 5m | 5-minute candles |
| `TEN_MINUTE` | 10m | 10-minute candles |
| `FIFTEEN_MINUTE` | 15m | 15-minute candles |
| `THIRTY_MINUTE` | 30m | 30-minute candles |
| `ONE_HOUR` | 1h | 1-hour candles |
| `ONE_DAY` | D | Daily candles |

### Converting to DataFrame

The `HistoryResponse` includes a convenience method for pandas:

```python
history = broker.get_history(
    instrument=Equity("RELIANCE"),
    start_date="2024-01-01",
    end_date="2024-01-31",
    interval=CandleInterval.ONE_DAY
)

# Convert to pandas DataFrame
df = history.to_dataframe()
print(df.head())
```

Output:

```text
    timestamp    open    high     low   close    volume  oi
0  1704067200  2820.0  2830.5  2815.0  2825.0   8234567   0
1  1704153600  2825.0  2850.0  2820.0  2845.0  10234567   0
2  1704240000  2845.0  2860.0  2840.0  2852.0   9234567   0
```

### Candle Fields

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | int | Unix timestamp |
| `open` | float | Opening price |
| `high` | float | High price |
| `low` | float | Low price |
| `close` | float | Closing price |
| `volume` | int | Volume |
| `oi` | int | Open interest |

---

## Historical Data Limitations

### Date Range Limits

Different intervals have different maximum date ranges:

| Interval | Maximum Range |
|----------|---------------|
| 1m | 30 days |
| 5m | 60 days |
| 15m | 90 days |
| 1h | 365 days |
| D | No limit |

### Missing Data

- Market holidays have no candles
- Pre-market and post-market data may be limited
- Very recent candles (last few minutes) may be delayed

### Handling Errors

```python
from india_stocks_api import BrokerError

try:
    history = broker.get_history(
        instrument=Equity("INVALID_SYMBOL"),
        start_date="2024-01-01",
        end_date="2024-01-31",
        interval=CandleInterval.ONE_DAY
    )
except BrokerError as e:
    print(f"Failed to fetch history: {e}")
```

---

## Index Data

Indices are non-tradable but you can fetch their quotes and history:

```python
from india_stocks_api.instruments import Index

# Get NIFTY 50 quote
nifty = Index("NIFTY 50")
quote = broker.get_quote(nifty)
print(f"NIFTY 50: {quote.ltp}")

# Get NIFTY historical data
history = broker.get_history(
    instrument=nifty,
    start_date="2024-01-01",
    end_date="2024-01-31",
    interval=CandleInterval.ONE_DAY
)
```

### Common Index Symbols

| Symbol | Description |
|--------|-------------|
| `NIFTY 50` | NIFTY 50 Index |
| `NIFTY BANK` | Bank NIFTY Index |
| `NIFTY FIN SERVICE` | FINNIFTY Index |
| `NIFTY NEXT 50` | NIFTY Next 50 |
| `INDIA VIX` | India VIX |

---

## F&O Data

### Futures

```python
from india_stocks_api.instruments import Future
from datetime import date

# Current month NIFTY future
nifty_fut = Future("NIFTY", expiry=date(2024, 12, 26))

quote = broker.get_quote(nifty_fut)
print(f"NIFTY FUT: {quote.ltp}, OI: {quote.oi}")

history = broker.get_history(
    instrument=nifty_fut,
    start_date="2024-12-01",
    end_date="2024-12-20",
    interval=CandleInterval.FIFTEEN_MINUTE
)
```

### Options

```python
from india_stocks_api.instruments import Option
from india_stocks_api.constants import OptionType
from datetime import date

# BANKNIFTY 52000 CE
opt = Option(
    symbol="BANKNIFTY",
    expiry=date(2024, 12, 26),
    strike=52000.0,
    opt_type=OptionType.CE
)

quote = broker.get_quote(opt)
print(f"BANKNIFTY 52000CE: {quote.ltp}, OI: {quote.oi}")
```

---

## Practical Examples

### Get Multiple Quotes

```python
from india_stocks_api.instruments import Equity

symbols = ["RELIANCE", "TCS", "INFY", "HDFC", "ICICIBANK"]

for sym in symbols:
    quote = broker.get_quote(Equity(sym))
    print(f"{sym}: {quote.ltp} ({quote.ltp - quote.prev_close:+.2f})")
```

### Calculate VWAP

```python
from india_stocks_api.constants import CandleInterval
from datetime import date

history = broker.get_history(
    instrument=Equity("RELIANCE"),
    start_date=date.today().strftime("%Y-%m-%d"),
    end_date=date.today().strftime("%Y-%m-%d"),
    interval=CandleInterval.ONE_MINUTE
)

df = history.to_dataframe()
df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
df['vwap'] = (df['typical_price'] * df['volume']).cumsum() / df['volume'].cumsum()

print(f"Current VWAP: {df['vwap'].iloc[-1]:.2f}")
```

---

## Next Steps

- [Orders & Trading](orders-trading.md) - Place, modify, and cancel orders
- [Streaming](streaming.md) - Real-time tick data via WebSocket
