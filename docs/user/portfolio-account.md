# Portfolio & Account

This guide covers viewing positions, holdings, trades, and account funds.

---

## Positions

Positions represent your current intraday and F&O trades.

### Get All Positions

```python
positions = broker.get_positions()

for pos in positions:
    print(f"{pos.symbol} ({pos.exchange})")
    print(f"  Product: {pos.product_type}")
    print(f"  Qty: {pos.quantity}")
    print(f"  Avg Price: {pos.average_price}")
    print(f"  LTP: {pos.ltp}")
    print(f"  P&L: {pos.pnl:+.2f}")
```

### Position Fields

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | str | Trading symbol |
| `exchange` | str | Exchange (NSE, NFO, BSE) |
| `product_type` | str | MIS, CNC, NRML |
| `quantity` | int | Net quantity (+ve long, -ve short) |
| `average_price` | float | Average entry price |
| `ltp` | float | Last traded price |
| `pnl` | float | Realized + Unrealized P&L |
| `raw` | dict | Full broker response |

### Filtering Positions

```python
# Get only F&O positions
fo_positions = [p for p in positions if p.exchange in ("NFO", "BFO", "MCX")]

# Get only profitable positions
profitable = [p for p in positions if p.pnl > 0]

# Get positions by product type
intraday = [p for p in positions if p.product_type == "MIS"]
```

---

## Holdings

Holdings represent your delivery portfolio (stocks held in demat).

### Get All Holdings

```python
holdings = broker.get_holdings()

total_investment = 0
total_current = 0

for h in holdings:
    print(f"{h.symbol}")
    print(f"  Qty: {h.quantity}")
    print(f"  Avg Price: {h.average_price}")
    print(f"  LTP: {h.ltp}")
    print(f"  P&L: {h.pnl:+.2f} ({h.pnl_percent:+.2f}%)")
    
    total_investment += h.average_price * h.quantity
    total_current += h.ltp * h.quantity

print(f"\nTotal Investment: {total_investment:,.2f}")
print(f"Current Value: {total_current:,.2f}")
print(f"Overall P&L: {total_current - total_investment:+,.2f}")
```

### Holding Fields

| Field | Type | Description |
|-------|------|-------------|
| `symbol` | str | Trading symbol |
| `exchange` | str | Exchange |
| `quantity` | int | Number of shares |
| `average_price` | float | Average buy price |
| `ltp` | float | Current market price |
| `pnl` | float | Unrealized profit/loss |
| `pnl_percent` | float | P&L as percentage |
| `raw` | dict | Full broker response |

---

## Trades

Trades represent executed orders for the day.

### Get Today's Trades

```python
trades = broker.get_trades()

for t in trades:
    print(f"{t.timestamp}: {t.symbol}")
    print(f"  {t.transaction_type} {t.quantity} @ {t.price}")
    print(f"  Value: {t.trade_value}")
    print(f"  Order ID: {t.order_id}")
```

### Trade Fields

| Field | Type | Description |
|-------|------|-------------|
| `order_id` | str | Order identifier |
| `symbol` | str | Trading symbol |
| `exchange` | str | Exchange |
| `transaction_type` | str | BUY or SELL |
| `quantity` | int | Executed quantity |
| `price` | float | Execution price |
| `trade_value` | float | Total trade value |
| `timestamp` | str | Execution time |
| `raw` | dict | Full broker response |

### Calculate Day's Turnover

```python
trades = broker.get_trades()

buy_value = sum(t.trade_value for t in trades if t.transaction_type == "BUY")
sell_value = sum(t.trade_value for t in trades if t.transaction_type == "SELL")
turnover = buy_value + sell_value

print(f"Buy Value: {buy_value:,.2f}")
print(f"Sell Value: {sell_value:,.2f}")
print(f"Total Turnover: {turnover:,.2f}")
```

---

## Funds

Check available margin and fund utilization.

### Get Funds

```python
funds = broker.get_funds()

print(f"Available Cash: {funds.available_cash:,.2f}")
print(f"Collateral: {funds.collateral:,.2f}")
print(f"Utilized: {funds.utilized_debits:,.2f}")
print(f"M2M Realized: {funds.m2m_realized:,.2f}")
print(f"M2M Unrealized: {funds.m2m_unrealized:,.2f}")
```

### FundsResponse Fields

| Field | Type | Description |
|-------|------|-------------|
| `available_cash` | float | Free cash for trading |
| `collateral` | float | Collateral margin (pledged holdings) |
| `m2m_realized` | float | Realized mark-to-market |
| `m2m_unrealized` | float | Unrealized mark-to-market |
| `utilized_debits` | float | Margin already used |

### Check Before Trading

```python
funds = broker.get_funds()
required_margin = 50000  # Example margin requirement

if funds.available_cash >= required_margin:
    # Safe to place order
    broker.place_order(...)
else:
    print(f"Insufficient funds. Available: {funds.available_cash}, Required: {required_margin}")
```

---

## Profile

Get account profile information.

```python
profile = broker.get_profile()

print(f"Client: {profile.client_code}")
print(f"Name: {profile.name}")
print(f"Email: {profile.email}")
print(f"Mobile: {profile.mobile}")
print(f"Exchanges: {', '.join(profile.exchanges)}")
print(f"Products: {', '.join(profile.products)}")
```

### ProfileResponse Fields

| Field | Type | Description |
|-------|------|-------------|
| `client_code` | str | Trading account ID |
| `name` | str | Account holder name |
| `exchanges` | tuple | Enabled exchanges |
| `products` | tuple | Enabled product types |
| `email` | str | Registered email |
| `mobile` | str | Registered mobile |
| `raw` | dict | Full broker response |

---

## Practical Examples

### Portfolio Summary

```python
def print_portfolio_summary(broker):
    # Holdings
    holdings = broker.get_holdings()
    holding_value = sum(h.ltp * h.quantity for h in holdings)
    holding_pnl = sum(h.pnl for h in holdings)
    
    # Positions
    positions = broker.get_positions()
    position_pnl = sum(p.pnl for p in positions)
    
    # Funds
    funds = broker.get_funds()
    
    print("=" * 40)
    print("PORTFOLIO SUMMARY")
    print("=" * 40)
    print(f"Holdings Value: {holding_value:>15,.2f}")
    print(f"Holdings P&L:   {holding_pnl:>15,+.2f}")
    print(f"Position P&L:   {position_pnl:>15,+.2f}")
    print(f"Available Cash: {funds.available_cash:>15,.2f}")
    print("=" * 40)
    print(f"Total P&L:      {holding_pnl + position_pnl:>15,+.2f}")
```

### Check Position Before Selling

```python
def can_sell(broker, symbol: str, quantity: int) -> bool:
    """Check if we have enough holdings to sell."""
    holdings = broker.get_holdings()
    
    for h in holdings:
        if h.symbol == symbol:
            return h.quantity >= quantity
    
    return False

# Usage
if can_sell(broker, "RELIANCE", 10):
    broker.place_order(
        instrument=Equity("RELIANCE"),
        transaction_type=TransactionType.SELL,
        quantity=10,
        product_type=ProductType.DELIVERY
    )
else:
    print("Insufficient holdings")
```

### Position P&L Report

```python
def position_report(broker):
    positions = broker.get_positions()
    
    if not positions:
        print("No open positions")
        return
    
    print(f"{'Symbol':<15} {'Qty':>8} {'Avg':>10} {'LTP':>10} {'P&L':>12}")
    print("-" * 60)
    
    total_pnl = 0
    for p in positions:
        print(f"{p.symbol:<15} {p.quantity:>8} {p.average_price:>10.2f} "
              f"{p.ltp:>10.2f} {p.pnl:>+12.2f}")
        total_pnl += p.pnl
    
    print("-" * 60)
    print(f"{'Total P&L':<45} {total_pnl:>+12.2f}")
```

### Holdings Analysis

```python
def analyze_holdings(broker):
    holdings = broker.get_holdings()
    
    # Group by performance
    gainers = [h for h in holdings if h.pnl_percent > 0]
    losers = [h for h in holdings if h.pnl_percent < 0]
    
    # Top performers
    gainers.sort(key=lambda x: x.pnl_percent, reverse=True)
    losers.sort(key=lambda x: x.pnl_percent)
    
    print("TOP GAINERS:")
    for h in gainers[:5]:
        print(f"  {h.symbol}: {h.pnl_percent:+.2f}%")
    
    print("\nTOP LOSERS:")
    for h in losers[:5]:
        print(f"  {h.symbol}: {h.pnl_percent:+.2f}%")
```

---

## Next Steps

- [Streaming](streaming.md) - Real-time tick data
- [Production Guide](production-guide.md) - Best practices
