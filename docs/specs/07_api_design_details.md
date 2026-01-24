# API Design: Unified Order & Data Interface

This document specifies the high-level API for placing orders and retrieving market data using the `openalgo-brokers` package.

## 1. Place Order API (Instrument-First)

The `place_order` method accepts either a string symbol OR an `Instrument` object.

### Signature

```python
def place_order(
    self,
    instrument: Union[str, Instrument], # Support both String and Object
    transaction_type: TransactionType,
    quantity: int,
    order_type: OrderType = OrderType.MARKET,
    product_type: ProductType = ProductType.INTRADAY,
    price: float = 0.0,
    trigger_price: float = 0.0,
    validity: OrderValidity = OrderValidity.DAY,
    disclosed_quantity: int = 0,
    tag: str = None,
    **kwargs
) -> OrderResponse:
```

### Validation Logic

1.  **Type Check**: If `instrument` is an instance of `Index`, the method immediately raises `TypeError("Cannot place order on non-tradable Index")`.
2.  **Resolution**:
    - If `Instrument` object: Uses `instrument.symbol`, `instrument.expiry`, etc., to resolve the Token ID from the internal DB.
    - If `str`: Performs a standard lookup assuming it is a Tradingsymbol.

### Usage Examples

**Smart Mode (Recommended)**

```python
# Step 1: Define Instrument
nifty_put = Option("NIFTY", date(2023, 12, 28), 21000, OptionType.PE)

# Step 2: Trade
client.place_order(nifty_put, TransactionType.BUY, qty=50)
```

**Legacy Mode**

```python
client.place_order("NSE:RELIANCE", TransactionType.BUY, qty=10)
```

---

## 2. Get Data API

### A. Real-Time/Snapshot Quotes (`get_quote`)

```python
# Create an Index object for safe quoting
nifty_idx = Index("NIFTY 50", exchange="NSE")

quote = client.get_quote(nifty_idx)
print(quote.ltp)
```

### B. Historical Candles (`get_historical_data`)

```python
history = client.get_historical_data(
    instrument=nifty_idx,
    interval=TimeFrame.MIN_5,
    from_date=datetime.now() - timedelta(days=5),
    to_date=datetime.now()
)
```

## 3. Response Standardization

All methods return **Pydantic Models** (Classes), not raw dictionaries.

```python
class OrderResponse(BaseModel):
    order_id: str
    status: str
    message: Optional[str]

class Candle(BaseModel):
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
```
