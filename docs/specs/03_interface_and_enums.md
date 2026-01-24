# Interface Design: Enums & Type Safety

To ensure a robust user experience, strict strict definitions will be used for all inputs. Strings (which are prone to typos like "mkt" vs "MARKET") will be replaced with Enums.

## Standard Enums

Defined in `openalgo_brokers.constants`:

```python
class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"
    SLM = "SL-M"

class TransactionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class ProductType(str, Enum):
    INTRADAY = "MIS"
    DELIVERY = "CNC"
    CARRYFORWARD = "NRML"

class OrderValidity(str, Enum):
    DAY = "DAY"
    IOC = "IOC"
```

## Internal Translation

The public facing adapters (`adapters/zerodha.py`) are responsible for translating these Enums into the specific format required by the internal Shimmed code.

**Example Flow:**

1. **User**: Calls `client.place_order(order_type=OrderType.MARKET)`
2. **Adapter (Zerodha)**: Receives Enum. Maps `OrderType.MARKET` -> `"MARKET"` (String).
3. **Adapter (Angel)**: Receives Enum. Maps `OrderType.MARKET` -> `"MKT"` (String).
4. **Internal Code**: Receives the specific string it expects for that broker's JSON payload.

## Benefits

1. **Autocomplete**: IDEs immediately show available options.
2. **Validation**: Invalid inputs are caught before the network request is made.
3. **Abstraction**: Users learn one set of terms (OpenAlgo Standard) rather than 20 different broker dialects.
