# Symbol Mapping Guide

This guide explains how to use the OpenAlgo-style symbol mapping functions to convert between standardized symbols and broker-specific symbols.

## Overview

The India Stocks API provides a unified symbol mapping system that allows you to:
- Use consistent symbols across all brokers (e.g., "RELIANCE", "BANKNIFTY")
- Get broker-specific tokens and symbols for API calls
- Convert between different broker formats seamlessly

## Available Functions

### Core Mapping Functions

#### `get_broker_token(standardized_symbol, exchange_code, broker_name)`
Get the broker-specific token for a standardized symbol.

```python
from india_stocks_api.database.broker_instruments_db import get_broker_token

# Get AngelOne token for RELIANCE
token = get_broker_token("RELIANCE", "NSE", "angelone")  # Returns: "2885"

# Get Fyers token for RELIANCE
token = get_broker_token("RELIANCE", "NSE", "fyers")     # Returns: "10100000002885"
```

#### `get_broker_symbol(standardized_symbol, exchange_code, broker_name)`
Get the broker-specific symbol for a standardized symbol.

```python
from india_stocks_api.database.broker_instruments_db import get_broker_symbol

# Get AngelOne symbol for BANKNIFTY
symbol = get_broker_symbol("BANKNIFTY", "NSE_INDEX", "angelone")  # Returns: "Nifty Bank"

# Get Fyers symbol for BANKNIFTY
symbol = get_broker_symbol("BANKNIFTY", "NSE_INDEX", "fyers")     # Returns: "NSE:NIFTYBANK-INDEX"
```

#### `get_standardized_symbol(broker_symbol, exchange_code, broker_name)`
Convert a broker-specific symbol back to the standardized format.

```python
from india_stocks_api.database.broker_instruments_db import get_standardized_symbol

# Convert Fyers symbol to standardized
std_symbol = get_standardized_symbol("NSE:RELIANCE-EQ", "NSE", "fyers")  # Returns: "RELIANCE"

# Convert AngelOne symbol to standardized
std_symbol = get_standardized_symbol("RELIANCE-EQ", "NSE", "angelone")   # Returns: "RELIANCE"
```

#### `get_broker_exchange_code(standardized_symbol, exchange_code, broker_name)`
Get the broker-specific exchange code.

```python
from india_stocks_api.database.broker_instruments_db import get_broker_exchange_code

# Get broker exchange code for RELIANCE
broker_exchange = get_broker_exchange_code("RELIANCE", "NSE", "dhan")  # Returns: "NSE_EQ"
```

### Advanced Functions

#### `get_symbol_info(standardized_symbol, exchange_code, broker_name)`
Get complete symbol information including all metadata.

```python
from india_stocks_api.database.broker_instruments_db import get_symbol_info

info = get_symbol_info("RELIANCE", "NSE", "angelone")
# Returns:
# {
#     "standardized_symbol": "RELIANCE",
#     "broker_symbol": "RELIANCE-EQ",
#     "broker_token": "2885",
#     "instrument_name": "RELIANCE",
#     "exchange_code": "NSE",
#     "broker_exchange_code": "NSE",
#     "lot_size": 1,
#     "tick_size": 0.1,
#     "instrument_type": "",
#     ...
# }
```

#### `get_all_broker_symbols(standardized_symbol, exchange_code)`
Get broker-specific symbols for a standardized symbol across all brokers.

```python
from india_stocks_api.database.broker_instruments_db import get_all_broker_symbols

all_mappings = get_all_broker_symbols("RELIANCE", "NSE")
# Returns:
# {
#     "angelone": {"broker_symbol": "RELIANCE-EQ", "broker_token": "2885", ...},
#     "fyers": {"broker_symbol": "NSE:RELIANCE-EQ", "broker_token": "10100000002885", ...},
#     "dhan": {"broker_symbol": "RELIANCE", "broker_token": "2885", ...},
#     ...
# }
```

#### `get_symbol_count()`
Get the total number of symbols in the database.

```python
from india_stocks_api.database.broker_instruments_db import get_symbol_count

total_symbols = get_symbol_count()  # Returns: 1323402
```

## Common Use Cases

### 1. Multi-Broker Trading Application

```python
def place_order(symbol, exchange, quantity, broker_name):
    # Get broker-specific token and symbol
    token = get_broker_token(symbol, exchange, broker_name)
    broker_symbol = get_broker_symbol(symbol, exchange, broker_name)

    if not token or not broker_symbol:
        raise ValueError(f"Symbol {symbol} not found for broker {broker_name}")

    # Use broker-specific values for API call
    return broker_api.place_order(
        symbol=broker_symbol,
        token=token,
        quantity=quantity
    )

# Usage - same symbol works across all brokers
place_order("RELIANCE", "NSE", 10, "angelone")  # Uses "RELIANCE-EQ" and token "2885"
place_order("RELIANCE", "NSE", 10, "fyers")     # Uses "NSE:RELIANCE-EQ" and token "10100000002885"
```

### 2. Symbol Lookup and Validation

```python
def validate_symbol(symbol, exchange, broker_name):
    info = get_symbol_info(symbol, exchange, broker_name)
    if info:
        return {
            "valid": True,
            "broker_symbol": info["broker_symbol"],
            "broker_token": info["broker_token"],
            "lot_size": info["lot_size"],
            "tick_size": info["tick_size"]
        }
    return {"valid": False}

# Usage
result = validate_symbol("BANKNIFTY", "NSE_INDEX", "angelone")
if result["valid"]:
    print(f"Valid symbol: {result['broker_symbol']} (token: {result['broker_token']})")
```

### 3. Broker Comparison

```python
def compare_broker_symbols(symbol, exchange):
    all_mappings = get_all_broker_symbols(symbol, exchange)

    print(f"Symbol: {symbol} ({exchange})")
    print("-" * 50)

    for broker, data in all_mappings.items():
        print(f"{broker:15}: {data['broker_symbol']:25} (token: {data['broker_token']})")

# Usage
compare_broker_symbols("RELIANCE", "NSE")
compare_broker_symbols("BANKNIFTY", "NSE_INDEX")
```

## Supported Brokers

| Broker | Code | Status |
|--------|------|--------|
| AngelOne | `angelone` | ✅ Ready |
| Dhan | `dhan` | ✅ Ready |
| Dhan Sandbox | `dhan_sandbox` | ✅ Ready |
| 5Paisa | `fivepaisa` | ✅ Ready |
| 5PaisaXTS | `fivepaisaxts` | ✅ Ready |
| Fyers | `fyers` | ✅ Ready |
| Groww | `groww` | ✅ Ready |
| Shoonya | `shoonya` | ✅ Ready |
| Upstox | `upstox` | ✅ Ready |

## Supported Exchanges

| Exchange | Code | Description |
|----------|------|-------------|
| NSE | `NSE` | National Stock Exchange (Equity) |
| BSE | `BSE` | Bombay Stock Exchange (Equity) |
| NFO | `NFO` | NSE Futures & Options |
| BFO | `BFO` | BSE Futures & Options |
| CDS | `CDS` | NSE Currency Derivatives |
| MCX | `MCX` | Multi Commodity Exchange |
| NSE_INDEX | `NSE_INDEX` | NSE Indices |
| BSE_INDEX | `BSE_INDEX` | BSE Indices |

## Examples

Run the comprehensive examples:

```bash
cd /Users/apurv/Desktop/algo-trading/india-stocks-api
python examples/symbol_mapping_example.py
```

## Error Handling

All functions return `None` if the symbol is not found:

```python
token = get_broker_token("INVALID_SYMBOL", "NSE", "angelone")
if token is None:
    print("Symbol not found")
```

## Performance

- All functions use direct database queries with proper indexing
- Typical response time: < 1ms per query
- Database contains 1,323,402+ instruments across 9 brokers
- Supports concurrent access from multiple threads

## Database Schema

The mapping functions work with the `broker_instruments` table:

```sql
CREATE TABLE broker_instruments (
    id INTEGER PRIMARY KEY,
    standardized_symbol VARCHAR NOT NULL,  -- e.g., "RELIANCE"
    broker_symbol VARCHAR NOT NULL,        -- e.g., "RELIANCE-EQ"
    broker_token VARCHAR NOT NULL,         -- e.g., "2885"
    exchange_code VARCHAR NOT NULL,        -- e.g., "NSE"
    broker_exchange_code VARCHAR,          -- e.g., "NSE_EQ"
    instrument_name VARCHAR,               -- e.g., "Reliance Industries Ltd"
    lot_size INTEGER,                      -- e.g., 1
    tick_size FLOAT,                       -- e.g., 0.1
    instrument_type VARCHAR,               -- e.g., "EQ"
    broker_name VARCHAR NOT NULL,          -- e.g., "angelone"
    is_active INTEGER DEFAULT 1,
    created_at DATETIME,
    updated_at DATETIME
);
```

## OpenAlgo Compatibility

These functions are designed to be drop-in replacements for OpenAlgo's symbol mapping functions:

- `get_broker_token()` → OpenAlgo's `get_token()`
- `get_broker_symbol()` → OpenAlgo's `get_br_symbol()`
- `get_standardized_symbol()` → OpenAlgo's `get_oa_symbol()`
- `get_broker_exchange_code()` → OpenAlgo's `get_brexchange()`

This ensures compatibility with existing OpenAlgo-based applications.
