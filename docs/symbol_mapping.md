# OpenAlgo Symbol Mapping: Implementation Reference

> This document explains how OpenAlgo maintains the NSE Symbol ↔ Broker Symbol mapping.

---

## 1. The Database Schema (`SymToken` table)

Both symbols are stored in the same table with these key columns:

| Column       | Purpose                          | Example (Zerodha)   |
| ------------ | -------------------------------- | ------------------- |
| `symbol`     | **OpenAlgo/NSE Standard Symbol** | `NIFTY26DEC24FUT`   |
| `brsymbol`   | **Broker-Specific Symbol**       | `NIFTY24DECFUT`     |
| `exchange`   | OpenAlgo exchange                | `NFO`               |
| `brexchange` | Broker exchange (if different)   | `NFO`               |
| `token`      | Broker's instrument token        | `12345678::::67890` |

---

## 2. How the Mapping is Created

Each broker has a `master_contract_db.py` file (located at `broker/<broker_name>/database/`) that:

1. **Downloads** raw instrument data from the broker's API (CSV/JSON)
2. **Transforms** it:
   - Stores the raw broker symbol as `brsymbol`
   - Generates a **standardized** OpenAlgo symbol and stores it as `symbol`

### Example from Zerodha (`broker/zerodha/database/master_contract_db.py`)

```python
# Line 206-207
df['brsymbol'] = df['symbol']                     # Store raw Zerodha symbol
df['symbol'] = df.apply(reformat_symbol, axis=1)  # Create standardized symbol
```

### The `reformat_symbol` Function

Converts broker format to OpenAlgo standard:

| Broker Symbol (Raw)       | OpenAlgo Symbol (Standardized)      |
| ------------------------- | ----------------------------------- |
| `NIFTY 24 DEC FUT`        | `NIFTY24DECFUT`                     |
| `NIFTY 24 12 26 21000 CE` | `NIFTY26DEC2421000CE`               |
| `RELIANCE`                | `RELIANCE` (no change for equities) |

### Index Name Standardization (Line 226-234)

```python
df['symbol'] = df['symbol'].replace({
    'NIFTY 50': 'NIFTY',
    'NIFTY NEXT 50': 'NIFTYNXT50',
    'NIFTY FIN SERVICE': 'FINNIFTY',
    'NIFTY BANK': 'BANKNIFTY',
    'NIFTY MID SELECT': 'MIDCPNIFTY',
    'INDIA VIX': 'INDIAVIX',
    'SNSX50': 'SENSEX50'
})
```

---

## 3. How the Mapping is Used (Order Flow)

When placing an order:

1. **User sends**: `symbol="NIFTY26DEC24FUT"` (OpenAlgo format)
2. **`transform_data()`** calls `get_br_symbol("NIFTY26DEC24FUT", "NFO")`
3. **`get_br_symbol`** queries:

   ```sql
   SELECT brsymbol FROM symtoken
   WHERE symbol='NIFTY26DEC24FUT' AND exchange='NFO'
   ```

4. **Returns broker symbol**: `NIFTY24DECFUT`
5. **This `brsymbol`** is sent to the broker API

### Code Reference (`broker/zerodha/mapping/transform_data.py`)

```python
from database.token_db import get_br_symbol

def transform_data(data):
    symbol = get_br_symbol(data['symbol'], data['exchange'])  # <-- Translation happens here

    transformed = {
        "tradingsymbol": symbol,  # Broker symbol sent to API
        "exchange": data['exchange'],
        # ... other fields
    }
    return transformed
```

---

## 4. Key Functions

| Function                            | Location                                  | Purpose                                   |
| ----------------------------------- | ----------------------------------------- | ----------------------------------------- |
| `get_br_symbol(symbol, exchange)`   | `database/token_db.py`                    | Lookup broker symbol from OpenAlgo symbol |
| `get_oa_symbol(brsymbol, exchange)` | `database/token_db.py`                    | Reverse lookup (broker → OpenAlgo)        |
| `get_token(symbol, exchange)`       | `database/token_db.py`                    | Get broker token ID                       |
| `reformat_symbol()`                 | `broker/*/database/master_contract_db.py` | Standardize symbols during download       |

---

## 5. Files Per Broker

Each broker has its own master contract handling:

```text
broker/
├── zerodha/database/master_contract_db.py
├── angel/database/master_contract_db.py
├── fyers/database/master_contract_db.py
├── dhan/database/master_contract_db.py
└── ... (29 brokers total)
```

Each file implements:

- `master_contract_download()` - Downloads and processes broker data
- `reformat_symbol()` - Broker-specific symbol standardization
- `process_*_csv()` - Parses broker's raw data format

---

## 6. Summary

OpenAlgo maintains a **one-to-one mapping** in the `SymToken` table:

- **`symbol`** column: Standardized OpenAlgo format (NSE-style)
- **`brsymbol`** column: Raw broker format

The translation happens at order time via `get_br_symbol()`, ensuring users always work with consistent symbols regardless of which broker they use.
