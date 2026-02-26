# Instrument System

This document explains the instrument database lifecycle and symbol resolution logic.

---

## Overview

The instrument system:

1. Downloads master contract data from brokers
2. Normalizes into a common SQLite schema
3. Resolves domain objects to broker-specific tokens
4. Auto-provisions on broker instantiation

---

## Database Location

```
_cache/
└── instruments.db    # SQLite database
```

The database is stored in `_cache/` relative to the package root.

---

## Database Schema

### Core Table: instruments

```sql
CREATE TABLE instruments (
    token TEXT PRIMARY KEY,      -- Broker's instrument token
    symbol TEXT NOT NULL,        -- NSE-style symbol (RELIANCE, NIFTY)
    tradingsymbol TEXT,          -- Broker's trading symbol
    exchange TEXT NOT NULL,      -- Exchange code (NSE, NFO, BSE)
    
    -- F&O specific
    expiry DATE,                 -- Expiry date for derivatives
    strike REAL,                 -- Strike price for options
    option_type TEXT,            -- CE or PE
    instrument_type TEXT,        -- EQ, FUT, OPT, IDX
    lot_size INTEGER,            -- Contract lot size
    
    -- Metadata
    tick_size REAL,              -- Minimum price movement
    segment TEXT,                -- Market segment
    name TEXT                    -- Full instrument name
);

-- Indices for fast lookups
CREATE INDEX idx_symbol_exchange ON instruments(symbol, exchange);
CREATE INDEX idx_expiry ON instruments(expiry);
CREATE INDEX idx_strike ON instruments(strike);
```

---

## Auto-Provisioning

### Trigger Conditions

Database is downloaded when:

1. `instruments.db` doesn't exist
2. File modification date is before today

### Implementation

```python
# brokers/base.py

class BrokerMeta(ABCMeta):
    def __call__(cls, *args, **kwargs):
        instance = super().__call__(*args, **kwargs)
        instance._ensure_instruments_ready()
        return instance

class BaseBroker(ABC, metaclass=BrokerMeta):
    def _ensure_instruments_ready(self):
        db_path = context.get_instruments_db_path()
        if self._is_db_stale(db_path):
            self._download_master_contract()
    
    @staticmethod
    def _is_db_stale(db_path) -> bool:
        if not db_path.exists():
            return True
        mtime = db_path.stat().st_mtime
        file_date = datetime.fromtimestamp(mtime).date()
        return file_date < date.today()
```

---

## Download Process

### Angel One Example

```python
# internal/angel/database/master_contract.py

def master_contract_download():
    """Download Angel One master contract."""
    
    # 1. Download raw JSON
    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    response = httpx.get(url, timeout=60)
    data = response.json()
    
    # 2. Transform to DataFrame
    df = pd.DataFrame(data)
    
    # 3. Normalize column names
    df = df.rename(columns={
        "token": "token",
        "symbol": "tradingsymbol",
        "name": "name",
        "expiry": "expiry",
        "strike": "strike",
        "lotsize": "lot_size",
        "instrumenttype": "instrument_type",
        "exch_seg": "exchange",
        "tick_size": "tick_size",
    })
    
    # 4. Extract standard symbol
    df["symbol"] = df.apply(extract_base_symbol, axis=1)
    
    # 5. Write to SQLite
    db_path = context.get_instruments_db_path()
    engine = create_engine(f"sqlite:///{db_path}")
    df.to_sql("instruments", engine, if_exists="replace", index=False)
```

### Symbol Normalization

Raw broker symbols are transformed to NSE-style:

| Broker Symbol | Normalized Symbol |
|---------------|-------------------|
| `RELIANCE-EQ` | `RELIANCE` |
| `NIFTY24DEC22000CE` | `NIFTY` (with metadata) |
| `BANKNIFTY-FUT` | `BANKNIFTY` |

---

## Instrument Resolution

### Resolution Flow

```
User: Equity("RELIANCE")
         │
         ▼
┌────────────────────────────┐
│  _resolve_instrument()     │
│  • Build query parameters  │
│  • Query SQLite            │
│  • Return token info       │
└────────────────────────────┘
         │
         ▼
Result: {
    "symbol": "RELIANCE-EQ",
    "token": "2885",
    "tradingsymbol": "RELIANCE-EQ",
    "exchange": "NSE"
}
```

### Query Building

Different instrument types require different queries:

```python
def _resolve_instrument(self, instrument):
    """Resolve domain object to broker token."""
    
    if isinstance(instrument, Equity):
        # Simple symbol + exchange lookup
        query = """
            SELECT * FROM instruments 
            WHERE symbol = ? AND exchange = ? 
            AND instrument_type = 'EQ'
        """
        params = (instrument.symbol, instrument.exchange)
    
    elif isinstance(instrument, Future):
        # Include expiry
        query = """
            SELECT * FROM instruments 
            WHERE symbol = ? AND exchange = ? 
            AND expiry = ? AND instrument_type = 'FUT'
        """
        params = (instrument.symbol, instrument.exchange, instrument.expiry)
    
    elif isinstance(instrument, Option):
        # Full contract specification
        query = """
            SELECT * FROM instruments 
            WHERE symbol = ? AND exchange = ? 
            AND expiry = ? AND strike = ? 
            AND option_type = ?
        """
        params = (
            instrument.symbol,
            instrument.exchange,
            instrument.expiry,
            instrument.strike,
            instrument.opt_type.value
        )
    
    # Execute and return
    ...
```

---

## Symbol Mapping

### The Translation Problem

Users work with NSE-style symbols, but brokers use different formats:

| User Input | Angel One | Zerodha |
|------------|-----------|---------|
| `RELIANCE` | `RELIANCE-EQ` | `RELIANCE` |
| `NIFTY FUT` | `NIFTY-FUT` | `NIFTY24DECFUT` |

### Solution: Two Columns

The database stores both:

- `symbol`: Standardized NSE-style symbol (user-facing)
- `tradingsymbol`: Broker's actual trading symbol (API-facing)

```sql
SELECT tradingsymbol, token 
FROM instruments 
WHERE symbol = 'RELIANCE' AND exchange = 'NSE';

-- Returns: RELIANCE-EQ, 2885
```

---

## Index Handling

### Non-Tradable Indices

Indices like NIFTY 50 cannot be traded directly. The SDK:

1. Allows `get_quote(Index("NIFTY 50"))`
2. Blocks `place_order(Index(...))` with `ValidationError`

### Index Symbol Normalization

```python
INDEX_MAP = {
    "NIFTY 50": "NIFTY",
    "NIFTY BANK": "BANKNIFTY",
    "NIFTY FIN SERVICE": "FINNIFTY",
    "NIFTY NEXT 50": "NIFTYNXT50",
    "INDIA VIX": "INDIAVIX",
}
```

---

## F&O Contract Resolution

### Futures

Resolution requires matching symbol + expiry:

```python
# Find current month NIFTY future
Future("NIFTY", expiry=date(2024, 12, 26))

# Query
SELECT * FROM instruments
WHERE symbol = 'NIFTY'
  AND exchange = 'NFO'
  AND expiry = '2024-12-26'
  AND instrument_type = 'FUT'
```

### Options

Resolution requires symbol + expiry + strike + type:

```python
# Find specific option
Option("BANKNIFTY", date(2024, 12, 26), 52000.0, OptionType.CE)

# Query
SELECT * FROM instruments
WHERE symbol = 'BANKNIFTY'
  AND exchange = 'NFO'
  AND expiry = '2024-12-26'
  AND strike = 52000.0
  AND option_type = 'CE'
```

---

## Error Cases

### Symbol Not Found

```python
try:
    broker.get_quote(Equity("INVALID"))
except ValidationError as e:
    print(e)  # "instrument token not found for INVALID on NSE"
```

### Ambiguous Contract

If multiple contracts match (shouldn't happen with proper query):

```python
# Validation should catch this
if len(results) > 1:
    raise ValidationError(
        f"Ambiguous contract: {len(results)} matches found",
        code=ErrorCode.INVALID_INSTRUMENT
    )
```

### Expired Contract

Attempting to resolve an expired F&O contract returns no results.

---

## Database Utilities

### Manual Refresh

```python
# Force database refresh
broker._download_master_contract()
```

### Check Staleness

```python
from india_stocks_api.internal.context import get_instruments_db_path
from india_stocks_api.brokers.base import BaseBroker

db_path = get_instruments_db_path()
is_stale = BaseBroker._is_db_stale(db_path)
print(f"Database is stale: {is_stale}")
```

### Direct Query (Debug Only)

```python
import sqlite3
from india_stocks_api.internal.context import get_instruments_db_path

db_path = get_instruments_db_path()
conn = sqlite3.connect(db_path)

# List all NIFTY options
cursor = conn.execute("""
    SELECT tradingsymbol, strike, option_type, expiry
    FROM instruments
    WHERE symbol = 'NIFTY' AND instrument_type = 'OPT'
    ORDER BY expiry, strike
    LIMIT 20
""")

for row in cursor:
    print(row)
```

---

## Performance

### Query Optimization

- Indices on frequently queried columns
- Single lookup per instrument (cached result could be added)
- ~50-100k instruments load in <5 seconds

### Memory Considerations

- Full database: ~50-100 MB
- Loaded on-demand (not cached in memory)
- SQLite handles caching efficiently

---

## Extending for New Brokers

To add instrument support for a new broker:

1. Implement `master_contract_download()` in `internal/<broker>/database/`
2. Normalize to the common schema
3. Update `_resolve_instrument()` if broker has special symbol formats

---

## Next Steps

- [Session & Auth](session-auth.md) - Token management
- [Broker Adapter Guide](broker-adapter-guide.md) - Adding brokers
