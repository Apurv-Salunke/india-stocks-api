# Database Initialization: The "Daily Build"

To ensure fast and accurate instrument resolution, the package includes an initialization step that transforms the raw Broker Master CSV/JSON into a structured, optimized internal database (SQLite).

## The "Build" Process (`init_instruments.py`)

When the user calls `client.init()` or runs the setup script, the system performs the following optimizations:

### 1. Raw Data Ingestion

- Downloads the Master Contract file from the broker (or OpenAlgo source).
- Parses the raw CSV/JSON.

### 2. Date Standardization

- Converts all expiry strings (e.g., `28-DEC-2023`, `28DEC23`) into standard ISO `YYYY-MM-DD` dates.
- This solves the "String Format" issues during runtime lookups.

### 3. Classification & Tagging (The "Smart" Layer)

We add computed columns to the DB to make querying trivial.

| Computed Column    | Logic                                    | Purpose                       |
| :----------------- | :--------------------------------------- | :---------------------------- |
| `is_monthly`       | True if expiry is last Thursday of month | Fast filtering                |
| `is_weekly`        | True if expiry is any other Thursday     | Fast filtering                |
| `days_to_expiry`   | `expiry_date - today`                    | "Give me next week's options" |
| `instrument_class` | EQ / FUT / OPT / IDX                     | Polymorphic loading           |

### 4. Indexing

- Creates indices on `(symbol, expiry, strike, opt_type)` for O(1) resolution speed.

## Runtime Benefits

Because we "Do the classifications for the day upfront," the runtime query becomes simple and robust:

```sql
-- "Find NIFTY Weekly ATM Call"
SELECT * FROM instruments
WHERE symbol='NIFTY'
  AND is_weekly=1
  AND days_to_expiry BETWEEN 0 AND 7
  AND strike_diff_from_spot < 50
```

## Schema Design

```python
class InstrumentMaster(Base):
    __tablename__ = 'instruments'

    token = Column(String, primary_key=True)
    symbol = Column(String, index=True) # "NIFTY"

    # Financial Details
    expiry_date = Column(Date, index=True)
    strike_price = Column(Float, index=True)
    option_type = Column(String) # CE/PE

    # Pre-Computed Metadata
    is_fno = Column(Boolean)
    is_monthly_expiry = Column(Boolean)

    # Broker Details
    tradingsymbol = Column(String) # For the actual API order
```

This "Build" step takes ~5-10 seconds once a day but saves milliseconds on every single order and prevents almost all "Resolution Errors".
