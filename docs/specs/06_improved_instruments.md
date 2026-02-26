# Symbol Search & Instrument Classification: The "Smart" Layer

OpenAlgo already has a robust SQLite-based master contract database (`SymToken` table). This new package should expose that richness through a high-level, structured API rather than raw SQL/ORM queries.

## The Goal

To allow users to find instruments using financial criteria (Expiry, Strike, Option Type) rather than memorizing symbol codes like `NIFTY23DEC21000CE`.

## Structured Instrument API

The API leverages the domain objects defined in `08_domain_objects.md`: `Equity`, `Index`, `Future`, `Option`.

## The Search Interface

The `Broker` client exposes a rich search capability backed by the SQLite file.

```python
# User Usage Example

# 1. Find the ATM Call Option for BankNifty
# Returns an Option() object
instrument = client.instruments.find_option(
    symbol="BANKNIFTY",
    expiry="current_week",  # Smart alias resolved logic
    strike_type="ATM",      # Smart alias (requires live price or mapping)
    opt_type=OptionType.CE
)

# 2. Get all Futures for NIFTY
# Returns List[Future]
futures = client.instruments.get_futures(
    symbol="NIFTY"
)

# 3. Fuzzy Search (e.g., "Relia")
# Returns List[Union[Equity, Future, Option]]
results = client.instruments.search("Relia")
```

## Implementation Strategy

1. **Reuse Existing Logic**: OpenAlgo's `Token DB` (`database/token_db_enhanced.py`) already has excellent logic for `search_symbols` and `fno_search_symbols`.
2. **Port to Internal**: We will port the **read-only** parts of `token_db_enhanced.py` and `symbol.py` into `openalgo_brokers.internal.instruments`.
3. **Factory Pattern**: The search results are automatically instantiated into the correct Domain Class (`Equity` vs `Future` vs `Option`) based on the `instrumenttype` column in the DB.

## Classification Tiers

1. **Equities (EQ)**: Maps to `Equity` class.
2. **Indices (IDX)**: Maps to `Index` class.
3. **Futures (FUT)**: Maps to `Future` class.
4. **Options (OPT)**: Maps to `Option` class.

This structure allows us to build powerful higher-order functions later, like "Buy the straddle" (Find ATM CE + ATM PE and buy both).
