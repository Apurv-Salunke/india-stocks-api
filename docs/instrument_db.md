# Instrument Master Database

The `instruments` table is the unified, broker-agnostic registry of all tradable contracts.

All broker raw instrument data **must be normalized into this format before insertion**.

Each row represents exactly one tradable contract.

---

# Data Contract (Python Representation)

```python
{
    "token": str,
    "symbol": str,
    "exchange": str,
    "tradingsymbol": str,
    "br_symbol": str,
    "expiry": datetime.date | None,
    "strike": float | None,
    "opt_type": "CE" | "PE" | None,
    "lot_size": int,
    "tick_size": float,
    "instrument_type": "EQ" | "FUT" | "OPT" | "IDX"
}
```

---

# Schema

```sql
CREATE TABLE instruments (
    token TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    tradingsymbol TEXT NOT NULL,
    br_symbol TEXT NOT NULL,
    expiry DATE,
    strike REAL,
    opt_type TEXT,
    lot_size INTEGER NOT NULL,
    tick_size REAL NOT NULL,
    instrument_type TEXT NOT NULL
);
```

---

# Column Rules

| Column          | Type         | Allowed Values                           | Rules                                                |
| --------------- | ------------ | ---------------------------------------- | ---------------------------------------------------- |
| token           | str          | Any                                      | Store exactly as broker provides. Primary key.       |
| symbol          | str          | Uppercase, no spaces                     | Base underlying only. No expiry, strike, CE/PE, FUT. |
| exchange        | str          | NSE, BSE, NFO, CDS, MCX, NCDEX, BFO, NCO | Must match broker exchange. No custom values.        |
| tradingsymbol   | str          | Any                                      | Store exactly as broker provides.                    |
| br_symbol       | str          | Any                                      | Raw broker value.                                    |
| expiry          | date / None  | ISO date                                 | Convert to `datetime.date`. Empty → None.            |
| strike          | float / None | Numeric                                  | Normalize scaling. None for non-options.             |
| opt_type        | str / None   | CE, PE, None                             | Only when instrument_type = OPT.                     |
| lot_size        | int          | > 0                                      | Cast to int.                                         |
| tick_size       | float        | > 0                                      | Cast to float.                                       |
| instrument_type | str          | EQ, FUT, OPT, IDX                        | Normalize broker types to these four only.           |

---

# Strict Constraints

## instrument_type

Allowed:

``` css
EQ
FUT
OPT
IDX
```

No broker-specific types permitted.

---

## opt_type

Allowed:

``` css
CE
PE
None
```

Only valid when `instrument_type = OPT`.

---

## exchange

Allowed:

``` css
NSE
BSE
NFO
CDS
MCX
NCDEX
BFO
NCO
```

Exchange represents venue only.
Never combine with instrument_type.

---

# Do / Don’t Examples

## symbol

| Don’t            | Do        |
| ---------------- | --------- |
| NIFTY24FEBFUT    | NIFTY     |
| BANKNIFTY45000CE | BANKNIFTY |

---

## instrument_type

| Don’t  | Do  |
| ------ | --- |
| FUTSTK | FUT |
| OPTIDX | OPT |

---

## exchange

| Don’t     | Do  |
| --------- | --- |
| NSE_INDEX | NSE |
| NFO_OPT   | NFO |

---

## expiry

| Don’t       | Do         |
| ----------- | ---------- |
| "19MAR2024" | 2024-03-19 |
| ""          | None       |

---

# Integration Checklist

Before inserting broker data:

* expiry is `datetime.date` or None
* symbol contains only underlying
* instrument_type ∈ {EQ, FUT, OPT, IDX}
* opt_type ∈ {CE, PE, None}
* exchange ∈ allowed list
* tradingsymbol unchanged
* token unchanged
* strike normalized

---

# Validation Guard

```python
VALID_EXCHANGES = {'NSE','BSE','NFO','CDS','MCX','NCDEX','BFO','NCO'}
VALID_TYPES = {'EQ','FUT','OPT','IDX'}
VALID_OPT = {'CE','PE',None}

assert df['exchange'].isin(VALID_EXCHANGES).all()
assert df['instrument_type'].isin(VALID_TYPES).all()
assert df['opt_type'].isin(VALID_OPT).all()
```
