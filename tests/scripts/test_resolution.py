import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from india_stocks_api.constants import OptionType
from india_stocks_api.instruments import Equity, Option
from india_stocks_api.instruments.database import InstrumentDB
from india_stocks_api.instruments.resolver import Resolver


def test_resolution():
    print("--- Testing Instrument Resolution ---")

    # Init DB (assumes instruments.db is in src/)
    db_path = Path(__file__).parent.parent / "instruments.db"
    if not db_path.exists():
        print(f"Error: DB not found at {db_path}")
        return

    db = InstrumentDB(str(db_path))
    resolver = Resolver(db)

    # Debug: Print first 5 records
    session = db.get_session()
    # Debug: Count
    session = db.get_session()

    from india_stocks_api.instruments.database import InstrumentMaster

    count = session.query(InstrumentMaster).count()
    print(f"DEBUG: Total Records in DB: {count}")

    print("DEBUG: Searching for '%REL%' (Symbol)...")
    rels = session.query(InstrumentMaster).filter(InstrumentMaster.symbol.like("%REL%")).limit(10).all()
    for s in rels:
        print(f"  Token={s.token} Symbol='{s.symbol}' Exch='{s.exchange}' Type={s.instrument_type}")

    print("DEBUG: Searching for 'NIFTY' (Exact symbol)...")
    niftys = session.query(InstrumentMaster).filter(InstrumentMaster.symbol == "NIFTY").limit(5).all()
    for s in niftys:
        print(
            f"  Token={s.token} Symbol='{s.symbol}' Exch='{s.exchange}' Type={s.instrument_type} TradeSym='{s.tradingsymbol}'"
        )

    print("DEBUG: Searching for ANY Option...")
    opts = session.query(InstrumentMaster).filter(InstrumentMaster.instrument_type == "OPT").limit(5).all()
    for s in opts:
        print(
            f"  Token={s.token} Symbol='{s.symbol}' Exch='{s.exchange}' Type={s.instrument_type} OptType={s.opt_type} Strike={s.strike} Expiry={s.expiry}"
        )

    session.close()

    # Test 1: Equity
    print("\n1. Resolving RELIANCE Equity...")
    try:
        reli = Equity("RELIANCE")
        res = resolver.resolve(reli)
        print(f"   ✅ Found: {res['symbol']} -> Token: {res['token']}")
    except Exception as e:
        print(f"   ❌ Failed: {e}")

    # Test 2: NIFTY Option (We need a valid date)
    # Since I don't know exact dates in DB, I'll search for one first
    print("\n2. Searching for NIFTY Options...")
    session = db.get_session()
    # Find a valid NIFTY option
    from india_stocks_api.instruments.database import InstrumentMaster

    sample = (
        session.query(InstrumentMaster)
        .filter(
            InstrumentMaster.symbol == "NIFTY",
            InstrumentMaster.instrument_type == "OPT",
            InstrumentMaster.opt_type == "CE",
        )
        .first()
    )
    session.close()

    if sample:
        print(f"   Using Sample: {sample.tradingsymbol} Expiry: {sample.expiry} Strike: {sample.strike}")

        # Now try to resolve using Domain Object
        opt = Option(
            symbol="NIFTY",
            expiry=sample.expiry,  # Ensure this is a date object
            strike=sample.strike,
            opt_type=OptionType.CE,
        )

        try:
            res = resolver.resolve(opt)
            print(f"   ✅ Resolved Option: {res['tradingsymbol']} -> Token: {res['token']}")
        except Exception as e:
            print(f"   ❌ Resolution Failed: {e}")
    else:
        print("   ⚠️ No NIFTY options found in DB to test.")


if __name__ == "__main__":
    test_resolution()
