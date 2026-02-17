import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from india_stocks_api.instruments.database import InstrumentDB


def test_db_operations():
    print("Testing DB...")
    db = InstrumentDB("instruments.db")

    # Test 1: Insert Real Samples
    records = [
        {
            "token": "2885",
            "symbol": "RELIANCE",
            "exchange": "NSE",
            "tradingsymbol": "RELIANCE-EQ",
            "br_symbol": "RELIANCE-EQ",
            "expiry": None,
            "strike": None,
            "opt_type": None,
            "lot_size": 1,
            "tick_size": 0.05,
            "instrument_type": "EQ",
        },
        {
            "token": "9992885",
            "symbol": "NIFTY",
            "exchange": "NFO",
            "tradingsymbol": "NIFTY24DEC23000CE",
            "br_symbol": "NIFTY24DEC23000CE",
            "expiry": "2024-12-26",
            "strike": 23000.0,
            "opt_type": "CE",
            "lot_size": 25,
            "tick_size": 0.05,
            "instrument_type": "OPT",
        },
    ]

    print("Inserting samples into instruments.db...")
    try:
        db.raw_bulk_insert(records, truncate=True)
        print("Insert Success!")
    except Exception as e:
        print(f"Insert Failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_db_operations()
