import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from india_stocks_api.instruments.database import InstrumentDB, InstrumentMaster

def test_db_operations():
    print("Testing DB...")
    db = InstrumentDB("test.db")
    
    # Test 1: Insert
    records = [
        {
            "token": "123",
            "symbol": "EST",
            "exchange": "NSE",
            "tradingsymbol": "TEST-EQ",
            "br_symbol": "TEST-EQ",
            "lotsize": 1,
            "instrument_type": "EQ"
        }
    ]
    
    print("Inserting...")
    try:
        db.bulk_insert(records)
        print("Insert Success!")
    except Exception as e:
        print(f"Insert Failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_db_operations()
