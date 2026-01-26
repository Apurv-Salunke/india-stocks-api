
import sys
import os
import sqlite3

# Add project root to path
sys.path.append(os.getcwd())

from india_stocks_api.internal.fivepaisa.database.master_contract_db import master_contract_download
from india_stocks_api.instruments.database import InstrumentDB

def test_download():
    db_path = "test_fp_instruments.db"
    if os.path.exists(db_path):
        os.remove(db_path)
    
    print(f"Downloading master contract to {db_path}...")
    try:
        master_contract_download(db_path)
        print("Download finished.")
        
        # Verify
        db = InstrumentDB(db_path)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT count(*) FROM instruments")
        count = cursor.fetchone()[0]
        print(f"Total records: {count}")
        
        cursor.execute("SELECT instrument_type, count(*) FROM instruments GROUP BY instrument_type")
        rows = cursor.fetchall()
        print("Instrument Type Stats:")
        for row in rows:
            print(f"{row[0]}: {row[1]}")
            
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_download()
