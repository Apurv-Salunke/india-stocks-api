"""
Instrument Builder
Downloads Master Contract from Angel One and populates the local SQLite DB.
"""
import requests
import json
import logging
from datetime import datetime
from .database import InstrumentDB

logger = logging.getLogger(__name__)

ANGEL_MASTER_URL = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'

def parse_date(date_str):
    """Convert '28MAR2024' -> date(2024, 3, 28)"""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, '%d%b%Y')
        return dt.date()
    except ValueError:
        return None

def build_instruments(db: InstrumentDB):
    """
    Downloads raw JSON, normalizes it, and saves to DB.
    """
    logger.info(f"Downloading master contract from {ANGEL_MASTER_URL}...")
    response = requests.get(ANGEL_MASTER_URL, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    logger.info(f"Downloaded {len(data)} instruments. Processing...")
    
    records = []
    seen_tokens = set()
    
    for item in data:
        # 1. Basic Extraction
        token = item.get("token")
        raw_symbol = item.get("symbol") # e.g. "RELIANCE-EQ" or "NIFTY28MAR24FUT"
        name = item.get("name")         # e.g. "RELIANCE" or "NIFTY"
        exch_seg = item.get("exch_seg") # "NSE", "NFO"
        strike_price = float(item.get("strike", 0)) / 100.0
        expiry_str = item.get("expiry")
        inst_type = item.get("instrumenttype") # "OPTIDX", "FUTIDX", "EQ"
        
        # 2. Normalization (OpenAlgo Logic)
        
        # Symbol
        symbol = name  # For most things, the 'name' is the underlying (NIFTY, RELIANCE)
        
        if symbol == "Nifty 50": symbol = "NIFTY"
        if symbol == "Nifty Bank": symbol = "BANKNIFTY"
        
        # Expiry
        expiry_date = parse_date(expiry_str)
        
        # Option Type
        opt_type = None
        if inst_type in ["OPTIDX", "OPTSTK"]:
            if raw_symbol.endswith("CE"):
                opt_type = "CE"
            elif raw_symbol.endswith("PE"):
                opt_type = "PE"
        
        # Determine Instrument Class
        i_type = "EQ"
        if "FUT" in inst_type: i_type = "FUT"
        elif "OPT" in inst_type: i_type = "OPT"
        elif "IDX" in inst_type and "FUT" not in inst_type and "OPT" not in inst_type: i_type = "IDX"
        
        record = {
            "token": token,
            "symbol": symbol,          # Standard: "NIFTY"
            "exchange": exch_seg,      # "NFO"
            "tradingsymbol": raw_symbol, # "NIFTY28MAR2418000CE"
            "br_symbol": raw_symbol,
            "expiry": expiry_date,
            "strike": strike_price if strike_price > 0 else None,
            "opt_type": opt_type,
            "lot_size": int(item.get("lotsize", 1)),
            "tick_size": float(item.get("tick_size", 0.05)) / 100.0,
            "instrument_type": i_type
        }
        
        if token in seen_tokens:
             continue
        seen_tokens.add(token)
        
        records.append(record)
        
    logger.info(f"Inserting {len(records)} records into DB in chunks...")
    
    CHUNK_SIZE = 10000
    for i in range(0, len(records), CHUNK_SIZE):
        chunk = records[i:i + CHUNK_SIZE]
        logger.info(f"Inserting chunk {i} to {i+CHUNK_SIZE}...")
        try:
            db.raw_bulk_insert(chunk, truncate=(i==0))
        except Exception as e:
            logger.error(f"Failed to insert chunk {i}: {e}")
            # Optional: Break or Continue based on preference
            raise e
            
    logger.info("Database build complete.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db = InstrumentDB() # Creates instruments.db in CWD
    build_instruments(db)
