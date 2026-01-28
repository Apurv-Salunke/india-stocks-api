import os
import sys
from pathlib import Path
from datetime import date, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from india_stocks_api.brokers import AngelOne
from india_stocks_api.instruments import Future, Option
from india_stocks_api.constants import OptionType
from india_stocks_api.instruments.database import InstrumentDB, InstrumentMaster

def get_valid_fo_instruments(db):
    """
    Locate the next NIFTY futures contract (expiry on or after today) and a CE option with the same expiry from the given instruments database.
    
    Parameters:
        db: An instruments database accessor with a get_session() method used to query InstrumentMaster records.
    
    Returns:
        tuple: A pair (future_record, option_record). Each is an InstrumentMaster record when found or `None` if no matching record exists.
    """
    session = db.get_session()
    today = date.today()
    
    # 1. Find Future (Next closest expiry)
    print("Searching for NIFTY Future...")
    fut_rec = session.query(InstrumentMaster).filter(
        InstrumentMaster.symbol == "NIFTY",
        InstrumentMaster.instrument_type == "FUT",
        InstrumentMaster.expiry >= today
    ).order_by(InstrumentMaster.expiry).first()
    
    # 2. Find Option (Same expiry as Future, CE, Strike near 23000?)
    # Just grab any valid one
    opt_rec = None
    if fut_rec:
        print(f"Found Future: {fut_rec.tradingsymbol} Expiry: {fut_rec.expiry}")
        opt_rec = session.query(InstrumentMaster).filter(
            InstrumentMaster.symbol == "NIFTY",
            InstrumentMaster.instrument_type == "OPT",
            InstrumentMaster.expiry == fut_rec.expiry,
            InstrumentMaster.opt_type == "CE"
        ).first()
        if opt_rec:
            print(f"Found Option: {opt_rec.tradingsymbol} Strike: {opt_rec.strike}")
            
    session.close()
    return fut_rec, opt_rec

def test_fo_data():
    """
    Run an end-to-end integration test that verifies futures history retrieval and option quote retrieval via the Angel One API.
    
    Connects to the local instruments database to locate a valid NIFTY futures contract and a matching CE option, reads Angel One credentials from environment variables, authenticates with the API, then (1) fetches and prints recent historical data for the selected future and any available open interest and (2) fetches and prints the real-time quote fields (LTP, OI, top bid) for the selected option. Prints clear failure messages and stack traces on errors and returns early if required resources or credentials are missing.
    """
    print("--- Testing Futures & Options Data ---")
    
    # Init DB
    db_path = Path(__file__).parent.parent / "instruments.db"
    db = InstrumentDB(str(db_path))
    
    # Find Instruments
    fut_rec, opt_rec = get_valid_fo_instruments(db)
    
    if not fut_rec or not opt_rec:
        print("❌ Could not find valid Futures/Options in DB to test.")
        return

    # Init Client
    api_key = os.getenv("ANGEL_API_KEY")
    client_id = os.getenv("ANGEL_CLIENT_ID")
    pin = os.getenv("ANGEL_PIN")
    totp = os.getenv("ANGEL_TOTP_SECRET")
    
    if not all([api_key, client_id, pin, totp]):
        print("Error: Missing env vars.")
        return

    client = AngelOne(api_key, client_id, pin, totp)
    if not client.authenticate():
        print("Auth failed.")
        return

    # 1. Get History for Future
    try:
        print(f"\n1. Fetching History for Future: {fut_rec.tradingsymbol}")
        # Create Domain Object
        future_obj = Future(
            symbol="NIFTY",
            expiry=fut_rec.expiry
        )
        
        # Last 5 days
        end = date.today()
        start = end - timedelta(days=5)
        
        df = client.get_history(future_obj, start.isoformat(), end.isoformat(), "1h")
        print(f"   ✅ Received {len(df)} rows.")
        print(df.tail(3))
        
        # Check Open Interest
        if 'oi' in df.columns:
            print(f"   ✅ Open Interest available: {df['oi'].tail(3).values}")
            
    except Exception as e:
        print(f"   ❌ History Failed: {e}")
        import traceback
        traceback.print_exc()

    # 2. Get Quote for Option
    try:
        print(f"\n2. Fetching Quote for Option: {opt_rec.tradingsymbol}")
        option_obj = Option(
            symbol="NIFTY",
            expiry=opt_rec.expiry,
            strike=opt_rec.strike,
            opt_type=OptionType[opt_rec.opt_type] # "CE" -> OptionType.CE
        )
        
        quote = client.get_quote(option_obj)
        print("    ✅ Quote Received!")
        print(f"   LTP: {quote['ltp']}")
        print(f"   OI: {quote['oi']}")
        print(f"   Depth (Top Bid): {quote['bid']}")
        
    except Exception as e:
        print(f"   ❌ Quote Failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_fo_data()