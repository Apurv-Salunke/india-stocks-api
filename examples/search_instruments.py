"""
Instrument Search: Look up tradable instruments in the database.

The SDK auto-provisions an instrument database when you instantiate a broker.
This example shows how to search for instruments directly.

Demonstrates:
    - Looking up equity symbols
    - Finding futures contracts
    - Finding options by strike/expiry
    - Direct database queries for advanced searches

Expected output:
    === Equity Lookup ===
    RELIANCE found: token=2885, exchange=NSE

    === Futures Lookup ===
    NIFTY Jan 2025 FUT: token=35001, tradingsymbol=NIFTY25JANFUT

    === Options Lookup ===
    BANKNIFTY 48000 CE: token=42567, tradingsymbol=BANKNIFTY25JAN4800CE

    === Search Results for 'INFY' ===
    Found 25 instruments:
      INFY (NSE) - Equity
      INFY25JANFUT (NFO) - Future
      INFY25JAN1800CE (NFO) - Option
      ...
"""

import os
import sys
from datetime import date, timedelta

from dotenv import load_dotenv

load_dotenv()


def search_instruments(db, query: str, exchange: str = None, limit: int = 20):
    """
    Search instruments by symbol pattern.

    Args:
        db: InstrumentDB instance
        query: Symbol to search (supports SQL LIKE patterns)
        exchange: Filter by exchange (NSE, NFO, BSE, etc.)
        limit: Maximum results to return
    """
    from india_stocks_api.instruments.database import InstrumentMaster

    session = db.get_session()
    try:
        q = session.query(InstrumentMaster).filter(InstrumentMaster.symbol.like(f"{query}%"))

        if exchange:
            q = q.filter(InstrumentMaster.exchange == exchange)

        return q.limit(limit).all()
    finally:
        session.close()


def main():
    from india_stocks_api.brokers import AngelOne
    from india_stocks_api.constants import OptionType
    from india_stocks_api.instruments import Equity, Future, Option
    from india_stocks_api.instruments.database import InstrumentDB
    from india_stocks_api.internal.context import get_instruments_db_path

    # Setup - need to instantiate broker to ensure DB is provisioned
    api_key = os.getenv("ANGEL_API_KEY")
    client_code = os.getenv("ANGEL_CLIENT_ID")
    password = os.getenv("ANGEL_PIN")
    totp_key = os.getenv("ANGEL_TOTP_SECRET")

    if not all([api_key, client_code, password, totp_key]):
        print("Error: Missing credentials. Check your .env file.")
        sys.exit(1)

    # Instantiate broker to auto-provision instrument database
    # Note: The broker instance itself triggers DB provisioning via metaclass
    _broker = AngelOne(
        api_key=api_key,
        client_code=client_code,
        password=password,
        totp_key=totp_key,
    )
    del _broker  # Only needed for DB provisioning

    # Get the instrument database path
    db_path = get_instruments_db_path()
    print(f"Using instrument database: {db_path}")
    print()

    # --- Method 1: Use domain objects (preferred for trading) ---
    print("=== Using Domain Objects ===")

    # Equity - just symbol and exchange
    reliance = Equity("RELIANCE", exchange="NSE")
    print(f"Equity: {reliance}")

    today = date.today()

    # simple next Thursday calculation (NFO index expiries are Thursdays)
    days_ahead = (3 - today.weekday()) % 7  # Thursday = 3
    if days_ahead == 0:
        days_ahead = 7

    next_expiry = today + timedelta(days=days_ahead)

    # Future - symbol, expiry date, exchange
    # Note: Use actual expiry dates from NSE calendar
    nifty_fut = Future("NIFTY", expiry=next_expiry, exchange="NFO")
    print(f"Future: {nifty_fut}")

    # Option - symbol, expiry, strike, option type, exchange
    banknifty_opt = Option(
        symbol="BANKNIFTY",
        expiry=next_expiry,
        strike=48000.0,
        opt_type=OptionType.CE,
        exchange="NFO",
    )
    print(f"Option: {banknifty_opt}")
    print()

    # --- Method 2: Direct database search ---
    print("=== Database Search ===")

    db = InstrumentDB(str(db_path))

    # Search for INFY instruments
    results = search_instruments(db, "INFY", limit=10)
    print(f"Found {len(results)} instruments matching 'INFY':")
    for r in results[:5]:
        inst_type = r.instrument_type or "Unknown"
        print(f"  {r.tradingsymbol} ({r.exchange}) - {inst_type}")

    if len(results) > 5:
        print(f"  ... and {len(results) - 5} more")
    print()

    # Search for specific exchange
    nfo_results = search_instruments(db, "NIFTY", exchange="NFO", limit=10)
    print(f"Found {len(nfo_results)} NIFTY instruments in NFO:")
    for r in nfo_results[:5]:
        expiry_str = r.expiry.strftime("%d-%b") if r.expiry else "N/A"
        print(f"  {r.tradingsymbol} | Expiry: {expiry_str} | Strike: {r.strike or 'N/A'}")

    # --- Lookup specific instrument token ---
    print()
    print("=== Token Lookup ===")
    record = db.lookup_token("RELIANCE", "NSE")
    if record:
        print(f"RELIANCE token: {record.token}")
        print(f"Trading symbol: {record.tradingsymbol}")
        print(f"Lot size: {record.lot_size}")


if __name__ == "__main__":
    main()
