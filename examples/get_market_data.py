"""
Market Data: Fetch quotes and historical OHLC candles.

Demonstrates:
    - Getting real-time quotes (LTP, bid/ask, OHLC)
    - Fetching historical candle data
    - Market depth (Level 2 data)

Expected output:
    === Quote Data ===
    Symbol: SBIN
    LTP: 825.40
    Bid: 825.35 | Ask: 825.45
    Open: 820.00 | High: 830.50 | Low: 818.20
    Volume: 12345678

    === Historical Data (5-min candles) ===
    Fetched 75 candles
    Latest: 2024-01-15 15:25 | O: 824.50 H: 825.00 L: 824.00 C: 825.40

    === Market Depth ===
    Top 3 Bids:
      825.35 x 500
      825.30 x 1200
      825.25 x 800
    Top 3 Asks:
      825.45 x 600
      825.50 x 900
      825.55 x 1500
"""

import os
import sys
from datetime import date, datetime, timedelta

from dotenv import load_dotenv

load_dotenv()


def main():
    from india_stocks_api.brokers import AngelOne
    from india_stocks_api.constants import CandleInterval
    from india_stocks_api.instruments import Equity, Index

    # Setup credentials
    api_key = os.getenv("ANGEL_API_KEY")
    client_code = os.getenv("ANGEL_CLIENT_ID")
    password = os.getenv("ANGEL_PIN")
    totp_key = os.getenv("ANGEL_TOTP_SECRET")

    if not all([api_key, client_code, password, totp_key]):
        print("Error: Missing credentials. Check your .env file.")
        sys.exit(1)

    broker = AngelOne(
        api_key=api_key,
        client_code=client_code,
        password=password,
        totp_key=totp_key,
    )
    broker.authenticate()

    # Define instruments
    sbin = Equity("SBIN", exchange="NSE")
    nifty = Index("NIFTY 500", exchange="NSE")

    # --- Quote Data ---
    print("=== Quote Data ===")
    quote = broker.get_quote(sbin)
    print("Symbol: SBIN")
    print(f"LTP: {quote.ltp}")
    print(f"Bid: {quote.bid} | Ask: {quote.ask}")
    print(f"Open: {quote.open} | High: {quote.high} | Low: {quote.low}")
    print(f"Volume: {quote.volume}")
    print()

    # --- Historical Data ---
    print("=== Historical Data (5-min candles) ===")

    # Get last 5 days of data
    end_date = date.today().strftime("%Y-%m-%d")
    start_date = (date.today() - timedelta(days=5)).strftime("%Y-%m-%d")

    history = broker.get_history(
        instrument=sbin,
        start_date=start_date,
        end_date=end_date,
        interval=CandleInterval.FIVE_MINUTE,
    )

    print(f"Fetched {len(history.candles)} candles")
    if history.candles:
        latest = history.candles[-1]
        # Convert timestamp to readable format
        ts = datetime.fromtimestamp(latest.timestamp)
        print(f"Latest: {ts:%Y-%m-%d %H:%M} | O: {latest.open} H: {latest.high} L: {latest.low} C: {latest.close}")
    print()

    # --- Market Depth ---
    print("=== Market Depth ===")
    depth = broker.get_depth(sbin)

    print("Top 3 Bids:")
    for level in depth.bids[:3]:
        print(f"  {level.price} x {level.quantity}")

    print("Top 3 Asks:")
    for level in depth.asks[:3]:
        print(f"  {level.price} x {level.quantity}")

    # --- Index Quote ---
    print()
    print("=== Index Quote ===")
    index_quote = broker.get_quote(nifty)
    print(f"NIFTY 50 LTP: {index_quote.ltp}")


if __name__ == "__main__":
    main()
