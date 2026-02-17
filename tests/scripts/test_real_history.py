import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from india_stocks_api.brokers import AngelOne
from india_stocks_api.constants import CandleInterval
from india_stocks_api.instruments import Equity


def test_history():
    print("--- Testing Historical Data with Enum ---")

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

    print("Getting History for RELIANCE (NSE)...")
    try:
        inst = Equity("RELIANCE")

        # Last 5 days
        end = date.today()
        start = end - timedelta(days=5)

        df = client.get_history(
            inst, start_date=start.isoformat(), end_date=end.isoformat(), interval=CandleInterval.FIFTEEN_MINUTE
        )

        print("\n✅ History Received!")
        print(f"Rows: {len(df)}")
        print("\nHead:")
        print(df.head())
        print("\nTail:")
        print(df.tail())

    except Exception as e:
        print(f"❌ History Failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_history()
