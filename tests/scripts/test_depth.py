import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from india_stocks_api.brokers import AngelOne
from india_stocks_api.instruments import Equity


def test_depth_api():
    print("--- Testing Depth API ---")

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

    print("Fetching Depth for RELIANCE (NSE)...")
    try:
        inst = Equity("RELIANCE")
        depth = client.get_depth(inst)

        print("\n✅ Depth Received!")
        print(f"Top 5 Bids: {depth['bids']}")
        print(f"Top 5 Asks: {depth['asks']}")
        print(f"Total Buy Qty: {depth['totalbuyqty']}")
        print(f"Total Sell Qty: {depth['totalsellqty']}")

    except Exception as e:
        print(f"❌ Depth Failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_depth_api()
