import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from india_stocks_api.brokers import AngelOne
from india_stocks_api.instruments import Equity


def test_real_quote():
    print("--- Testing Real Angel One Quote ---")

    api_key = os.getenv("ANGEL_API_KEY")
    client_id = os.getenv("ANGEL_CLIENT_ID")
    pin = os.getenv("ANGEL_PIN")
    totp = os.getenv("ANGEL_TOTP_SECRET")

    if not all([api_key, client_id, pin, totp]):
        print("Error: Missing env vars.")
        return

    client = AngelOne(api_key, client_id, pin, totp)

    print("1. Authenticating...")
    if not client.authenticate():
        print("Auth failed.")
        return

    print("2. Getting Quote for RELIANCE (NSE)...")
    try:
        # Create Domain Object
        inst = Equity("RELIANCE", exchange="NSE")

        quote = client.get_quote(inst)

        print("   ✅ Quote Received!")
        print(f"   LTP: {quote['ltp']}")
        print(f"   Volume: {quote['volume']}")
        print(f"   Full Data: {quote}")

    except Exception as e:
        print(f"   ❌ Quote Failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_real_quote()
