import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from india_stocks_api.brokers import AngelOne

def test_real_angel_integration():
    print("--- Testing Real Angel One Integration ---")
    
    # Read credentials from Environment (Set these when running the script!)
    api_key = os.getenv("ANGEL_API_KEY")
    client_id = os.getenv("ANGEL_CLIENT_ID")
    pin = os.getenv("ANGEL_PIN")
    totp = os.getenv("ANGEL_TOTP_SECRET")
    
    if not all([api_key, client_id, pin, totp]):
        print("Error: Missing environment variables.")
        print("Please set: ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PIN, ANGEL_TOTP_SECRET")
        return

    # 1. Initialize
    print(f"1. Initializing client for {client_id}...")
    client = AngelOne(
        api_key=api_key,
        client_code=client_id,
        password=pin,
        totp_key=totp
    )
    
    # 2. Login
    print("2. Authenticating...")
    try:
        if client.authenticate():
            print("   ✅ Authentication Successful!")
        else:
            print("   ❌ Authentication Failed.")
            return
    except Exception as e:
        print(f"   ❌ Authentication Exception: {e}")
        return

    # 3. Get Funds
    print("3. Fetching Funds...")
    try:
        funds = client.get_funds()
        print(f"   ✅ Funds Response: {funds}")
    except Exception as e:
        print(f"   ❌ Funds Exception: {e}")

if __name__ == "__main__":
    test_real_angel_integration()
