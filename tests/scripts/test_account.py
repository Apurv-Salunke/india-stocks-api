import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load .env from project root (needed when running this file directly)
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

from india_stocks_api.brokers import AngelOne

def test_account_info():
    print("--- Testing Account & Portfolio Data ---")
    
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
        
    # 1. Profile
    print("\n1. Fetching Profile...")
    try:
        profile = client.get_profile()
        print(f"   ✅ Profile Received!")
        print(f"   Name: {profile.get('name')}")
        print(f"   Exchanges: {profile.get('exchanges')}")
        print(f"   Client ID: {profile.get('clientcode')}")
    except Exception as e:
        print(f"   ❌ Profile Failed: {e}")

    # 2. Trades
    print("\n2. Fetching Trade Book...")
    try:
        trades = client.get_trades()
        print(f"   ✅ Trade Book Loaded! count: {len(trades)}")
        if trades:
            print(f"   Last Trade: {trades[-1].get('tradingsymbol')} @ {trades[-1].get('fillprice')}")
    except Exception as e:
        print(f"   ❌ Trades Failed: {e}")

    # 3. Repeat Positions (Just to verify again)
    print("\n3. Fetching Positions (Raw)...")
    try:
        positions = client.get_positions()
        # Angel get_positions returns raw resp if mapped via order_api
        # Wait, in angel.py get_positions returns get_positions(jwt)
        # Internal get_positions returns json.loads(response.text)
        print(f"   ✅ Positions API raw status: {positions.get('status')}")
        if positions.get('data'):
            print(f"   Open Positions: {len(positions.get('data'))}")
        else:
            print("   No open positions.")
    except Exception as e:
        print(f"   ❌ Positions Failed: {e}")

if __name__ == "__main__":
    test_account_info()
