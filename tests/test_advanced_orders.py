import os
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from india_stocks_api.brokers import AngelOne
from india_stocks_api.instruments import Equity
from india_stocks_api.constants import TransactionType, ProductType


def test_advanced_orders():
    print("--- Testing Advanced Order & GTT APIs ---")
    
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
        
    # 1. Standard Filters
    print("\n1. Testing Filters...")
    try:
        time.sleep(1)
        pending = client.get_pending_orders()
        time.sleep(1)
        executed = client.get_executed_orders()
        print(f"   SUCCESS: Pending: {len(pending)}, Executed: {len(executed)}")
        
        if len(pending) > 0:
             order_id = pending[0].get('orderid')
             print(f"   Fetching details for {order_id}...")
             time.sleep(1)
             details = client.get_order_details(order_id)
             print(f"   SUCCESS: Details: {details.get('tradingsymbol')} - {details.get('status')}")
    except Exception as e:
        print(f"   ERROR: Filter/Details Failed: {e}")

    # 2. GTT List
    print("\n2. Fetching GTT List...")
    try:
        time.sleep(1)
        gtt_list = client.get_gtt_list()
        print(f"   SUCCESS: GTT Rules: {len(gtt_list)}")
        if gtt_list:
            print(f"   First GTT ID: {gtt_list[0].get('id')}")
    except Exception as e:
        print(f"   GTT List Failed: {e}")

    # 3. GTT Creation (SBIN)
    # Using 'NSE' exchange.
    print("\n3. Creating GTT Rule for SBIN @ 100.0 (Trigger)...")
    try:
        inst = Equity("SBIN", exchange="NSE")
        
        time.sleep(1)
        res = client.create_gtt(
            instrument=inst,
            transaction_type=TransactionType.BUY,
            quantity=1,
            trigger_price=100.0,
            price=105.0,
            product_type=ProductType.DELIVERY
        )
        
        print(f"   ✅ GTT Response: {res}")
        if res.get('status') and res.get('data'):
            gtt_id = res['data'].get('id')
            print(f"   ✅ GTT Created! ID: {gtt_id}")
            
            # Cancel it immediately
            print(f"   Cancelling GTT {gtt_id}...")
            time.sleep(1)
            c_res = client.cancel_gtt(gtt_id, inst)
            print(f"   ✅ Cancel Response: {c_res}")
        else:
             print(f"   ❌ GTT Create logic failed: {res}")
            
    except Exception as e:
        print(f"   ❌ GTT Workflow Failed: {e}")

if __name__ == "__main__":
    test_advanced_orders()
