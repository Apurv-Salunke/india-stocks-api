import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from india_stocks_api.brokers import AngelOne
from india_stocks_api.constants import OrderType, ProductType, TransactionType
from india_stocks_api.instruments import Equity


def test_trading_workflow():
    print("--- Testing Trading Workflow (Buy Limit + Cancel) ---")

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

    # 1. Holdings
    print("\n1. Fetching Holdings...")
    try:
        holdings = client.get_holdings()
        print(f"   ✅ Holdings count: {len(holdings)}")
        if holdings and len(holdings) > 0:
            print(f"   First Holding: {holdings[0]}")
    except Exception as e:
        print(f"   ❌ Holdings Failed: {e}")

    # 2. Orders (Before)
    print("\n2. Fetching Order Book (Initial)...")
    try:
        orders = client.get_orders()
        print(f"   ✅ Open Orders count: {len(orders)}")
    except Exception as e:
        print(f"   ❌ Orders Failed: {e}")

    # 3. Place Limit Order (SBIN)
    # Using 'NSE' exchange.
    symbol_to_trade = "SBIN"
    print(f"\n3. Placing Limit Buy Order for {symbol_to_trade} @ 200.0...")

    order_id = None
    try:
        inst = Equity(symbol_to_trade, exchange="NSE")

        # Limit Buy @ 200.0
        resp = client.place_order(
            instrument=inst,
            transaction_type=TransactionType.BUY,
            quantity=1,
            order_type=OrderType.LIMIT,
            product_type=ProductType.DELIVERY,  # CNC
            price=2.0,
        )

        print(f"   Response: {resp}")

        if resp["status"] == "success":
            order_id = resp["order_id"]
            print(f"   ✅ Order Placed! ID: {order_id}")
        else:
            print(f"   ❌ Order Placement Failed logic: {resp}")

    except Exception as e:
        print(f"   ❌ Order Placement Exception: {e}")
        # If symbol resolution failed, maybe check DB?
        import traceback

        traceback.print_exc()

    # 4. Cancel Order (If placed)
    if order_id:
        print(f"\n4. Cancelling Order {order_id}...")
        try:
            c_resp = client.cancel_order(order_id)
            print(f"   Cancel Response: {c_resp}")
            if c_resp["status"] == "success":
                print("   ✅ Order Cancelled!")
            else:
                print("   ❌ Cancel Failed.")
        except Exception as e:
            print(f"   ❌ Cancel Exception: {e}")


if __name__ == "__main__":
    test_trading_workflow()
