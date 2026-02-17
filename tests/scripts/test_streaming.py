"""
Test WebSocket Streaming
Run with: poetry run python tests/test_streaming.py
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from india_stocks_api.brokers import AngelOne
from india_stocks_api.constants import StreamMode
from india_stocks_api.instruments import Equity


def test_streaming():
    print("--- Testing WebSocket Streaming ---")

    api_key = os.getenv("ANGEL_API_KEY")
    client_id = os.getenv("ANGEL_CLIENT_ID")
    pin = os.getenv("ANGEL_PIN")
    totp = os.getenv("ANGEL_TOTP_SECRET")

    if not all([api_key, client_id, pin, totp]):
        print("Error: Missing env vars.")
        print("Set: ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PIN, ANGEL_TOTP_SECRET")
        return

    # 1. Initialize & Authenticate
    print("1. Authenticating...")
    broker = AngelOne(api_key, client_id, pin, totp)
    if not broker.authenticate():
        print("Auth failed.")
        return
    print("   ✅ Authenticated!")

    # 2. Set Callbacks
    tick_count = [0]  # Use list for mutable closure

    def on_tick(tick):
        tick_count[0] += 1
        symbol = tick.get("token", "UNKNOWN")
        ltp = tick.get("last_traded_price", 0) / 100.0  # Price is in paise
        mode = tick.get("subscription_mode_val", "LTP")
        print(f"   [{tick_count[0]}] Token: {symbol} | LTP: {ltp:.2f} | Mode: {mode}")

        # Stop after 10 ticks for testing
        if tick_count[0] >= 10:
            print("\n   Received 10 ticks. Stopping...")
            broker.stop_streaming()

    def on_open():
        print("   ✅ WebSocket Connected!")

    def on_error(error_type, error_msg):
        print(f"   ❌ Error: {error_type} - {error_msg}")

    def on_close():
        print("   WebSocket Closed.")

    broker.on_tick = on_tick
    broker.on_open = on_open
    broker.on_error = on_error
    broker.on_close = on_close

    # 3. Subscribe
    print("2. Subscribing to RELIANCE...")
    broker.subscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)

    # 4. Start Streaming (Blocking)
    print("3. Starting stream (will stop after 10 ticks)...")
    try:
        broker.start_streaming()
    except KeyboardInterrupt:
        print("\n   Interrupted by user.")
        broker.stop_streaming()

    print("\n--- Test Complete ---")


if __name__ == "__main__":
    test_streaming()
