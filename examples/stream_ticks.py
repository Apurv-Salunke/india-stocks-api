"""
Live Tick Streaming: Real-time market data via WebSocket.

Demonstrates:
    - Setting up WebSocket callbacks
    - Subscribing to instruments
    - Different streaming modes (LTP, Quote, Depth)
    - Graceful shutdown with Ctrl+C

IMPORTANT: start_streaming() is a BLOCKING call.
Use threading or asyncio for non-blocking behavior.

Expected output:
    Authenticated successfully
    Subscribed to 3 instruments
    Starting live stream (press Ctrl+C to stop)...

    [TICK] RELIANCE | LTP: 2845.50 | Vol: 1234567
    [TICK] SBIN | LTP: 825.40 | Vol: 9876543
    [TICK] TCS | LTP: 3920.15 | Vol: 456789
    ...

    Stream stopped
"""

import os
import signal
import sys

from dotenv import load_dotenv

load_dotenv()

# Global reference for signal handler
broker = None


def handle_shutdown(signum, frame):
    """Handle Ctrl+C gracefully."""
    print("\nShutting down...")
    if broker:
        broker.stop_streaming()
    sys.exit(0)


def main():
    global broker

    from india_stocks_api.brokers import AngelOne
    from india_stocks_api.constants import StreamMode
    from india_stocks_api.instruments import Equity
    from india_stocks_api.responses import WebSocketTick

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
    print("Authenticated successfully")

    # --- Define instruments to stream ---
    instruments = [
        Equity("RELIANCE", exchange="NSE"),
        Equity("SBIN", exchange="NSE"),
        Equity("TCS", exchange="BSE"),
    ]

    # --- Setup callbacks ---

    def on_tick(tick: WebSocketTick):
        """Called for each incoming tick."""
        print(f"[TICK] {tick.symbol} | LTP: {tick.ltp} | Vol: {tick.volume}")

    def on_open():
        """Called when WebSocket connection is established."""
        print("WebSocket connected")

    def on_error(error_type, error_msg):
        """Called on WebSocket errors."""
        print(f"[ERROR] {error_type}: {error_msg}")

    def on_close():
        """Called when WebSocket connection closes."""
        print("WebSocket disconnected")

    # Assign callbacks to broker
    broker.on_tick = on_tick
    broker.on_open = on_open
    broker.on_error = on_error
    broker.on_close = on_close

    # --- Subscribe to instruments ---
    # StreamMode options:
    #   LTP (1) - Last traded price only
    #   QUOTE (2) - LTP + OHLC + Volume
    #   SNAP_QUOTE (3) - Quote + Best 5 Bid/Ask
    #   DEPTH (4) - Full 20-level depth (NSE CM only)

    broker.subscribe(instruments, mode=StreamMode.QUOTE)
    print(f"Subscribed to {len(instruments)} instruments")

    # Setup signal handler for graceful shutdown
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # --- Start streaming (BLOCKING) ---
    print("Starting live stream (press Ctrl+C to stop)...")
    print()

    try:
        broker.start_streaming()
    except KeyboardInterrupt:
        pass
    finally:
        broker.stop_streaming()
        print("Stream stopped")


if __name__ == "__main__":
    main()
