"""
Broker integrations using OpenAlgo implementations

Unified API for 24+ Indian stock brokers:
- AngelOne, Zerodha, Upstox, Dhan, Fyers, Groww, Kotak
- AliceBlue, Flattrade, Shoonya, 5Paisa, Firstock, and more

Example:
    >>> from india_stocks_api.brokers import Broker
    >>>
    >>> broker = Broker("angelone")
    >>> broker.authenticate({
    ...     "user_id": "A1234",
    ...     "pin": "1234",
    ...     "totp_secret": "BASE32SECRET",
    ...     "api_key": "your_key"
    ... })
    >>>
    >>> # Place order
    >>> order = broker.place_order("RELIANCE", "NSE", "BUY", 10)
    >>>
    >>> # Get positions
    >>> positions = broker.get_positions()
    >>>
    >>> # Get historical data
    >>> from datetime import datetime, timedelta
    >>> end = datetime.now()
    >>> start = end - timedelta(days=1)
    >>> candles = broker.get_historical_data("RELIANCE", "NSE", "1m", start, end)
"""

# Broker integrations using OpenAlgo implementations
# Individual broker modules are available in their respective directories

__all__ = []
