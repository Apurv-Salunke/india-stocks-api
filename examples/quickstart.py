"""
Quickstart: Authenticate and fetch LTP for a stock.

Before running:
    1. Copy .env.example to .env
    2. Fill in your Angel One API credentials
    3. Run: python examples/quickstart.py

Expected output:
    Authenticated successfully
    RELIANCE LTP: 2845.50
"""

import os
import sys

from dotenv import load_dotenv

from india_stocks_api.brokers import AngelOne
from india_stocks_api.instruments import Equity

# Load credentials from .env file
load_dotenv()

# Get credentials from environment
api_key = os.getenv("ANGEL_API_KEY")
client_code = os.getenv("ANGEL_CLIENT_ID")
password = os.getenv("ANGEL_PIN")
totp_key = os.getenv("ANGEL_TOTP_SECRET")

if not all([api_key, client_code, password, totp_key]):
    print("Error: Missing credentials. Check your .env file.")
    sys.exit(1)

# Create broker instance (auto-provisions instrument database)
broker = AngelOne(
api_key=api_key,
client_code=client_code,
password=password,
totp_key=totp_key,
)

# Authenticate with Angel One
broker.authenticate()
print("Authenticated successfully")

# Define the instrument
reliance = Equity("IIFL", exchange="NSE")

# Fetch quote and print LTP
quote = broker.get_quote(reliance)
print(f"RELIANCE LTP: {quote.ltp}")
