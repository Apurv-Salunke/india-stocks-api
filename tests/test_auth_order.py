import os
import pytest
from dotenv import load_dotenv

from india_stocks_api.brokers.angel import AngelOne
from india_stocks_api.internal.context import get_credentials, get_auth_token, get_feed_token
from india_stocks_api.instruments import Equity
from india_stocks_api.constants import TransactionType, OrderType, ProductType, OrderValidity
load_dotenv()

RUN_LIVE_ORDER = os.getenv("RUN_LIVE_ORDER") == "1"


# ---------- helpers ----------
    
def load_credentials():
    api_key = os.getenv("ANGEL_API_KEY")
    client = os.getenv("ANGEL_CLIENT_ID")
    pin = os.getenv("ANGEL_PIN")        # MPIN
    totp = os.getenv("ANGEL_TOTP_SECRET")

    if not all([api_key, client, pin, totp]):
        stored = get_credentials("angel") or {}
        api_key = api_key or stored.get("api_key")
        client = client or stored.get("client_code")
        pin = pin or stored.get("password")
        totp = totp or stored.get("totp_key")

    assert all([api_key, client, pin, totp]), "Missing credentials"

    return api_key, client, pin, totp


def create_broker():
    api_key, client, pin, totp = load_credentials()

    return AngelOne(
        api_key=api_key,
        client_code=client,
        password=pin,
        totp_key=totp,
    )

@pytest.mark.skipif(not RUN_LIVE_ORDER, reason="Live order disabled")
def test_place_equity_order_raw():
    """
    Place order WITHOUT instrument resolver.

    Uses raw AngelOne parameters.
    """

    broker = create_broker()
    auth = broker.authenticate()

    assert auth.success, f"Login failed: {auth.error}"

    reliance= Equity("RELIANCE", "NSE")
    order = broker.place_order(
        instrument=reliance,
        transaction_type=TransactionType.BUY,
        quantity=1,
        order_type=OrderType.LIMIT,
        product_type=ProductType.DELIVERY, # CNC
        price=2.0,
        validity=OrderValidity.DAY,
    )
    
    assert order.success, f"Order failed: {order.error}"
    assert order.execution_id, "Missing execution id"

    print("\n✓ Order placed")
    print(order)
