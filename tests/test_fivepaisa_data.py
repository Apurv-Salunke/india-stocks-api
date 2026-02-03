"""
Integration test for FivePaisa data (REAL API smoke test)
"""

import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from india_stocks_api.brokers.fivepaisa import FivePaisa
from india_stocks_api.instruments.models import Equity
from india_stocks_api.constants import CandleInterval
from india_stocks_api.internal.context import get_auth_token, get_credentials

load_dotenv()


def test_fivepaisa_data_integration():
    """Simple real API smoke test (quote + history + funds)"""

    # -------- credentials --------
    api_key = os.getenv("FIVEPAISA_API_KEY")
    clientcode = os.getenv("FIVEPAISA_CLIENT_CODE")
    broker_pin = os.getenv("FIVEPAISA_PIN")
    totp = os.getenv("FIVEPAISA_TOTP")
    user_id = os.getenv("FIVEPAISA_USER_ID")
    api_secret = os.getenv("FIVEPAISA_ENCRYPTION_KEY")

    if not all([api_key, clientcode, broker_pin, totp]):
        stored = get_credentials("fivepaisa")
        api_key = api_key or stored.get("api_key")
        clientcode = clientcode or stored.get("clientcode")
        broker_pin = broker_pin or stored.get("broker_pin")
        totp = totp or stored.get("totp_code")
        user_id = user_id or stored.get("user_id")
        api_secret = api_secret or stored.get("api_secret")

    assert all([api_key, clientcode, broker_pin, totp, user_id, api_secret]), "Missing credentials"

    # -------- auth --------
    broker = FivePaisa(
        api_key=api_key,
        clientcode=clientcode,
        broker_pin=broker_pin,
        totp_code=totp,
        api_secret=api_secret,
        user_id=user_id
    )

    assert broker.authenticate() is True

    token = get_auth_token()
    assert token is not None
    print("✓ Auth success")

    # -------- quote test --------
    quote = broker.get_quote(Equity("ABB", "NSE"))
    assert quote and quote["ltp"] > 0
    print(f"✓ Quote OK: {quote['ltp']}")

    # -------- history test --------
    end = datetime.now()
    start = end - timedelta(days=3)

    history = broker.get_history(
        instrument=Equity("ABB", "NSE"),
        start_date=start.strftime("%Y-%m-%d"),
        end_date=end.strftime("%Y-%m-%d"),
        interval=CandleInterval.ONE_DAY
    )

    assert history is not None and len(history) > 0
    print(f"✓ History OK: {len(history)} candles")

    # -------- funds test --------
    funds = broker.get_funds()
    assert funds is not None
    print("✓ Funds OK")

    print("FivePaisa data integration passed")
