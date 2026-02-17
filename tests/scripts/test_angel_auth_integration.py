import os
from dotenv import load_dotenv
from india_stocks_api.brokers.angel import AngelOne
from india_stocks_api.internal.context import get_auth_token, get_feed_token, load_session

load_dotenv()


def test_angel_one_authentication():
    """Test real AngelOne authentication integration."""
    api_key = os.getenv("ANGEL_API_KEY")
    clientcode = os.getenv("ANGEL_CLIENT_ID")
    password = os.getenv("ANGEL_PIN")
    totp = os.getenv("ANGEL_TOTP_SECRET")

    assert all([api_key, clientcode, password, totp]), \
        "Missing credentials - set ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PIN, ANGEL_TOTP_SECRET"

    print(f"Using credentials: clientcode={clientcode[:4]}XXXX")

    broker = AngelOne(api_key=api_key, client_code=clientcode, password=password, totp_key=totp)
    auth_result = broker.authenticate()

    assert auth_result is True

    # In-memory tokens should be set
    assert get_auth_token() is not None
    assert get_feed_token() is not None

    # Session should be persisted with only non-secret data
    session = load_session("angel")
    assert "access_token" in session
    assert "feed_token" in session
    assert "api_key" in session
    assert "client_code" in session
    assert "authenticated_at" in session
    assert "expires_at" in session

    # Secrets must NOT be on disk
    assert "password" not in session
    assert "totp_key" not in session

    assert session["api_key"] == api_key
    assert session["client_code"] == clientcode
