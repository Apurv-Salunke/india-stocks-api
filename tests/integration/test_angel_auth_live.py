"""Live integration test for AngelOne authentication.

Requires env vars: ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PIN, ANGEL_TOTP_SECRET.
Skipped automatically when credentials are missing.

Authenticates ONCE per class to avoid Angel API rate-limiting on TOTP reuse
within the same 30-second window.
"""
import os
import pytest

from india_stocks_api.brokers.angel import AngelOne
from india_stocks_api.internal.context import get_auth_token, get_feed_token, load_session


def _get_creds():
    return {
        "api_key": os.getenv("ANGEL_API_KEY", ""),
        "client_code": os.getenv("ANGEL_CLIENT_ID", ""),
        "password": os.getenv("ANGEL_PIN", ""),
        "totp_key": os.getenv("ANGEL_TOTP_SECRET", ""),
    }


_missing = not all(_get_creds().values())


@pytest.fixture(scope="module")
def angel_session():
    """Authenticate once for the entire module and return (broker, creds)."""
    creds = _get_creds()
    broker = AngelOne(**creds)
    broker.authenticate()
    return broker, creds


@pytest.mark.integration
@pytest.mark.skipif(_missing, reason="Live Angel credentials not set in env")
class TestAngelAuthLive:

    def test_authenticate_returns_true(self, angel_session):
        broker, _ = angel_session
        # If we got here, authenticate() already returned True (no exception).
        assert get_auth_token() is not None

    def test_in_memory_tokens_set(self, angel_session):
        assert get_auth_token() is not None
        assert get_feed_token() is not None

    def test_session_persisted_with_correct_keys(self, angel_session):
        session = load_session("angel")

        assert "access_token" in session
        assert "feed_token" in session
        assert "api_key" in session
        assert "client_code" in session
        assert "authenticated_at" in session
        assert "expires_at" in session

    def test_no_secrets_on_disk(self, angel_session):
        session = load_session("angel")

        assert "password" not in session
        assert "totp_key" not in session

    def test_session_identity_matches(self, angel_session):
        _, creds = angel_session
        session = load_session("angel")

        assert session["api_key"] == creds["api_key"]
        assert session["client_code"] == creds["client_code"]
