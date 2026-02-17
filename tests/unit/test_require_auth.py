"""Tests for BaseBroker._require_auth() guard.

Validates token presence checks and IST midnight expiry logic.
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

from india_stocks_api.brokers.angel import AngelOne
from india_stocks_api.exceptions import AuthenticationError, SessionExpiredError
from india_stocks_api.internal import context

_IST = ZoneInfo("Asia/Kolkata")


@pytest.fixture(autouse=True)
def _skip_instruments(mocker):
    mocker.patch.object(AngelOne, "_ensure_instruments_ready")


@pytest.fixture()
def broker(dummy_creds):
    return AngelOne(**dummy_creds)


class TestNoToken:

    def test_raises_authentication_error(self, broker):
        context._CONFIG["access_token"] = None
        with pytest.raises(AuthenticationError, match="Not authenticated"):
            broker._require_auth()


class TestTokenPresentNoSession:

    def test_returns_token_gracefully(self, broker, tmp_session_file):
        context._CONFIG["access_token"] = "jwt_abc"
        # No session saved → no expires_at → should just return token
        result = broker._require_auth()
        assert result == "jwt_abc"


class TestTokenPresentSessionNotExpired:

    def test_returns_token(self, broker, tmp_session_file):
        context._CONFIG["access_token"] = "jwt_abc"
        future = (datetime.now(_IST) + timedelta(hours=6)).isoformat()
        context.save_session("angel", {"expires_at": future})

        assert broker._require_auth() == "jwt_abc"


class TestTokenPresentSessionExpired:

    def test_raises_session_expired_error(self, broker, tmp_session_file):
        context._CONFIG["access_token"] = "jwt_abc"
        past = (datetime.now(_IST) - timedelta(hours=1)).isoformat()
        context.save_session("angel", {"expires_at": past})

        with pytest.raises(SessionExpiredError, match="expired"):
            broker._require_auth()

    def test_clears_in_memory_tokens_on_expiry(self, broker, tmp_session_file):
        context._CONFIG["access_token"] = "jwt_abc"
        context._CONFIG["feed_token"] = "feed_abc"
        past = (datetime.now(_IST) - timedelta(hours=1)).isoformat()
        context.save_session("angel", {"expires_at": past})

        with pytest.raises(SessionExpiredError):
            broker._require_auth()

        assert context._CONFIG["access_token"] is None
        assert context._CONFIG["feed_token"] is None


class TestExpiryBoundary:

    def test_exactly_at_current_time_is_expired(self, broker, tmp_session_file):
        """expires_at <= now → expired (boundary: equal)."""
        context._CONFIG["access_token"] = "jwt_abc"
        now = datetime.now(_IST).isoformat()
        context.save_session("angel", {"expires_at": now})

        with pytest.raises(SessionExpiredError):
            broker._require_auth()
