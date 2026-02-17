"""Tests for AngelOne.authenticate() — the public-facing auth method.

All network calls are mocked; tests exercise the orchestration logic:
credential validation, TOTP generation, session persistence, error paths.
"""
import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from zoneinfo import ZoneInfo

from india_stocks_api.brokers.angel import AngelOne
from india_stocks_api.exceptions import AuthenticationError
from india_stocks_api.internal import context

_IST = ZoneInfo("Asia/Kolkata")


@pytest.fixture(autouse=True)
def _skip_instruments(mocker):
    """Bypass instrument DB download during broker instantiation."""
    mocker.patch.object(AngelOne, "_ensure_instruments_ready")


class TestAuthenticateSuccess:

    def test_returns_true(self, dummy_creds, mock_auth_success, tmp_session_file):
        broker = AngelOne(**dummy_creds)
        assert broker.authenticate() is True

    def test_sets_auth_token_in_context(self, dummy_creds, mock_auth_success, tmp_session_file):
        broker = AngelOne(**dummy_creds)
        broker.authenticate()
        assert context.get_auth_token() == "jwt_token_xxx"

    def test_sets_feed_token_in_context(self, dummy_creds, mock_auth_success, tmp_session_file):
        broker = AngelOne(**dummy_creds)
        broker.authenticate()
        assert context.get_feed_token() == "feed_token_xxx"

    def test_persists_session_with_correct_keys(self, dummy_creds, mock_auth_success, tmp_session_file):
        broker = AngelOne(**dummy_creds)
        broker.authenticate()

        session = context.load_session("angel")
        assert session["access_token"] == "jwt_token_xxx"
        assert session["feed_token"] == "feed_token_xxx"
        assert session["api_key"] == dummy_creds["api_key"]
        assert session["client_code"] == dummy_creds["client_code"]
        assert "authenticated_at" in session
        assert "expires_at" in session

    def test_no_secrets_on_disk(self, dummy_creds, mock_auth_success, tmp_session_file):
        broker = AngelOne(**dummy_creds)
        broker.authenticate()

        raw = tmp_session_file.read_text()
        assert dummy_creds["password"] not in raw
        assert dummy_creds["totp_key"] not in raw

    def test_expires_at_is_next_midnight_ist(self, dummy_creds, mock_auth_success, tmp_session_file):
        broker = AngelOne(**dummy_creds)
        broker.authenticate()

        session = context.load_session("angel")
        expires = datetime.fromisoformat(session["expires_at"])

        assert expires.hour == 0
        assert expires.minute == 0
        assert expires.second == 0
        assert expires.tzinfo is not None

    def test_totp_generation_is_called(self, dummy_creds, mock_auth_success, tmp_session_file, mocker):
        totp_mock = mocker.patch("india_stocks_api.brokers.angel.pyotp.TOTP")
        totp_instance = MagicMock()
        totp_instance.now.return_value = "123456"
        totp_mock.return_value = totp_instance

        broker = AngelOne(**dummy_creds)
        broker.authenticate()

        totp_mock.assert_called_once_with(dummy_creds["totp_key"])
        totp_instance.now.assert_called_once()


class TestAuthenticateFailure:

    def test_raises_on_api_failure(self, dummy_creds, mock_auth_failure, tmp_session_file):
        broker = AngelOne(**dummy_creds)
        with pytest.raises(AuthenticationError, match="Invalid TOTP"):
            broker.authenticate()

    def test_missing_api_key(self, tmp_session_file):
        broker = AngelOne(api_key="", client_code="C1", password="1234", totp_key="KEY")
        with pytest.raises(AuthenticationError, match="Incomplete credentials"):
            broker.authenticate()

    def test_missing_client_code(self, tmp_session_file):
        broker = AngelOne(api_key="AK", client_code="", password="1234", totp_key="KEY")
        with pytest.raises(AuthenticationError, match="Incomplete credentials"):
            broker.authenticate()

    def test_missing_password(self, tmp_session_file):
        broker = AngelOne(api_key="AK", client_code="C1", password="", totp_key="KEY")
        with pytest.raises(AuthenticationError, match="Incomplete credentials"):
            broker.authenticate()

    def test_missing_totp_key(self, tmp_session_file):
        broker = AngelOne(api_key="AK", client_code="C1", password="1234", totp_key="")
        with pytest.raises(AuthenticationError, match="Incomplete credentials"):
            broker.authenticate()

    def test_missing_creds_no_network_call(self, tmp_session_file, mocker):
        """When creds are incomplete, authenticate_broker must never be called."""
        spy = mocker.patch("india_stocks_api.brokers.angel.authenticate_broker")
        broker = AngelOne(api_key="", client_code="", password="", totp_key="")

        with pytest.raises(AuthenticationError):
            broker.authenticate()

        spy.assert_not_called()
