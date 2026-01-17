"""
Unit tests for india_stocks_api.auth module.
Checks authentication flow and state management using mocked broker API.
"""

import pytest
import india_stocks_api.auth as auth

from india_stocks_api.auth import (
    authenticate_broker,
    is_authenticated,
    get_auth_headers,
    get_supported_brokers,
    get_missing_credentials,
)

BROKER_NAME = "angelone"


# ------------------------------------------------------------------
# GLOBAL FIXTURE: isolate file-based auth state (NON-NEGOTIABLE)
# ------------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolated_token_storage(tmp_path, monkeypatch):
    """
    Redirect TOKEN_STORAGE_PATH to a temp file so tests
    do NOT touch real auth_tokens.json
    """
    fake_token_file = tmp_path / "auth_tokens.json"
    monkeypatch.setattr(auth, "TOKEN_STORAGE_PATH", fake_token_file)
    yield


# ------------------------------------------------------------------
# BASIC / PURE FUNCTION TESTS
# ------------------------------------------------------------------

def test_supported_brokers_contains_angelone():
    brokers = get_supported_brokers()

    assert isinstance(brokers, list)
    assert "angelone" in brokers


def test_missing_credentials_returns_list():
    missing = get_missing_credentials(BROKER_NAME)
    assert isinstance(missing, list)


# ------------------------------------------------------------------
# AUTHENTICATION FLOW (MOCKED BROKER API)
# ------------------------------------------------------------------

def test_authenticate_broker_success(monkeypatch):
    # --- mock broker auth API ---
    def fake_authenticate(client_code, pin, totp):
        return "auth-token-123", "feed-token-123", None

    monkeypatch.setattr(
        "india_stocks_api.brokers.angelone.api.auth_api.authenticate_broker",
        fake_authenticate,
    )

    # --- mock environment variables ---
    monkeypatch.setenv("ANGELONE_CLIENT_CODE", "ABC123")
    monkeypatch.setenv("ANGELONE_PIN", "1234")
    monkeypatch.setenv("ANGELONE_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    monkeypatch.setenv("BROKER_API_KEY", "dummy")

    result = authenticate_broker(BROKER_NAME)

    assert result["success"] is True
    assert result["auth_token"] == "auth-token-123"
    assert result["broker_user_id"] == "ABC123"
    assert result["error"] is None


def test_authenticate_broker_failure_from_broker(monkeypatch):
    def fake_authenticate(client_code, pin, totp):
        return None, None, "invalid credentials"

    monkeypatch.setattr(
        "india_stocks_api.brokers.angelone.api.auth_api.authenticate_broker",
        fake_authenticate,
    )

    monkeypatch.setenv("ANGELONE_CLIENT_CODE", "ABC123")
    monkeypatch.setenv("ANGELONE_PIN", "1234")
    monkeypatch.setenv("ANGELONE_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    monkeypatch.setenv("BROKER_API_KEY", "dummy")

    result = authenticate_broker(BROKER_NAME)

    assert result["success"] is False
    assert result["auth_token"] is None
    assert result["error"] == "invalid credentials"


# ------------------------------------------------------------------
# AUTH STATE (FILE-BACKED REALITY)
# ------------------------------------------------------------------

def test_is_authenticated_false_when_no_token():
    assert is_authenticated(BROKER_NAME) is False


def test_is_authenticated_true_after_successful_auth(monkeypatch):
    def fake_authenticate(client_code, pin, totp):
        return "auth-token-123", None, None

    monkeypatch.setattr(
        "india_stocks_api.brokers.angelone.api.auth_api.authenticate_broker",
        fake_authenticate,
    )

    monkeypatch.setenv("ANGELONE_CLIENT_CODE", "ABC123")
    monkeypatch.setenv("ANGELONE_PIN", "1234")
    monkeypatch.setenv("ANGELONE_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    monkeypatch.setenv("BROKER_API_KEY", "dummy")

    authenticate_broker(BROKER_NAME)

    assert is_authenticated(BROKER_NAME) is True


# ------------------------------------------------------------------
# HEADER GENERATION
# ------------------------------------------------------------------

def test_get_auth_headers_after_auth(monkeypatch):
    def fake_authenticate(client_code, pin, totp):
        return "auth-token-123", None, None

    monkeypatch.setattr(
        "india_stocks_api.brokers.angelone.api.auth_api.authenticate_broker",
        fake_authenticate,
    )

    monkeypatch.setenv("ANGELONE_CLIENT_CODE", "ABC123")
    monkeypatch.setenv("ANGELONE_PIN", "1234")
    monkeypatch.setenv("ANGELONE_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    monkeypatch.setenv("BROKER_API_KEY", "dummy")

    authenticate_broker(BROKER_NAME)

    headers = get_auth_headers(BROKER_NAME)

    assert isinstance(headers, dict)
    assert "Authorization" in headers
    assert headers["Authorization"].startswith("Bearer ")
