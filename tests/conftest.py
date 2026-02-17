"""Shared fixtures and marker auto-application for the test suite."""
import json
import pytest
from pathlib import Path
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Exclude tests/scripts/ from collection (old ad-hoc scripts)
# ---------------------------------------------------------------------------

collect_ignore_glob = ["scripts/*"]


# ---------------------------------------------------------------------------
# Auto-apply the 'integration' marker to every test under tests/integration/
# ---------------------------------------------------------------------------

def pytest_collection_modifyitems(config, items):
    for item in items:
        if "/integration/" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def dummy_creds():
    """Minimal credential dict for constructing AngelOne without real secrets."""
    return {
        "api_key": "test_api_key_abc",
        "client_code": "T99999",
        "password": "secret_pin_xyz",
        "totp_key": "JBSWY3DPEHPK3PXP",  # well-known test base32 secret
    }


@pytest.fixture()
def tmp_session_file(tmp_path):
    """
    Redirect session persistence to a temp directory.

    Patches the module-level ``_SESSION_FILE`` in ``context`` so that every
    call to ``save_session`` / ``load_session`` / ``clear_session`` during
    the test writes to an isolated file.  Also resets the in-memory cache
    so tests don't leak state into one another.
    """
    fake_path = tmp_path / "_cache" / "sessions.json"
    with patch("india_stocks_api.internal.context._SESSION_FILE", fake_path), \
         patch("india_stocks_api.internal.context._SESSION_LOADED", False), \
         patch("india_stocks_api.internal.context._SESSION_CACHE", {}):
        yield fake_path


@pytest.fixture()
def mock_auth_success(mocker):
    """Patch ``authenticate_broker`` to return a successful response."""
    return mocker.patch(
        "india_stocks_api.brokers.angel.authenticate_broker",
        return_value=("jwt_token_xxx", "feed_token_xxx", None),
    )


@pytest.fixture()
def mock_auth_failure(mocker):
    """Patch ``authenticate_broker`` to return a failure response."""
    return mocker.patch(
        "india_stocks_api.brokers.angel.authenticate_broker",
        return_value=(None, None, "Invalid TOTP"),
    )


@pytest.fixture(autouse=True)
def _reset_context_globals():
    """
    Reset the in-memory config between tests so token state doesn't leak.
    """
    from india_stocks_api.internal import context
    original_config = dict(context._CONFIG)
    yield
    context._CONFIG.update(original_config)
    context._CONFIG["access_token"] = None
    context._CONFIG["feed_token"] = None
