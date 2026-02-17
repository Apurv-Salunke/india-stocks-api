"""Tests for authenticate_broker() in internal.angel.api.auth_api.

Uses ``respx`` to intercept httpx calls — no real network traffic.
"""

import httpx
import pytest
import respx

from india_stocks_api.internal.angel.api.auth_api import authenticate_broker

ANGEL_LOGIN_URL = "https://apiconnect.angelbroking.com/rest/auth/angelbroking/user/v1/loginByPassword"


@pytest.fixture(autouse=True)
def _patch_httpx_client(mocker):
    """Provide a respx-aware httpx.Client so mocked routes are always active."""
    router = respx.MockRouter()
    client = httpx.Client(transport=httpx.MockTransport(router.handler))
    mocker.patch(
        "india_stocks_api.internal.angel.api.auth_api.get_httpx_client",
        return_value=client,
    )
    yield router
    client.close()


class TestAuthenticateBrokerSuccess:
    def test_returns_tokens_on_success(self, _patch_httpx_client):
        router = _patch_httpx_client
        router.post(ANGEL_LOGIN_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "status": True,
                    "message": "SUCCESS",
                    "data": {
                        "jwtToken": "jwt_abc",
                        "refreshToken": "ref_abc",
                        "feedToken": "feed_abc",
                    },
                },
            )
        )

        jwt, feed, err = authenticate_broker("key", "C123", "1234", "999999")

        assert jwt == "jwt_abc"
        assert feed == "feed_abc"
        assert err is None


class TestAuthenticateBrokerFailure:
    def test_returns_error_message_on_api_error(self, _patch_httpx_client):
        router = _patch_httpx_client
        router.post(ANGEL_LOGIN_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "status": False,
                    "message": "Invalid totp",
                    "data": None,
                },
            )
        )

        jwt, feed, err = authenticate_broker("key", "C123", "1234", "000000")

        assert jwt is None
        assert feed is None
        assert err == "Invalid totp"

    def test_returns_error_on_network_failure(self, _patch_httpx_client):
        router = _patch_httpx_client
        router.post(ANGEL_LOGIN_URL).mock(side_effect=httpx.ConnectError("conn refused"))

        jwt, feed, err = authenticate_broker("key", "C123", "1234", "999999")

        assert jwt is None
        assert feed is None
        assert "conn refused" in err


class TestNoCredentialLeaks:
    def test_no_print_output(self, _patch_httpx_client, capsys):
        """Regression: authenticate_broker must not print credentials."""
        router = _patch_httpx_client
        router.post(ANGEL_LOGIN_URL).mock(
            return_value=httpx.Response(
                200,
                json={
                    "status": True,
                    "message": "SUCCESS",
                    "data": {"jwtToken": "jwt", "feedToken": "feed"},
                },
            )
        )

        authenticate_broker("secret_key", "C123", "secret_pin", "123456")

        captured = capsys.readouterr()
        assert "secret_key" not in captured.out
        assert "secret_pin" not in captured.out
        assert captured.out == ""
