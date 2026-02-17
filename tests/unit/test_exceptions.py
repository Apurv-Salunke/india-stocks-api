"""Tests for the exception hierarchy in india_stocks_api.exceptions."""
import pytest

from india_stocks_api.exceptions import ISAError, AuthenticationError, SessionExpiredError


class TestExceptionHierarchy:

    def test_isa_error_is_exception(self):
        assert issubclass(ISAError, Exception)

    def test_authentication_error_is_isa_error(self):
        assert issubclass(AuthenticationError, ISAError)

    def test_session_expired_error_is_authentication_error(self):
        assert issubclass(SessionExpiredError, AuthenticationError)

    def test_raise_isa_error(self):
        with pytest.raises(ISAError, match="boom"):
            raise ISAError("boom")

    def test_catch_authentication_error_as_isa_error(self):
        with pytest.raises(ISAError):
            raise AuthenticationError("bad creds")

    def test_catch_session_expired_as_authentication_error(self):
        with pytest.raises(AuthenticationError):
            raise SessionExpiredError("expired")

    def test_catch_session_expired_as_isa_error(self):
        with pytest.raises(ISAError):
            raise SessionExpiredError("expired")

    def test_exception_message_preserved(self):
        err = AuthenticationError("test message")
        assert str(err) == "test message"
