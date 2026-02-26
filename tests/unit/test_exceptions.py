"""Tests for the exception hierarchy in india_stocks_api.exceptions."""

import pytest

from india_stocks_api.exceptions import (
    AuthenticationError,
    BrokerError,
    ErrorCode,
    ISAError,
    NetworkError,
    RateLimitError,
    SessionExpiredError,
    ValidationError,
)


class TestExceptionHierarchy:
    def test_isa_error_is_exception(self):
        assert issubclass(ISAError, Exception)

    def test_authentication_error_is_isa_error(self):
        assert issubclass(AuthenticationError, ISAError)

    def test_session_expired_error_is_authentication_error(self):
        assert issubclass(SessionExpiredError, AuthenticationError)

    def test_network_error_is_isa_error(self):
        assert issubclass(NetworkError, ISAError)

    def test_rate_limit_error_is_isa_error(self):
        assert issubclass(RateLimitError, ISAError)

    def test_validation_error_is_isa_error(self):
        assert issubclass(ValidationError, ISAError)

    def test_broker_error_is_isa_error(self):
        assert issubclass(BrokerError, ISAError)

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
        assert str(err) == "test message [AUTH_FAILED]"


class TestErrorCodes:
    def test_default_error_code_is_unknown(self):
        err = ISAError("generic error")
        assert err.code == ErrorCode.UNKNOWN

    def test_authentication_error_default_code(self):
        err = AuthenticationError("auth failed")
        assert err.code == ErrorCode.AUTH_FAILED

    def test_session_expired_error_code(self):
        err = SessionExpiredError("session expired")
        assert err.code == ErrorCode.SESSION_EXPIRED

    def test_network_error_default_code(self):
        err = NetworkError("connection failed")
        assert err.code == ErrorCode.NETWORK_ERROR

    def test_rate_limit_error_code(self):
        err = RateLimitError("too many requests")
        assert err.code == ErrorCode.RATE_LIMITED

    def test_validation_error_default_code(self):
        err = ValidationError("invalid input")
        assert err.code == ErrorCode.VALIDATION_ERROR

    def test_broker_error_default_code(self):
        err = BrokerError("broker rejected")
        assert err.code == ErrorCode.BROKER_ERROR

    def test_custom_error_code(self):
        err = ISAError("timeout", code=ErrorCode.TIMEOUT)
        assert err.code == ErrorCode.TIMEOUT
        assert err.code.value == 2001

    def test_error_code_ranges(self):
        assert 1000 <= ErrorCode.AUTH_FAILED.value < 2000
        assert 2000 <= ErrorCode.NETWORK_ERROR.value < 3000
        assert 3000 <= ErrorCode.RATE_LIMITED.value < 4000
        assert 4000 <= ErrorCode.VALIDATION_ERROR.value < 5000
        assert 5000 <= ErrorCode.BROKER_ERROR.value < 6000
        assert 9000 <= ErrorCode.UNKNOWN.value < 10000

    def test_details_preserved(self):
        details = {"broker_code": "ABC123", "raw_response": {"status": "failed"}}
        err = BrokerError("order rejected", details=details)
        assert err.details == details
        assert err.details["broker_code"] == "ABC123"

    def test_details_default_none(self):
        err = ISAError("error")
        assert err.details is None

    def test_repr_includes_code_name(self):
        err = NetworkError("timeout", code=ErrorCode.TIMEOUT)
        assert "TIMEOUT" in repr(err)
        assert "timeout" in repr(err)

    def test_exception_chaining(self):
        original = ConnectionError("socket closed")
        try:
            raise NetworkError("connection lost") from original
        except NetworkError as e:
            assert e.__cause__ is original
