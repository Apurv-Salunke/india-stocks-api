"""
Production-grade tests for india_stocks_api.exceptions.

Focus:
- Public behavior
- Contract stability
- Inheritance guarantees
- Retry semantics
- Serialization correctness
- Performance sanity
"""

import time

import pytest

from india_stocks_api.exceptions import (
    AuthenticationError,
    BrokerError,
    DataError,
    DataIntegrityError,
    DownloadError,
    ErrorCode,
    ISAError,
    NetworkError,
    OrderError,
    ParsingError,
    RateLimitError,
    SchemaValidationError,
    SessionExpiredError,
    TimeoutError,
    ValidationError,
)

# ==========================================================
# ErrorCode Enum
# ==========================================================


class TestErrorCode:
    def test_enum_members_are_unique(self):
        values = [e.value for e in ErrorCode]
        assert len(values) == len(set(values)), "Duplicate ErrorCode values detected"

    def test_enum_is_hashable(self):
        error_set = {ErrorCode.AUTH_FAILED, ErrorCode.NETWORK_ERROR}
        assert ErrorCode.AUTH_FAILED in error_set

    def test_expected_ranges(self):
        # Spot-check critical public values (if part of contract)
        assert ErrorCode.AUTH_FAILED.value == 1000
        assert ErrorCode.NETWORK_ERROR.value == 2000
        assert ErrorCode.RATE_LIMITED.value == 3000
        assert ErrorCode.BROKER_ERROR.value == 5000
        assert ErrorCode.UNKNOWN.value == 9000


# ==========================================================
# ISAError Base Behavior
# ==========================================================


class TestISAError:
    def test_basic_creation(self):
        err = ISAError("failure")
        assert err.args[0] == "failure"
        assert err.code == ErrorCode.UNKNOWN
        assert err.details is None

    def test_str_format_with_code(self):
        err = ISAError("network issue", ErrorCode.NETWORK_ERROR)
        assert str(err) == "network issue [NETWORK_ERROR]"

    def test_str_format_unknown_code(self):
        err = ISAError("generic")
        assert str(err) == "generic"

    def test_details_preserved(self):
        details = {"endpoint": "/orders"}
        err = ISAError("error", ErrorCode.BROKER_ERROR, details)
        assert err.details == details

    def test_to_dict_serialization(self):
        details = {"id": 123}
        err = ISAError("failed", ErrorCode.BROKER_ERROR, details)
        payload = err.to_dict()

        assert payload["type"] == "ISAError"
        assert payload["message"] == "failed"
        assert payload["code"] == "BROKER_ERROR"
        assert payload["details"] == details

    def test_is_retryable_default_false(self):
        assert ISAError("x").is_retryable is False


# ==========================================================
# Authentication Errors
# ==========================================================


class TestAuthenticationErrors:
    def test_default_code(self):
        err = AuthenticationError("invalid")
        assert err.code == ErrorCode.AUTH_FAILED

    def test_session_expired_code(self):
        err = SessionExpiredError("expired")
        assert err.code == ErrorCode.SESSION_EXPIRED

    def test_not_retryable(self):
        assert AuthenticationError("x").is_retryable is False
        assert SessionExpiredError("x").is_retryable is False


# ==========================================================
# Network Errors
# ==========================================================


class TestNetworkErrors:
    def test_network_error_defaults(self):
        err = NetworkError("connection lost")
        assert err.code == ErrorCode.NETWORK_ERROR
        assert err.is_retryable is True

    def test_timeout_error(self):
        err = TimeoutError("timeout")
        assert err.code == ErrorCode.TIMEOUT
        assert err.is_retryable is True

    def test_download_error_inheritance(self):
        err = DownloadError("download failed")
        assert isinstance(err, NetworkError)
        assert err.code == ErrorCode.DOWNLOAD_FAILED
        assert err.is_retryable is True


# ==========================================================
# Rate Limiting
# ==========================================================


class TestRateLimitError:
    def test_retryable(self):
        err = RateLimitError("too many requests")
        assert err.code == ErrorCode.RATE_LIMITED
        assert err.is_retryable is True


# ==========================================================
# Validation Errors
# ==========================================================


class TestValidationErrors:
    def test_default_code(self):
        err = ValidationError("bad input")
        assert err.code == ErrorCode.VALIDATION_ERROR

    def test_not_retryable(self):
        assert ValidationError("x").is_retryable is False


# ==========================================================
# Broker + Order Errors
# ==========================================================


class TestBrokerErrors:
    def test_broker_error_default(self):
        err = BrokerError("broker failure")
        assert err.code == ErrorCode.BROKER_ERROR
        assert err.is_retryable is False

    def test_order_error_default(self):
        err = OrderError("order rejected")
        assert err.code == ErrorCode.ORDER_REJECTED
        assert isinstance(err, BrokerError)

    def test_order_error_custom_code(self):
        err = OrderError("not found", ErrorCode.ORDER_NOT_FOUND)
        assert err.code == ErrorCode.ORDER_NOT_FOUND


# ==========================================================
# Data Errors
# ==========================================================


class TestDataErrors:
    def test_data_error_default(self):
        err = DataError("data issue")
        assert err.code == ErrorCode.DATA_ERROR
        assert err.is_retryable is False

    def test_data_integrity_error(self):
        err = DataIntegrityError("duplicate")
        assert err.code == ErrorCode.DATA_INTEGRITY_ERROR

    def test_parsing_error(self):
        err = ParsingError("invalid json")
        assert err.code == ErrorCode.PARSING_ERROR

    def test_schema_validation_error(self):
        err = SchemaValidationError("schema mismatch")
        assert err.code == ErrorCode.SCHEMA_VALIDATION_ERROR

    def test_inheritance(self):
        assert issubclass(DataIntegrityError, DataError)
        assert issubclass(ParsingError, DataError)
        assert issubclass(SchemaValidationError, DataError)


# ==========================================================
# Inheritance Guarantees
# ==========================================================


class TestExceptionHierarchy:
    def test_all_inherit_from_isa(self):
        classes = [
            AuthenticationError,
            SessionExpiredError,
            NetworkError,
            TimeoutError,
            RateLimitError,
            ValidationError,
            BrokerError,
            OrderError,
            DataError,
            DataIntegrityError,
            ParsingError,
            SchemaValidationError,
            DownloadError,
        ]

        for cls in classes:
            assert issubclass(cls, ISAError)

    def test_catch_behavior(self):
        with pytest.raises(NetworkError):
            raise TimeoutError("timeout")

        with pytest.raises(BrokerError):
            raise OrderError("rejected")

        with pytest.raises(DataError):
            raise ParsingError("bad json")


# ==========================================================
# Retryable Contract
# ==========================================================


class TestRetryableContract:
    def test_retryable_types(self):
        retryable = [
            NetworkError("x"),
            TimeoutError("x"),
            RateLimitError("x"),
            DownloadError("x"),
        ]
        for err in retryable:
            assert err.is_retryable is True

    def test_non_retryable_types(self):
        non_retryable = [
            ISAError("x"),
            AuthenticationError("x"),
            ValidationError("x"),
            BrokerError("x"),
            OrderError("x"),
            DataError("x"),
        ]
        for err in non_retryable:
            assert err.is_retryable is False


# ==========================================================
# Performance Sanity
# ==========================================================


class TestPerformance:
    def test_exception_creation_speed(self):
        iterations = 10000
        start = time.perf_counter()

        for _ in range(iterations):
            err = OrderError("test", ErrorCode.ORDER_REJECTED)
            str(err)
            err.to_dict()

        elapsed = time.perf_counter() - start

        # Allow margin for CI variability
        assert elapsed < 2.0, f"Exception handling too slow: {elapsed:.2f}s"
