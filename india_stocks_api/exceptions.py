"""Custom exceptions for india-stocks-api."""

from enum import Enum
from typing import Any


class ErrorCode(Enum):
    """Standardized broker-agnostic error codes."""

    # 1xxx - Authentication
    AUTH_FAILED = 1000
    SESSION_EXPIRED = 1001
    INVALID_CREDENTIALS = 1002
    TOKEN_INVALID = 1003

    # 2xxx - Network/Transport
    NETWORK_ERROR = 2000
    TIMEOUT = 2001
    CONNECTION_REFUSED = 2002
    CONNECTION_RESET = 2003
    DOWNLOAD_FAILED = 2004

    # 3xxx - Rate limiting
    RATE_LIMITED = 3000

    # 4xxx - Invalid request
    INVALID_REQUEST = 4000
    VALIDATION_ERROR = 4001
    INVALID_INSTRUMENT = 4002
    INVALID_ORDER = 4003
    INSUFFICIENT_FUNDS = 4004
    INSUFFICIENT_QUANTITY = 4005

    # 5xxx - Broker service
    BROKER_ERROR = 5000
    SERVICE_UNAVAILABLE = 5001
    ORDER_REJECTED = 5002
    ORDER_NOT_FOUND = 5003
    POSITION_NOT_FOUND = 5004

    # 6xxx - Data integrity / processing
    DATA_ERROR = 6000
    DATA_INTEGRITY_ERROR = 6001
    PARSING_ERROR = 6002
    SCHEMA_VALIDATION_ERROR = 6003
    CHECKSUM_MISMATCH = 6004
    ATOMIC_OPERATION_FAILED = 6005

    # 9xxx - Internal
    UNKNOWN = 9000
    INTERNAL_ERROR = 9001


# ---------------------------------------------------------------------------
# Base Exception
# ---------------------------------------------------------------------------


class ISAError(Exception):
    """Base exception for all india-stocks-api errors."""

    __slots__ = ("code", "details")

    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.UNKNOWN,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.args[0]!r}, code={self.code.name}, details={self.details!r})"

    def __str__(self) -> str:
        base = self.args[0] if self.args else ""
        if self.code != ErrorCode.UNKNOWN:
            base = f"{base} [{self.code.name}]"
        return base

    @property
    def is_retryable(self) -> bool:
        """Whether retrying this operation may succeed."""
        return False

    def to_dict(self) -> dict[str, Any]:
        """Serialize for logging or telemetry."""
        return {
            "type": self.__class__.__name__,
            "message": self.args[0] if self.args else "",
            "code": self.code.name,
            "details": self.details,
        }


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


class AuthenticationError(ISAError):
    __slots__ = ()

    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.AUTH_FAILED,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, details)


class SessionExpiredError(AuthenticationError):
    __slots__ = ()

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, ErrorCode.SESSION_EXPIRED, details)


# ---------------------------------------------------------------------------
# Network / Rate Limit
# ---------------------------------------------------------------------------


class NetworkError(ISAError):
    __slots__ = ()

    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.NETWORK_ERROR,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, details)

    @property
    def is_retryable(self) -> bool:
        return True


class TimeoutError(NetworkError):
    __slots__ = ()

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, ErrorCode.TIMEOUT, details)


class RateLimitError(ISAError):
    __slots__ = ()

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, ErrorCode.RATE_LIMITED, details)

    @property
    def is_retryable(self) -> bool:
        return True


# ---------------------------------------------------------------------------
# Request Validation
# ---------------------------------------------------------------------------


class ValidationError(ISAError):
    __slots__ = ()

    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.VALIDATION_ERROR,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, details)


# ---------------------------------------------------------------------------
# Broker Errors
# ---------------------------------------------------------------------------


class BrokerError(ISAError):
    __slots__ = ()

    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.BROKER_ERROR,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, details)


class OrderError(BrokerError):
    __slots__ = ()

    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.ORDER_REJECTED,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, details)


# ---------------------------------------------------------------------------
# Data Layer
# ---------------------------------------------------------------------------


class DataError(ISAError):
    """Base class for all data processing errors."""

    __slots__ = ()

    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.DATA_ERROR,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, details)


class DataIntegrityError(DataError):
    __slots__ = ()

    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.DATA_INTEGRITY_ERROR,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code, details)


class ParsingError(DataError):
    __slots__ = ()

    def __init__(
        self,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, ErrorCode.PARSING_ERROR, details)


class SchemaValidationError(DataError):
    __slots__ = ()

    def __init__(
        self,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, ErrorCode.SCHEMA_VALIDATION_ERROR, details)


class DownloadError(NetworkError):
    __slots__ = ()

    def __init__(
        self,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message, ErrorCode.DOWNLOAD_FAILED, details)
