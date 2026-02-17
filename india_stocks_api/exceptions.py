"""Custom exceptions for india-stocks-api."""


class ISAError(Exception):
    """Base exception for all india-stocks-api errors."""


class AuthenticationError(ISAError):
    """Raised when authentication fails or is missing."""


class SessionExpiredError(AuthenticationError):
    """Raised when the broker session has expired (past midnight IST)."""
