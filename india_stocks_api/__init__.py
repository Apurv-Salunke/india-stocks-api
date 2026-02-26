"""
Indian Stocks API
"""

__version__ = "2.0.0"

from .exceptions import (
    AuthenticationError,
    BrokerError,
    ErrorCode,
    ISAError,
    NetworkError,
    RateLimitError,
    SessionExpiredError,
    ValidationError,
)

__all__ = [
    "AuthenticationError",
    "BrokerError",
    "ErrorCode",
    "ISAError",
    "NetworkError",
    "RateLimitError",
    "SessionExpiredError",
    "ValidationError",
]
