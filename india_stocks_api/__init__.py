"""
Indian Stocks API
"""

__version__ = "2.0.0"

from .exceptions import AuthenticationError, ISAError, SessionExpiredError

__all__ = ["AuthenticationError", "ISAError", "SessionExpiredError"]
