"""
india_stocks_api/brokers/base/errors.py

This module defines custom exception classes for handling specific error conditions in the package.

Custom exceptions are used to signal different types of errors that may occur during the operation of the library,
making it easier to handle and debug issues.

The following custom exceptions are defined:
- InputError: Raised when there is an error with the input provided.
- ResponseError: Raised when the response from a service is invalid or incorrect.
- TokenDownloadError: Raised when there is an issue downloading or handling a token.
- RequestTimeout: Raised when a request times out.
- NetworkError: Raised for general network-related errors.
- BrokerError: Raised for errors specific to the broker service.

Each exception class inherits from Python's built-in `Exception` class and can be extended or customized if needed.

Usage:
    To raise a specific exception, use the class name and provide a descriptive message if necessary.
    Example:
        raise InputError("Invalid input format.")
"""

__all__ = [
    "InputError",
    "ResponseError",
    "TokenDownloadError",
    "RequestTimeout",
    "NetworkError",
    "BrokerError",
]


class InputError(Exception):
    """
    Exception raised for errors with the input provided.
    """

    def __init__(self, message=None):
        super().__init__(message)
        self.message = message


class ResponseError(Exception):
    """
    Exception raised when the response from a service is invalid or incorrect.
    """

    def __init__(self, message=None):
        super().__init__(message)
        self.message = message


class TokenDownloadError(Exception):
    """
    Exception raised for issues related to downloading or handling a token.
    """

    def __init__(self, message=None):
        super().__init__(message)
        self.message = message


class RequestTimeout(Exception):
    """
    Exception raised when a request times out.
    """

    def __init__(self, message=None):
        super().__init__(message)
        self.message = message


class NetworkError(Exception):
    """
    Exception raised for general network-related errors.
    """

    def __init__(self, message=None):
        super().__init__(message)
        self.message = message


class BrokerError(Exception):
    """
    Exception raised for errors specific to the broker service.
    """

    def __init__(self, message=None):
        super().__init__(message)
        self.message = message
