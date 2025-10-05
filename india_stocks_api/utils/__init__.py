"""
Utilities module for India Stocks API
Common utilities for broker operations and OpenAlgo compatibility
"""

from .logging import get_logger, setup_logging
from .httpx_client import get_http_client, cleanup_http_client
from .common import (
    ensure_directory_exists,
    format_currency,
    format_percentage,
    safe_divide,
)


__all__ = [
    # Logging utilities
    "get_logger",
    "setup_logging",
    # HTTP client utilities
    "get_http_client",
    "cleanup_http_client",
    # Common utilities
    "ensure_directory_exists",
    "format_currency",
    "format_percentage",
    "safe_divide",
]

__version__ = "1.0.0"
