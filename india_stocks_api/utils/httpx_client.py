"""
HTTPX Client utilities for India Stocks API
"""

import httpx
from typing import Optional
from .logging import get_logger

# Global HTTP client
_http_client: Optional[httpx.Client] = None


def get_http_client() -> httpx.Client:
    """
    Get shared HTTP client with connection pooling

    Returns:
        Configured httpx.Client instance
    """
    global _http_client

    if _http_client is None:
        _http_client = httpx.Client(
            timeout=30.0,
            limits=httpx.Limits(
                max_keepalive_connections=20, max_connections=50, keepalive_expiry=120.0
            ),
            verify=True,
        )
        get_logger(__name__).info("Created HTTP client with connection pooling")

    return _http_client


def cleanup_http_client():
    """Cleanup HTTP client resources"""
    global _http_client

    if _http_client is not None:
        _http_client.close()
        _http_client = None
        get_logger(__name__).info("Closed HTTP client")
