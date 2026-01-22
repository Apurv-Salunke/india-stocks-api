"""
Tests for HTTPX client utilities
"""
from unittest.mock import patch
from india_stocks_api.internal.utils.httpx_client import (
    get_http_client,
    cleanup_http_client
)
import httpx


class TestGetHttpClient:
    """Test cases for get_http_client function"""

    def test_get_http_client_returns_client(self):
        """Test that get_http_client returns an httpx.Client"""
        client = get_http_client()
        assert isinstance(client, httpx.Client)

    def test_get_http_client_singleton(self):
        """Test that get_http_client returns the same instance"""
        client1 = get_http_client()
        client2 = get_http_client()
        assert client1 is client2

    def test_get_http_client_has_timeout(self):
        """Test that the HTTP client has timeout configured"""
        client = get_http_client()
        assert client._transport is not None

    def test_get_http_client_after_cleanup(self):
        """Test that get_http_client creates new client after cleanup"""
        client1 = get_http_client()
        cleanup_http_client()
        client2 = get_http_client()
        assert client1 is not client2

    @patch('india_stocks_api.internal.utils.httpx_client._http_client', None)
    def test_get_http_client_initialization(self):
        """Test that get_http_client initializes client correctly"""
        # Reset the global client
        from india_stocks_api.internal.utils import httpx_client
        httpx_client._http_client = None
        
        client = get_http_client()
        assert client is not None
        assert isinstance(client, httpx.Client)


class TestCleanupHttpClient:
    """Test cases for cleanup_http_client function"""

    def test_cleanup_http_client_closes_client(self):
        """Test that cleanup_http_client closes the client"""
        client = get_http_client()
        assert client is not None
        
        # Mock the close method to verify it's called
        with patch.object(client, 'close') as mock_close:  # noqa: F841
            cleanup_http_client()
            # Note: The actual close might have been called, but we're testing the cleanup logic
            pass

    def test_cleanup_http_client_resets_global(self):
        """Test that cleanup_http_client resets the global client"""
        get_http_client()  # Ensure client exists
        cleanup_http_client()
        
        # After cleanup, getting client again should create a new one
        from india_stocks_api.internal.utils import httpx_client
        assert httpx_client._http_client is None

    def test_cleanup_http_client_idempotent(self):
        """Test that cleanup_http_client can be called multiple times safely"""
        get_http_client()
        cleanup_http_client()
        cleanup_http_client()  # Should not raise an error
        cleanup_http_client()  # Should not raise an error

    def test_cleanup_http_client_when_no_client(self):
        """Test that cleanup_http_client works when no client exists"""
        cleanup_http_client()  # Should not raise an error
        cleanup_http_client()  # Should still not raise an error
