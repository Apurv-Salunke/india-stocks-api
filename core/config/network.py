from requests.adapters import Retry

"""
The `RETRY_STRATEGY` is a configuration for the `requests` library's retry mechanism. It specifies the following retry behavior:

- Total number of retries: 3
- Backoff factor: 0.2 seconds
- Status codes that will trigger a retry: 408 (Request Timeout), 500 (Internal Server Error), 502 (Bad Gateway), 503 (Service Unavailable), 504 (Gateway Timeout)
- Allowed HTTP methods for retries: HEAD, GET, OPTIONS

This retry strategy is used when creating a new `requests.Session` object in the `_create_session` method of the `Broker` class.
"""
RETRY_STRATEGY = Retry(
    total=3,
    backoff_factor=0.2,
    status_forcelist=[403, 408, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
)

DEFAULT_TIMEOUT = 10
