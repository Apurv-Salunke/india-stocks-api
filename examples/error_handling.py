"""
Error Handling: Proper exception handling for production use.

Demonstrates:
    - Catching authentication errors
    - Handling session expiry
    - Network error recovery
    - Validation errors
    - Rate limiting
    - Broker-specific errors

Expected output:
    === Authentication Error Demo ===
    Caught AuthenticationError: Invalid credentials

    === Session Expiry Demo ===
    Caught SessionExpiredError: Session expired (past midnight IST)

    === Validation Error Demo ===
    Caught ValidationError: Invalid instrument

    === Best Practices ===
    All operations completed with proper error handling
"""

import os
import sys
import time

from dotenv import load_dotenv

load_dotenv()


def main():
    from india_stocks_api.brokers import AngelOne
    from india_stocks_api.exceptions import (
        AuthenticationError,
        ISAError,
    )

    # Setup credentials
    api_key = os.getenv("ANGEL_API_KEY")
    client_code = os.getenv("ANGEL_CLIENT_ID")
    password = os.getenv("ANGEL_PIN")
    totp_key = os.getenv("ANGEL_TOTP_SECRET")

    # --- Example 1: Authentication Error ---
    print("=== Authentication Error Demo ===")
    try:
        # Using invalid credentials
        bad_broker = AngelOne(
            api_key="INVALID_KEY",
            client_code="INVALID_CODE",
            password="wrong",
            totp_key="wrong",
        )
        bad_broker.authenticate()
    except AuthenticationError as e:
        print(f"Caught AuthenticationError: {e}")
        print(f"  Error code: {e.code.name}")
        if e.details:
            print(f"  Details: {e.details}")
    except ISAError as e:
        print(f"Caught ISAError: {e}")
    print()

    # --- Example 2: Proper Broker Setup with Error Handling ---
    print("=== Proper Authentication ===")
    if not all([api_key, client_code, password, totp_key]):
        print("Skipping (no valid credentials in .env)")
        print()
    else:
        try:
            broker = AngelOne(
                api_key=api_key,
                client_code=client_code,
                password=password,
                totp_key=totp_key,
            )
            broker.authenticate()
            print("Authentication successful")
        except AuthenticationError as e:
            print(f"Authentication failed: {e}")
            sys.exit(1)
        print()

        # --- Example 3: API Call Error Handling ---
        print("=== API Call with Error Handling ===")
        demonstrate_api_errors(broker)


def demonstrate_api_errors(broker):
    """Show error handling patterns for various API calls."""
    from india_stocks_api.exceptions import (
        BrokerError,
        NetworkError,
        SessionExpiredError,
    )
    from india_stocks_api.instruments import Equity

    # Pattern 1: Simple try-except
    print("--- Pattern 1: Simple Error Handling ---")
    try:
        quote = broker.get_quote(Equity("RELIANCE"))
        print(f"RELIANCE LTP: {quote.ltp}")
    except SessionExpiredError:
        # Re-authenticate and retry
        broker.authenticate()
        quote = broker.get_quote(Equity("RELIANCE"))
        print(f"RELIANCE LTP: {quote.ltp} (after re-auth)")
    except NetworkError as e:
        print(f"Network error, will retry: {e}")
    except BrokerError as e:
        print(f"Broker returned error: {e}")
    print()

    # Pattern 2: Retry logic with backoff
    print("--- Pattern 2: Retry with Backoff ---")
    result = retry_with_backoff(
        lambda: broker.get_quote(Equity("SBIN")),
        max_retries=3,
    )
    if result:
        print(f"SBIN LTP: {result.ltp}")
    print()

    # Pattern 3: Comprehensive error handling
    print("--- Pattern 3: Comprehensive Handling ---")
    safe_get_quote(broker, Equity("TCS"))


def retry_with_backoff(func, max_retries=3, base_delay=1.0):
    """
    Execute a function with exponential backoff on failure.

    Args:
        func: Callable to execute
        max_retries: Maximum retry attempts
        base_delay: Initial delay in seconds (doubles each retry)

    Returns:
        Function result or None on failure
    """
    from india_stocks_api.exceptions import NetworkError, RateLimitError

    last_error = None
    delay = base_delay

    for attempt in range(max_retries):
        try:
            return func()
        except RateLimitError as e:
            last_error = e
            print(f"Rate limited, waiting {delay}s...")
            time.sleep(delay)
            delay *= 2
        except NetworkError as e:
            last_error = e
            print(f"Network error (attempt {attempt + 1}/{max_retries}), retrying...")
            time.sleep(delay)
            delay *= 2

    print(f"All retries failed: {last_error}")
    return None


def safe_get_quote(broker, instrument):
    """
    Safely fetch a quote with comprehensive error handling.

    This is the recommended pattern for production code.
    """
    from india_stocks_api.exceptions import (
        AuthenticationError,
        BrokerError,
        ISAError,
        NetworkError,
        RateLimitError,
        SessionExpiredError,
        ValidationError,
    )

    try:
        quote = broker.get_quote(instrument)
        print(f"{instrument.symbol} LTP: {quote.ltp}")
        return quote

    except SessionExpiredError:
        # Session expired - need to re-authenticate
        print("Session expired, re-authenticating...")
        try:
            broker.authenticate()
            return broker.get_quote(instrument)
        except AuthenticationError as e:
            print(f"Re-authentication failed: {e}")
            return None

    except RateLimitError as e:
        # Too many requests - back off
        print(f"Rate limited: {e}")
        print("Consider implementing request throttling")
        return None

    except NetworkError as e:
        # Network issues - may be temporary
        print(f"Network error: {e}")
        print("Check internet connection or try again later")
        return None

    except ValidationError as e:
        # Invalid input - don't retry
        print(f"Invalid request: {e}")
        return None

    except BrokerError as e:
        # Broker-side error
        print(f"Broker error: {e}")
        print(f"Error code: {e.code.name}")
        return None

    except ISAError as e:
        # Catch-all for SDK errors
        print(f"Unexpected SDK error: {e}")
        return None


def demonstrate_error_codes():
    """Show how to use error codes for programmatic handling."""
    from india_stocks_api.exceptions import ErrorCode

    # Error codes allow programmatic handling
    _error_handlers = {
        ErrorCode.AUTH_FAILED: lambda: "Check credentials",
        ErrorCode.SESSION_EXPIRED: lambda: "Re-authenticate",
        ErrorCode.RATE_LIMITED: lambda: "Wait and retry",
        ErrorCode.INSUFFICIENT_FUNDS: lambda: "Add funds",
        ErrorCode.ORDER_REJECTED: lambda: "Check order parameters",
    }

    # Example usage:
    # try:
    #     broker.place_order(...)
    # except ISAError as e:
    #     handler = _error_handlers.get(e.code)
    #     if handler:
    #         action = handler()
    #         print(f"Recommended action: {action}")


if __name__ == "__main__":
    main()
