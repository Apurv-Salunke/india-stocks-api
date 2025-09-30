#!/usr/bin/env python3
"""
Test script to verify AngelOne broker integration with new database system
"""

import sys
import os
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import after path modification
from india_stocks_api.database import InstrumentService  # noqa: E402
from india_stocks_api.brokers.angelone import AngelOne  # noqa: E402

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_database_integration():
    """Test AngelOne broker integration with database"""
    logger.info("Testing AngelOne broker integration with new database system...")

    # Test database path (uses cache directory by default)
    from india_stocks_api.utils.cache_utils import get_database_path

    db_path = get_database_path()

    if not os.path.exists(db_path):
        logger.error(
            f"Database file {db_path} not found. Please run sync script first."
        )
        return False

    try:
        # Test 1: Initialize AngelOne broker
        logger.info("Test 1: Initializing AngelOne broker...")
        AngelOne()  # Test initialization
        logger.info("✓ AngelOne broker initialized successfully")

        # Test 2: Test equity instrument resolution
        logger.info("Test 2: Testing equity instrument resolution...")
        test_symbols = [
            ("RELIANCE", "NSE"),
            ("TCS", "NSE"),
            ("INFY", "NSE"),
            ("HDFCBANK", "BSE"),
        ]

        for symbol, exchange in test_symbols:
            try:
                result = AngelOne.resolve_equity_instrument(symbol, exchange, db_path)
                logger.info(
                    f"✓ {symbol} ({exchange}): Token={result['broker_token']}, Symbol={result['broker_symbol']}"
                )
            except KeyError as e:
                logger.warning(f"✗ {symbol} ({exchange}): {e}")

        # Test 3: Test F&O instrument resolution
        logger.info("Test 3: Testing F&O instrument resolution...")
        test_fno_symbols = [
            ("BANKNIFTY28OCT25FUT", "NSE"),  # BANKNIFTY future
            ("LTIM30SEP25FUT", "NSE"),  # LTIM future
        ]

        for symbol, exchange in test_fno_symbols:
            try:
                result = AngelOne.resolve_fno_instrument(symbol, exchange, db_path)
                logger.info(
                    f"✓ {symbol} ({exchange}): Token={result['broker_token']}, Symbol={result['broker_symbol']}"
                )
            except KeyError as e:
                logger.warning(f"✗ {symbol} ({exchange}): {e}")

        # Test 4: Test database service integration
        logger.info("Test 4: Testing database service integration...")
        service = InstrumentService(db_path)
        stats = service.get_database_stats()
        logger.info(
            f"✓ Database stats: {stats['total_instruments']} instruments, {stats['total_broker_instruments']} broker mappings"
        )

        logger.info("✓ All integration tests passed!")
        return True

    except Exception as e:
        logger.error(f"✗ Integration test failed: {e}")
        return False


def test_backward_compatibility():
    """Test backward compatibility with legacy system"""
    logger.info("Testing backward compatibility with legacy system...")

    try:
        # Test legacy token creation still works
        logger.info("Test: Legacy token creation...")
        eq_tokens = AngelOne.create_eq_tokens()
        logger.info(f"✓ Legacy equity tokens created: {len(eq_tokens)} exchanges")

        # Test legacy F&O token creation
        fno_tokens = AngelOne.create_fno_tokens()
        logger.info(f"✓ Legacy F&O tokens created: {len(fno_tokens)} exchanges")

        logger.info("✓ Backward compatibility tests passed!")
        return True

    except Exception as e:
        logger.error(f"✗ Backward compatibility test failed: {e}")
        return False


def main():
    """Main test function"""
    logger.info("Starting AngelOne broker integration tests...")

    # Test database integration
    db_test_passed = test_database_integration()

    # Test backward compatibility
    compat_test_passed = test_backward_compatibility()

    if db_test_passed and compat_test_passed:
        logger.info("🎉 All tests passed! AngelOne integration is working correctly.")
        return 0
    else:
        logger.error("❌ Some tests failed. Please check the logs above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
