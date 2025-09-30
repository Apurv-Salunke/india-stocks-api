#!/usr/bin/env python3
"""
Script to sync AngelOne data to the instrument database
"""

import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from india_stocks_api.database import InstrumentService, AngelOneTokensManager  # noqa: E402

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def sync_angelone_data(db_path: str = "instruments.db", instrument_types: list = None):
    """Sync AngelOne data to the instrument database"""

    if instrument_types is None:
        instrument_types = ["equity", "fno", "commodity", "currency"]

    logger.info(f"Starting AngelOne data sync to database: {db_path}")
    logger.info(f"Instrument types to sync: {instrument_types}")

    try:
        # Initialize instrument service (uses cache directory by default)
        logger.info("Initializing instrument service...")
        if db_path == "instruments.db":
            # Use default cache directory if default path is specified
            instrument_service = InstrumentService()
        else:
            instrument_service = InstrumentService(db_path)

        # Initialize AngelOne provider with more threads for F&O processing
        logger.info("Initializing AngelOne provider...")
        provider = AngelOneTokensManager(instrument_service, "angelone", max_workers=16)

        # Get initial database stats
        initial_stats = instrument_service.get_database_stats()
        logger.info("Initial database statistics:")
        logger.info(f"  Total instruments: {initial_stats.get('total_instruments', 0)}")
        logger.info(
            f"  Total broker instruments: {initial_stats.get('total_broker_instruments', 0)}"
        )

        # Sync data based on requested types
        results = {}

        if "equity" in instrument_types:
            logger.info("Syncing equity instruments...")
            results["equity"] = provider.sync_equity_instruments()
            logger.info(f"Synced {results['equity']} equity instruments")

        if "fno" in instrument_types:
            logger.info("Syncing F&O instruments...")
            results["fno"] = provider.sync_fno_instruments()
            logger.info(f"Synced {results['fno']} F&O instruments")

        if "commodity" in instrument_types:
            logger.info("Syncing commodity instruments...")
            results["commodity"] = provider.sync_commodity_instruments()
            logger.info(f"Synced {results['commodity']} commodity instruments")

        if "currency" in instrument_types:
            logger.info("Syncing currency instruments...")
            results["currency"] = provider.sync_currency_instruments()
            logger.info(f"Synced {results['currency']} currency instruments")

        # Get final database stats
        final_stats = instrument_service.get_database_stats()
        logger.info("Final database statistics:")
        logger.info(f"  Total instruments: {final_stats.get('total_instruments', 0)}")
        logger.info(
            f"  Total broker instruments: {final_stats.get('total_broker_instruments', 0)}"
        )

        # Show instruments by category
        if final_stats.get("instruments_by_category"):
            logger.info("  Instruments by category:")
            for category, count in final_stats["instruments_by_category"].items():
                logger.info(f"    {category}: {count}")

        # Show broker instruments
        if final_stats.get("broker_instruments"):
            logger.info("  Broker instruments:")
            for broker, count in final_stats["broker_instruments"].items():
                logger.info(f"    {broker}: {count}")

        logger.info("AngelOne data sync completed successfully!")
        logger.info(f"Sync results: {results}")
        return True

    except Exception as e:
        logger.error(f"Error during AngelOne data sync: {e}")
        return False


def test_angelone_provider(db_path: str = "test_angelone.db"):
    """Test AngelOne provider with a small sample"""

    logger.info("Testing AngelOne provider...")

    try:
        # Initialize services
        instrument_service = InstrumentService(db_path)
        provider = AngelOneTokensManager(instrument_service, "angelone")

        # Test fetching data (without storing)
        logger.info("Testing data fetching...")

        equity_data = provider.fetch_equity_data()
        logger.info(f"Fetched {len(equity_data)} equity instruments")

        if equity_data:
            sample = equity_data[0]
            logger.info(
                f"Sample equity instrument: {sample['standardized_symbol']} - {sample['instrument_name']}"
            )

        fno_data = provider.fetch_fno_data()
        logger.info(f"Fetched {len(fno_data)} F&O instruments")

        commodity_data = provider.fetch_commodity_data()
        logger.info(f"Fetched {len(commodity_data)} commodity instruments")

        currency_data = provider.fetch_currency_data()
        logger.info(f"Fetched {len(currency_data)} currency instruments")

        logger.info("AngelOne provider test completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Error testing AngelOne provider: {e}")
        return False
    finally:
        # Clean up test database
        if Path(db_path).exists():
            Path(db_path).unlink()
            logger.info(f"Cleaned up test database: {db_path}")


def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Sync AngelOne data to instrument database"
    )
    parser.add_argument(
        "--db-path",
        default="instruments.db",
        help="Database file path (default: instruments.db)",
    )
    parser.add_argument(
        "--test", action="store_true", help="Run provider test instead of full sync"
    )
    parser.add_argument(
        "--types",
        nargs="+",
        choices=["equity", "fno", "commodity", "currency"],
        default=["equity", "fno", "commodity", "currency"],
        help="Instrument types to sync (default: all)",
    )

    args = parser.parse_args()

    if args.test:
        success = test_angelone_provider()
    else:
        success = sync_angelone_data(args.db_path, args.types)

    if success:
        logger.info("Operation completed successfully!")
        sys.exit(0)
    else:
        logger.error("Operation failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
