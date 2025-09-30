#!/usr/bin/env python3
"""
Setup script for India Stocks API database
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
from india_stocks_api.database.migrations import MigrationManager  # noqa: E402

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def setup_database(db_path: str = "instruments.db"):
    """Setup database with initial schema and reference data"""

    logger.info(f"Setting up database: {db_path}")

    try:
        # Initialize migration manager
        migration_manager = MigrationManager(db_path)

        # Create database with schema
        logger.info("Creating database schema...")
        success = migration_manager.create_database()

        if not success:
            logger.error("Failed to create database")
            return False

        logger.info("Database schema created successfully")

        # Initialize instrument service
        logger.info("Initializing instrument service...")
        instrument_service = InstrumentService(db_path)

        # Get database statistics
        stats = instrument_service.get_database_stats()
        logger.info("Database statistics:")
        logger.info(f"  Total instruments: {stats.get('total_instruments', 0)}")
        logger.info(
            f"  Total broker instruments: {stats.get('total_broker_instruments', 0)}"
        )

        # Show instruments by category
        if stats.get("instruments_by_category"):
            logger.info("  Instruments by category:")
            for category, count in stats["instruments_by_category"].items():
                logger.info(f"    {category}: {count}")

        logger.info("Database setup completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        return False


def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description="Setup India Stocks API database")
    parser.add_argument(
        "--db-path",
        default="instruments.db",
        help="Database file path (default: instruments.db)",
    )
    parser.add_argument("--reset", action="store_true", help="Reset existing database")

    args = parser.parse_args()

    if args.reset and os.path.exists(args.db_path):
        logger.info(f"Resetting database: {args.db_path}")
        migration_manager = MigrationManager(args.db_path)
        migration_manager.reset_database()

    success = setup_database(args.db_path)

    if success:
        logger.info("Setup completed successfully!")
        sys.exit(0)
    else:
        logger.error("Setup failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
