"""India Stocks API - A Python library for trading Indian stocks.

This library provides a unified interface for trading stocks in India
through various brokers like Angel One, etc.
"""

__version__ = "1.0.0"
__author__ = "Apurv Salunke"
__license__ = "MIT"

import logging
from pathlib import Path

from india_stocks_api import brokers
from india_stocks_api import config
from india_stocks_api.utils.cache_utils import get_database_path
from india_stocks_api.database.migrations.migration_manager import MigrationManager

logger = logging.getLogger(__name__)


def _ensure_database_migrated():
    """Ensure database is created and migrated to latest version"""
    try:
        db_path = get_database_path()
        migration_manager = MigrationManager(db_path)

        # Create database if it doesn't exist
        if not Path(db_path).exists():
            logger.info("Creating database with initial schema...")
            migration_manager.create_database()
        else:
            # Run any pending migrations
            pending_migrations = migration_manager.get_pending_migrations()
            if pending_migrations:
                logger.info(f"Running {len(pending_migrations)} pending migrations...")
                migration_manager.run_all_migrations()

    except Exception as e:
        logger.error(f"Failed to ensure database migration: {e}")
        # Don't raise - let the package load even if migration fails
        # This allows users to handle migration issues manually


# Run database migration on package import
_ensure_database_migrated()

__all__ = [
    "brokers",
    "config",
]
