"""
Database migration manager
"""

import sqlite3
import os
from pathlib import Path
from typing import List
import logging

logger = logging.getLogger(__name__)


class MigrationManager:
    """Handles database schema migrations"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.migrations_dir = Path(__file__).parent

    def get_current_version(self) -> int:
        """Get current database version"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT MAX(version) FROM schema_version")
                result = cursor.fetchone()
                return result[0] if result and result[0] is not None else 0
        except sqlite3.OperationalError:
            # Table doesn't exist yet
            return 0

    def get_pending_migrations(self) -> List[str]:
        """Get list of pending migration files"""
        current_version = self.get_current_version()
        migration_files = []

        for file_path in sorted(self.migrations_dir.glob("*.sql")):
            if file_path.name == "001_initial_schema.sql":
                continue  # Skip the initial schema file (handled by create_database)

            # Extract version number from filename
            try:
                version = int(file_path.stem.split("_")[0])
                if version > current_version:
                    migration_files.append(file_path.name)
            except (ValueError, IndexError):
                logger.warning(f"Invalid migration filename: {file_path.name}")

        return migration_files

    def run_migration(self, migration_file: str) -> bool:
        """Run a specific migration file"""
        migration_path = self.migrations_dir / migration_file

        if not migration_path.exists():
            logger.error(f"Migration file not found: {migration_file}")
            return False

        try:
            with sqlite3.connect(self.db_path) as conn:
                # Read and execute migration file
                with open(migration_path, "r") as f:
                    migration_sql = f.read()

                # Execute migration
                conn.executescript(migration_sql)
                conn.commit()

                logger.info(f"Successfully applied migration: {migration_file}")
                return True

        except sqlite3.Error as e:
            logger.error(f"Error applying migration {migration_file}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error applying migration {migration_file}: {e}")
            return False

    def run_all_migrations(self) -> bool:
        """Run all pending migrations"""
        pending_migrations = self.get_pending_migrations()

        if not pending_migrations:
            logger.info("No pending migrations found")
            return True

        logger.info(f"Found {len(pending_migrations)} pending migrations")

        for migration_file in pending_migrations:
            if not self.run_migration(migration_file):
                logger.error(f"Failed to apply migration: {migration_file}")
                return False

        logger.info("All migrations completed successfully")
        return True

    def create_database(self) -> bool:
        """Create database with initial schema"""
        try:
            # Create database directory if it doesn't exist
            db_dir = Path(self.db_path).parent
            db_dir.mkdir(parents=True, exist_ok=True)

            # Run initial migration
            return self.run_migration("001_initial_schema.sql")

        except Exception as e:
            logger.error(f"Error creating database: {e}")
            return False

    def reset_database(self) -> bool:
        """Reset database (drop all tables and recreate)"""
        try:
            # Remove existing database file
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
                logger.info(f"Removed existing database: {self.db_path}")

            # Create fresh database
            return self.create_database()

        except Exception as e:
            logger.error(f"Error resetting database: {e}")
            return False

    def get_database_info(self) -> dict:
        """Get database information"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
                tables = [row[0] for row in cursor.fetchall()]

                current_version = self.get_current_version()
                pending_migrations = self.get_pending_migrations()

                return {
                    "database_path": self.db_path,
                    "current_version": current_version,
                    "tables": tables,
                    "pending_migrations": len(pending_migrations),
                    "database_exists": os.path.exists(self.db_path),
                }
        except Exception as e:
            logger.error(f"Error getting database info: {e}")
            return {
                "database_path": self.db_path,
                "error": str(e),
                "database_exists": os.path.exists(self.db_path),
            }
