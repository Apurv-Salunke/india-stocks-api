"""
Database migration manager
"""

import sqlite3
import os
from pathlib import Path
from typing import List, Optional
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class MigrationManager:
    """Handles database schema migrations"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.migrations_dir = Path(__file__).parent

    @contextmanager
    def _connect(self):
        """SQLite connection with safe PRAGMAs and busy timeout."""
        conn = sqlite3.connect(self.db_path, timeout=10)
        try:
            # Reasonable defaults for robustness
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA busy_timeout = 5000")
            yield conn
        finally:
            conn.close()

    def _ensure_version_table(self, conn: Optional[sqlite3.Connection] = None) -> None:
        """Ensure schema_version table exists before querying versions.

        If a connection is provided, use it (and do not commit here).
        Otherwise, create a short-lived connection and commit.
        """
        try:
            if conn is None:
                with self._connect() as _conn:
                    _conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS schema_version (
                            version INTEGER PRIMARY KEY,
                            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            description TEXT
                        )
                        """
                    )
                    _conn.commit()
            else:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schema_version (
                        version INTEGER PRIMARY KEY,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        description TEXT
                    )
                    """
                )
        except sqlite3.Error as e:
            logger.error(f"Error ensuring schema_version table: {e}")
            raise

    def get_current_version(self) -> int:
        """Get current database version"""
        try:
            self._ensure_version_table()
            with self._connect() as conn:
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

    def _record_version(
        self, conn: sqlite3.Connection, version: int, description: Optional[str] = None
    ) -> None:
        """Record a migration version if not already recorded."""
        desc = description or "Applied via MigrationManager"
        conn.execute(
            "INSERT OR IGNORE INTO schema_version (version, description) VALUES (?, ?)",
            (version, desc),
        )

    def _parse_version_from_filename(self, migration_file: str) -> Optional[int]:
        try:
            return int(Path(migration_file).stem.split("_")[0])
        except (ValueError, IndexError):
            return None

    @contextmanager
    def _acquire_lock(self):
        """Simple process-level lock using an IMMEDIATE transaction.

        SQLite allows BEGIN IMMEDIATE to acquire a reserved lock preventing
        concurrent writers. This reduces migration race risk across processes.
        """
        with self._connect() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise

    def run_migration(self, migration_file: str) -> bool:
        """Run a specific migration file"""
        migration_path = self.migrations_dir / migration_file

        if not migration_path.exists():
            logger.error(f"Migration file not found: {migration_file}")
            return False

        try:
            with self._acquire_lock() as conn:
                # Ensure version table exists inside the locked transaction using same connection
                self._ensure_version_table(conn)

                # Read and execute migration file
                with open(migration_path, "r") as f:
                    migration_sql = f.read()

                # Execute migration within the same transaction
                conn.executescript(migration_sql)

                # Ensure version is recorded even if SQL file didn't insert it
                version = self._parse_version_from_filename(migration_file)
                if version is not None:
                    self._record_version(
                        conn, version, description=f"Applied {migration_file}"
                    )

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
        # Best-effort WAL checkpoint to truncate -wal/-shm after migrations
        try:
            self.checkpoint("TRUNCATE")
        except Exception as e:
            logger.warning(f"WAL checkpoint after migrations failed: {e}")
        return True

    def create_database(self) -> bool:
        """Create database with initial schema"""
        try:
            # Create database directory if it doesn't exist
            db_dir = Path(self.db_path).parent
            db_dir.mkdir(parents=True, exist_ok=True)

            # Run initial migration
            ok = self.run_migration("001_initial_schema.sql")
            if ok:
                # Best-effort checkpoint after initial creation
                try:
                    self.checkpoint("TRUNCATE")
                except Exception as e:
                    logger.warning(f"WAL checkpoint after create failed: {e}")
            return ok

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
            with self._connect() as conn:
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

    def checkpoint(self, mode: str = "TRUNCATE") -> None:
        """Run a WAL checkpoint. mode can be PASSIVE, FULL, RESTART, or TRUNCATE."""
        mode = (mode or "TRUNCATE").upper()
        if mode not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
            raise ValueError("Invalid checkpoint mode")
        with self._connect() as conn:
            conn.execute(f"PRAGMA wal_checkpoint({mode})")
            conn.commit()
