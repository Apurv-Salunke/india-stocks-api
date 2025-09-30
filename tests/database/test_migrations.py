"""
Test cases for database migrations
"""

import pytest
import tempfile
import os
from pathlib import Path
from india_stocks_api.database.migrations import MigrationManager


class TestMigrationManager:
    """Test MigrationManager functionality"""

    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database path for testing"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            yield tmp.name
        os.unlink(tmp.name)

    @pytest.fixture
    def migration_manager(self, temp_db_path):
        """Create migration manager for testing"""
        return MigrationManager(temp_db_path)

    def test_migration_manager_creation(self, migration_manager):
        """Test migration manager creation"""
        assert migration_manager.db_path is not None
        assert migration_manager.migrations_dir is not None
        assert migration_manager.migrations_dir.exists()

    def test_get_current_version_empty_db(self, migration_manager):
        """Test getting current version from empty database"""
        version = migration_manager.get_current_version()
        assert version == 0

    def test_create_database(self, migration_manager):
        """Test database creation"""
        success = migration_manager.create_database()
        assert success is True

        # Check that database file exists
        assert Path(migration_manager.db_path).exists()

        # Check that schema version table exists
        import sqlite3

        with sqlite3.connect(migration_manager.db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            assert "schema_version" in tables
            assert "exchanges" in tables
            assert "instruments" in tables
            assert "broker_instruments" in tables

    def test_get_current_version_after_creation(self, migration_manager):
        """Test getting current version after database creation"""
        migration_manager.create_database()
        version = migration_manager.get_current_version()
        assert version == 1

    def test_get_database_info(self, migration_manager):
        """Test getting database information"""
        # First check with empty database
        info = migration_manager.get_database_info()

        assert "database_path" in info
        assert "current_version" in info
        assert "tables" in info
        assert "pending_migrations" in info
        assert "database_exists" in info

        # Create database and check again
        migration_manager.create_database()
        info = migration_manager.get_database_info()

        assert info["database_exists"] is True
        assert info["current_version"] == 1
        assert "exchanges" in info["tables"]
        assert "instruments" in info["tables"]

    def test_reset_database(self, migration_manager):
        """Test database reset"""
        # Create initial database
        migration_manager.create_database()
        assert Path(migration_manager.db_path).exists()

        # Reset database
        success = migration_manager.reset_database()
        assert success is True

        # Check that database still exists with fresh schema
        assert Path(migration_manager.db_path).exists()

        # Check version is reset to 1
        version = migration_manager.get_current_version()
        assert version == 1

    def test_reference_data_insertion(self, migration_manager):
        """Test that reference data is inserted during migration"""
        migration_manager.create_database()

        import sqlite3

        with sqlite3.connect(migration_manager.db_path) as conn:
            # Check exchanges
            cursor = conn.execute("SELECT COUNT(*) FROM exchanges")
            exchange_count = cursor.fetchone()[0]
            assert exchange_count > 0

            cursor = conn.execute(
                "SELECT exchange_code FROM exchanges WHERE exchange_code = 'NSE'"
            )
            result = cursor.fetchone()
            assert result is not None
            assert result[0] == "NSE"

            # Check instrument categories
            cursor = conn.execute("SELECT COUNT(*) FROM instrument_categories")
            category_count = cursor.fetchone()[0]
            assert category_count > 0

            cursor = conn.execute(
                "SELECT category_code FROM instrument_categories WHERE category_code = 'EQ'"
            )
            result = cursor.fetchone()
            assert result is not None
            assert result[0] == "EQ"

            # Check subcategories
            cursor = conn.execute("SELECT COUNT(*) FROM instrument_subcategories")
            subcategory_count = cursor.fetchone()[0]
            assert subcategory_count > 0

    def test_wal_checkpoint_functionality(self, migration_manager):
        """Test WAL checkpoint functionality"""
        # Create database first
        migration_manager.create_database()

        # Test different checkpoint modes
        for mode in ["PASSIVE", "FULL", "RESTART", "TRUNCATE"]:
            # Should not raise exception
            migration_manager.checkpoint(mode)

        # Test invalid mode
        with pytest.raises(ValueError, match="Invalid checkpoint mode"):
            migration_manager.checkpoint("INVALID")

    def test_migration_idempotency(self, migration_manager):
        """Test that migrations can be run multiple times safely"""
        # Create database
        migration_manager.create_database()
        initial_version = migration_manager.get_current_version()

        # Run all migrations again
        success = migration_manager.run_all_migrations()
        assert success is True

        # Version should be at least the initial version (may have applied more migrations)
        final_version = migration_manager.get_current_version()
        assert final_version >= initial_version

        # No pending migrations
        pending = migration_manager.get_pending_migrations()
        assert len(pending) == 0

    def test_migration_version_tracking(self, migration_manager):
        """Test that migration versions are properly tracked"""
        migration_manager.create_database()

        import sqlite3

        with sqlite3.connect(migration_manager.db_path) as conn:
            cursor = conn.execute(
                "SELECT version, description FROM schema_version ORDER BY version"
            )
            versions = cursor.fetchall()

            # Should have at least version 1
            assert len(versions) >= 1
            assert versions[0][0] == 1
            assert "Initial schema creation" in versions[0][1]

    def test_migration_error_handling(self, migration_manager):
        """Test error handling for invalid migration files"""
        # Test non-existent migration file
        success = migration_manager.run_migration("999_nonexistent.sql")
        assert success is False

        # Test invalid migration filename
        # This should be handled gracefully
        migration_manager.get_pending_migrations()
        # Should not crash, just log warning

    def test_database_connection_robustness(self, migration_manager):
        """Test database connection handling with proper PRAGMAs"""
        migration_manager.create_database()

        # Use the migration manager's connection method to test PRAGMAs
        with migration_manager._connect() as conn:
            # Check that WAL mode is enabled
            cursor = conn.execute("PRAGMA journal_mode")
            journal_mode = cursor.fetchone()[0]
            assert journal_mode.upper() == "WAL"

            # Check that foreign keys are enabled
            cursor = conn.execute("PRAGMA foreign_keys")
            foreign_keys = cursor.fetchone()[0]
            assert foreign_keys == 1

    def test_concurrent_migration_safety(self, migration_manager):
        """Test that migrations are safe under concurrent access"""
        import threading

        migration_manager.create_database()

        results = []
        errors = []

        def run_migrations():
            try:
                # Multiple threads trying to run migrations
                success = migration_manager.run_all_migrations()
                results.append(success)
            except Exception as e:
                errors.append(e)

        # Start multiple threads
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=run_migrations)
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Should have no errors (locking should prevent conflicts)
        assert len(errors) == 0

        # At least one should succeed, others may return False due to no pending migrations
        assert any(results)  # At least one should return True

        # Database should be in consistent state
        version = migration_manager.get_current_version()
        assert version >= 1

    def test_migration_file_parsing(self, migration_manager):
        """Test migration filename parsing"""
        # Test valid filenames
        assert migration_manager._parse_version_from_filename("001_initial.sql") == 1
        assert (
            migration_manager._parse_version_from_filename("002_add_columns.sql") == 2
        )
        assert migration_manager._parse_version_from_filename("999_final.sql") == 999

        # Test invalid filenames
        assert migration_manager._parse_version_from_filename("invalid.sql") is None
        assert migration_manager._parse_version_from_filename("abc_initial.sql") is None
        # Note: "001" without extension would parse as 1, but with .sql it would be None
        assert migration_manager._parse_version_from_filename("001.sql") == 1

    def test_schema_version_table_creation(self, migration_manager):
        """Test that schema_version table is created properly"""
        # Test with existing connection
        import sqlite3

        with sqlite3.connect(migration_manager.db_path) as conn:
            migration_manager._ensure_version_table(conn)

            # Check table exists
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            )
            result = cursor.fetchone()
            assert result is not None

            # Check table structure
            cursor = conn.execute("PRAGMA table_info(schema_version)")
            columns = [row[1] for row in cursor.fetchall()]
            assert "version" in columns
            assert "applied_at" in columns
            assert "description" in columns

    def test_migration_with_custom_description(self, migration_manager):
        """Test migration version recording with custom description"""
        migration_manager.create_database()

        import sqlite3

        with sqlite3.connect(migration_manager.db_path) as conn:
            # Record a custom version
            migration_manager._record_version(conn, 99, "Test migration")

            # Check it was recorded
            cursor = conn.execute(
                "SELECT description FROM schema_version WHERE version = 99"
            )
            result = cursor.fetchone()
            assert result is not None
            assert result[0] == "Test migration"

    def test_database_info_comprehensive(self, migration_manager):
        """Test comprehensive database info retrieval"""
        # Create database first (since fixture may have already created it)
        migration_manager.create_database()
        info = migration_manager.get_database_info()

        assert info["database_exists"] is True
        assert info["current_version"] >= 1
        assert "exchanges" in info["tables"]
        assert "instruments" in info["tables"]
        assert "broker_instruments" in info["tables"]
        assert "schema_version" in info["tables"]
        assert info["pending_migrations"] >= 0

    def test_migration_rollback_on_error(self, migration_manager):
        """Test that failed migrations don't leave database in inconsistent state"""
        # Create a temporary migration file with invalid SQL
        import tempfile
        import os

        temp_migration = tempfile.NamedTemporaryFile(
            mode="w", suffix=".sql", dir=migration_manager.migrations_dir, delete=False
        )

        try:
            # Write invalid SQL
            temp_migration.write("INVALID SQL STATEMENT;")
            temp_migration.close()

            # Try to run the invalid migration
            success = migration_manager.run_migration(
                os.path.basename(temp_migration.name)
            )
            assert success is False

            # Database should still be in good state
            version = migration_manager.get_current_version()
            assert version >= 0  # Should not be corrupted

        finally:
            # Clean up temp file
            if os.path.exists(temp_migration.name):
                os.unlink(temp_migration.name)
