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
