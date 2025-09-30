"""
Instrument service for database operations
"""

import sqlite3
from typing import List, Optional, Dict, Any
from pathlib import Path
import logging

from ..models.enums import Exchange, InstrumentCategory
from ...utils.cache_utils import get_database_path
from ..migrations import MigrationManager

logger = logging.getLogger(__name__)


class InstrumentService:
    """Main service for instrument database operations"""

    def __init__(self, db_path: Optional[str] = None):
        # Use default cache database path if not specified
        if db_path is None:
            db_path = get_database_path()

        self.db_path = Path(db_path)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Initialize database
        self._init_database()

    def _init_database(self):
        """Initialize database connection and verify schema"""
        try:
            # Create and migrate database if it doesn't exist (supports custom paths)
            if not self.db_path.exists():
                self.logger.info("Database not found, creating with initial schema...")
                migration_manager = MigrationManager(str(self.db_path))
                if not migration_manager.create_database():
                    raise RuntimeError("Failed to create database schema")
                migration_manager.run_all_migrations()

            # Verify database has required tables; if not, create/migrate and verify again
            try:
                self._verify_database_schema()
            except RuntimeError:
                self.logger.info(
                    "Database schema missing, applying initial schema and migrations..."
                )
                migration_manager = MigrationManager(str(self.db_path))
                migration_manager.create_database()
                migration_manager.run_all_migrations()
                # Verify again after applying schema
                self._verify_database_schema()

            # Auto-populate with AngelOne data if database is empty
            if self._is_database_empty():
                self.logger.info(
                    "Database is empty, auto-populating with AngelOne data..."
                )
                self._auto_populate_data()

        except Exception as e:
            self.logger.error(f"Error initializing database: {e}")
            raise

    def _verify_database_schema(self):
        """Verify database has required tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
                tables = [row[0] for row in cursor.fetchall()]

                required_tables = [
                    "exchanges",
                    "instruments",
                    "broker_instruments",
                    "schema_version",
                ]
                missing_tables = [
                    table for table in required_tables if table not in tables
                ]

                if missing_tables:
                    raise RuntimeError(
                        f"Database schema incomplete. Missing tables: {missing_tables}. "
                        "Please ensure migrations have been applied."
                    )

        except sqlite3.Error as e:
            raise RuntimeError(f"Error verifying database schema: {e}")

    def _is_database_empty(self) -> bool:
        """Check if database is empty (no instruments)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM instruments")
                count = cursor.fetchone()[0]
                return count == 0
        except sqlite3.Error:
            return True  # Assume empty if we can't check

    def _auto_populate_data(self):
        """Auto-populate database with AngelOne data on first creation"""
        try:
            print("🚀 Auto-populating database with AngelOne instrument data...")
            print("⏳ This may take a few minutes for the first time...")

            from ..providers.angelone_provider import AngelOneTokensManager

            # Initialize AngelOne provider with this service instance
            provider = AngelOneTokensManager(symbol_db=self, max_workers=8)

            # Fetch and store equity data (most commonly used)
            equity_data = provider.fetch_equity_data()
            if equity_data:
                stored_count = provider._store_equity_data(equity_data)
                print(f"✅ Auto-populated {stored_count:,} equity instruments")

            # Optionally fetch F&O data (can be slow, so make it optional)
            try:
                fno_data = provider.fetch_fno_data()
                if fno_data:
                    stored_count = provider._store_fno_data(fno_data)
                    print(f"✅ Auto-populated {stored_count:,} F&O instruments")
            except Exception as e:
                print(f"⚠️  F&O data population skipped: {e}")
                print("💡 You can manually sync F&O data later if needed")

            print("🎉 Database auto-population completed!")

        except Exception as e:
            print(f"⚠️  Auto-population failed: {e}")
            print("💡 You can manually sync data later using sync scripts")
            # Don't raise the error - let the database work without data

    def resolve_instrument(
        self,
        standardized_symbol: str,
        broker_name: str,
        exchange: Exchange,
        category: InstrumentCategory,
        expiry_date: Optional[str] = None,
        strike_price: Optional[float] = None,
        option_type: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Resolve standardized symbol to broker-specific format"""

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            # Build query based on instrument type
            query = """
                SELECT i.standardized_symbol, i.instrument_name, i.sector,
                       bi.broker_symbol, bi.broker_token, bi.broker_instrument_key,
                       bi.tick_size, bi.lot_size, bi.margin_percentage,
                       bi.expiry_date, bi.strike_price, bi.option_type,
                       e.exchange_code, ic.category_code
                FROM instruments i
                JOIN broker_instruments bi ON i.id = bi.instrument_id
                JOIN exchanges e ON i.exchange_id = e.id
                JOIN instrument_categories ic ON i.category_id = ic.id
                WHERE i.standardized_symbol = ?
                AND bi.broker_name = ?
                AND e.exchange_code = ?
                AND ic.category_code = ?
                AND bi.is_tradeable = 1
            """

            params = [standardized_symbol, broker_name, exchange.value, category.value]

            # Add derivative-specific filters
            if expiry_date:
                query += " AND bi.expiry_date = ?"
                params.append(expiry_date)

            if strike_price:
                query += " AND bi.strike_price = ?"
                params.append(strike_price)

            if option_type:
                query += " AND bi.option_type = ?"
                params.append(option_type)

            cursor = conn.execute(query, params)
            row = cursor.fetchone()

            return dict(row) if row else None

    def search_instruments(
        self,
        query: str,
        broker_name: str,
        exchange: Optional[Exchange] = None,
        category: Optional[InstrumentCategory] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Search instruments with filters"""

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            search_query = """
                SELECT i.standardized_symbol, i.instrument_name, i.sector,
                       bi.broker_symbol, bi.broker_token, bi.tick_size, bi.lot_size,
                       e.exchange_code, ic.category_code
                FROM instruments i
                JOIN broker_instruments bi ON i.id = bi.instrument_id
                JOIN exchanges e ON i.exchange_id = e.id
                JOIN instrument_categories ic ON i.category_id = ic.id
                WHERE (i.standardized_symbol LIKE ? OR i.instrument_name LIKE ?)
                AND bi.broker_name = ?
                AND bi.is_tradeable = 1
            """

            params = [f"%{query}%", f"%{query}%", broker_name]

            if exchange:
                search_query += " AND e.exchange_code = ?"
                params.append(exchange.value)

            if category:
                search_query += " AND ic.category_code = ?"
                params.append(category.value)

            search_query += " ORDER BY i.standardized_symbol LIMIT ?"
            params.append(limit)

            cursor = conn.execute(search_query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_derivatives(
        self, underlying_symbol: str, broker_name: str, instrument_type: str = "OPTIONS"
    ) -> List[Dict[str, Any]]:
        """Get derivatives for specific underlying"""

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                """
                SELECT i.standardized_symbol, i.instrument_name,
                       bi.broker_symbol, bi.broker_token,
                       bi.expiry_date, bi.strike_price, bi.option_type,
                       bi.tick_size, bi.lot_size
                FROM instruments i
                JOIN broker_instruments bi ON i.id = bi.instrument_id
                JOIN instrument_categories ic ON i.category_id = ic.id
                WHERE i.underlying_symbol = ?
                AND bi.broker_name = ?
                AND ic.category_code = ?
                AND bi.is_tradeable = 1
                ORDER BY bi.expiry_date, bi.strike_price
            """,
                (underlying_symbol, broker_name, instrument_type),
            )

            return [dict(row) for row in cursor.fetchall()]

    def get_commodities(
        self, commodity_type: str, broker_name: str, exchange: Exchange = Exchange.MCX
    ) -> List[Dict[str, Any]]:
        """Get commodities by type"""

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                """
                SELECT i.standardized_symbol, i.instrument_name,
                       bi.broker_symbol, bi.broker_token,
                       bi.tick_size, bi.lot_size, i.commodity_unit
                FROM instruments i
                JOIN broker_instruments bi ON i.id = bi.instrument_id
                JOIN exchanges e ON i.exchange_id = e.id
                WHERE i.commodity_type = ?
                AND bi.broker_name = ?
                AND e.exchange_code = ?
                AND bi.is_tradeable = 1
                ORDER BY i.standardized_symbol
            """,
                (commodity_type, broker_name, exchange.value),
            )

            return [dict(row) for row in cursor.fetchall()]

    def add_instrument(self, instrument_data: Dict[str, Any]) -> int:
        """Add new instrument to database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO instruments (
                    standardized_symbol, instrument_name, exchange_id, category_id,
                    subcategory_id, isin, sector, industry, market_cap, face_value,
                    commodity_type, commodity_unit, delivery_center,
                    base_currency, quote_currency, underlying_symbol, underlying_type,
                    is_active, listing_date, expiry_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    instrument_data["standardized_symbol"],
                    instrument_data["instrument_name"],
                    instrument_data["exchange_id"],
                    instrument_data["category_id"],
                    instrument_data.get("subcategory_id"),
                    instrument_data.get("isin"),
                    instrument_data.get("sector"),
                    instrument_data.get("industry"),
                    instrument_data.get("market_cap"),
                    instrument_data.get("face_value"),
                    instrument_data.get("commodity_type"),
                    instrument_data.get("commodity_unit"),
                    instrument_data.get("delivery_center"),
                    instrument_data.get("base_currency"),
                    instrument_data.get("quote_currency"),
                    instrument_data.get("underlying_symbol"),
                    instrument_data.get("underlying_type"),
                    instrument_data.get("is_active", True),
                    instrument_data.get("listing_date"),
                    instrument_data.get("expiry_date"),
                ),
            )
            return cursor.lastrowid

    def add_broker_instrument(self, broker_instrument_data: Dict[str, Any]) -> int:
        """Add broker-specific instrument mapping"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO broker_instruments (
                    instrument_id, broker_name, broker_symbol, broker_token,
                    broker_instrument_key, tick_size, lot_size, margin_percentage,
                    expiry_date, strike_price, option_type, is_tradeable, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    broker_instrument_data["instrument_id"],
                    broker_instrument_data["broker_name"],
                    broker_instrument_data["broker_symbol"],
                    broker_instrument_data["broker_token"],
                    broker_instrument_data.get("broker_instrument_key"),
                    broker_instrument_data["tick_size"],
                    broker_instrument_data["lot_size"],
                    broker_instrument_data.get("margin_percentage"),
                    broker_instrument_data.get("expiry_date"),
                    broker_instrument_data.get("strike_price"),
                    broker_instrument_data.get("option_type"),
                    broker_instrument_data.get("is_tradeable", True),
                    broker_instrument_data.get("is_active", True),
                ),
            )
            return cursor.lastrowid

    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        with sqlite3.connect(self.db_path) as conn:
            stats = {}

            # Count instruments by category
            cursor = conn.execute("""
                SELECT ic.category_name, COUNT(*) as count
                FROM instruments i
                JOIN instrument_categories ic ON i.category_id = ic.id
                WHERE i.is_active = 1
                GROUP BY ic.category_name
            """)
            stats["instruments_by_category"] = dict(cursor.fetchall())

            # Count broker instruments
            cursor = conn.execute("""
                SELECT broker_name, COUNT(*) as count
                FROM broker_instruments
                WHERE is_active = 1
                GROUP BY broker_name
            """)
            stats["broker_instruments"] = dict(cursor.fetchall())

            # Total counts
            cursor = conn.execute(
                "SELECT COUNT(*) FROM instruments WHERE is_active = 1"
            )
            stats["total_instruments"] = cursor.fetchone()[0]

            cursor = conn.execute(
                "SELECT COUNT(*) FROM broker_instruments WHERE is_active = 1"
            )
            stats["total_broker_instruments"] = cursor.fetchone()[0]

            return stats
