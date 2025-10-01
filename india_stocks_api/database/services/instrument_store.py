"""
Instrument store for database operations
"""

import sqlite3
import threading
from typing import List, Dict, Any, Optional
from pathlib import Path
import logging

from ...brokers.base.constants import Exchange, InstrumentCategory
from ..utils.db_utils import DatabaseUtils

logger = logging.getLogger(__name__)


class InstrumentStore:
    """Database store for instrument operations"""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self._db_lock = threading.Lock()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def _connect(self):
        """Get database connection with proper settings"""
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def upsert_equities(self, rows: List[Dict[str, Any]]) -> int:
        """Bulk upsert equity instruments"""
        if not rows:
            return 0

        with self._db_lock:
            try:
                with self._connect() as conn:
                    conn.execute("BEGIN IMMEDIATE TRANSACTION")

                    # Pre-fetch exchange and category IDs
                    exchange_ids = {}
                    for item in rows:
                        exchange_code = item["exchange_code"]
                        if exchange_code not in exchange_ids:
                            exchange_ids[exchange_code] = DatabaseUtils.get_exchange_id(
                                exchange_code, str(self.db_path)
                            )

                    category_id = DatabaseUtils.get_category_id("EQ", str(self.db_path))

                    # Bulk insert instruments
                    instrument_sql = """
                    INSERT OR REPLACE INTO instruments
                    (standardized_symbol, instrument_name, exchange_id, category_id, subcategory_id,
                     underlying_symbol, expiry_date, isin, sector, industry, is_active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """

                    instrument_data = []
                    for item in rows:
                        exchange_id = exchange_ids.get(item["exchange_code"])

                        if not exchange_id or not category_id:
                            continue

                        instrument_data.append(
                            (
                                item["standardized_symbol"],
                                item["instrument_name"],
                                exchange_id,
                                category_id,
                                None,  # subcategory_id
                                None,  # underlying_symbol
                                None,  # expiry_date
                                item.get("isin"),
                                item.get("sector"),
                                item.get("industry"),
                            )
                        )

                    if instrument_data:
                        # Insert instruments one by one to get IDs
                        instrument_ids = []
                        for item_data in instrument_data:
                            cursor = conn.execute(instrument_sql, item_data)
                            instrument_ids.append(cursor.lastrowid)

                        # Get the first inserted ID to calculate range
                        # Note: executemany doesn't return lastrowid, so we need a different approach
                        # For now, we'll use a simpler approach without bulk inserts
                        instrument_ids = []
                        for item_data in instrument_data:
                            cursor = conn.execute(instrument_sql, item_data)
                            instrument_ids.append(cursor.lastrowid)

                        # Bulk insert broker instruments
                        broker_sql = """
                        INSERT OR REPLACE INTO broker_instruments
                        (instrument_id, broker_name, broker_symbol, broker_token, tick_size, lot_size,
                         created_at, updated_at)
                        VALUES (?, 'angelone', ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """

                        broker_data = []
                        for i, item in enumerate(rows):
                            if i < len(instrument_ids):
                                broker_data.append(
                                    (
                                        instrument_ids[i],
                                        item["broker_symbol"],
                                        item["broker_token"],
                                        item["tick_size"],
                                        item["lot_size"],
                                    )
                                )

                        if broker_data:
                            conn.executemany(broker_sql, broker_data)

                    conn.commit()
                    return len(instrument_data)

            except Exception as e:
                self.logger.error(f"Error in bulk equity upsert: {e}")
                return 0

    def upsert_fno(self, rows: List[Dict[str, Any]]) -> int:
        """Bulk upsert F&O instruments"""
        if not rows:
            return 0

        with self._db_lock:
            try:
                with self._connect() as conn:
                    conn.execute("BEGIN IMMEDIATE TRANSACTION")

                    # Pre-fetch exchange and category IDs
                    exchange_ids = {}
                    for item in rows:
                        exchange_code = item["exchange_code"]
                        if exchange_code not in exchange_ids:
                            exchange_ids[exchange_code] = DatabaseUtils.get_exchange_id(
                                exchange_code, str(self.db_path)
                            )

                    # Split rows into futures and options based on instrument_type
                    future_rows = []
                    option_rows = []

                    for row in rows:
                        instrument_type = row.get("instrument_type", "")
                        if instrument_type in ["FUTSTK", "FUTIDX", "FUTCOM", "FUTCUR"]:
                            future_rows.append(row)
                        elif instrument_type in [
                            "OPTSTK",
                            "OPTIDX",
                            "OPTCOM",
                            "OPTCUR",
                        ]:
                            option_rows.append(row)
                        else:
                            # Default to future for unknown types
                            future_rows.append(row)

                    # Process futures and options separately
                    total_stored = 0

                    # Process futures
                    if future_rows:
                        future_category_id = DatabaseUtils.get_category_id(
                            "FUT", str(self.db_path)
                        )
                        stored_count = self._process_fno_batch(
                            future_rows, future_category_id, conn
                        )
                        total_stored += stored_count

                    # Process options
                    if option_rows:
                        option_category_id = DatabaseUtils.get_category_id(
                            "OPT", str(self.db_path)
                        )
                        stored_count = self._process_fno_batch(
                            option_rows, option_category_id, conn
                        )
                        total_stored += stored_count

                    conn.execute("COMMIT")
                    return total_stored

            except Exception as e:
                self.logger.error(f"Error in bulk F&O upsert: {e}")
                return 0

    def _process_fno_batch(
        self, rows: List[Dict[str, Any]], category_id: int, conn
    ) -> int:
        """Process a batch of F&O instruments with the given category

        New schema: F&O derivatives are stored ONLY in broker_instruments,
        linked to their underlying instrument via underlying_instrument_id.
        """
        if not rows:
            return 0

        # Pre-fetch exchange IDs
        exchange_ids = {}
        for item in rows:
            exchange_code = item["exchange_code"]
            if exchange_code not in exchange_ids:
                exchange_ids[exchange_code] = DatabaseUtils.get_exchange_id(
                    exchange_code, str(self.db_path)
                )

        # For each derivative, find or resolve its underlying instrument
        broker_data = []

        for item in rows:
            underlying_symbol = item.get("underlying_symbol")
            if not underlying_symbol:
                self.logger.warning(
                    f"Skipping {item.get('broker_symbol')}: no underlying_symbol"
                )
                continue

            # Find the underlying instrument in instruments table
            # The underlying should already exist (equity, index, or commodity)
            underlying_query = """
                SELECT i.id
                FROM instruments i
                WHERE i.standardized_symbol = ?
                LIMIT 1
            """

            cursor = conn.execute(underlying_query, (underlying_symbol,))
            underlying_row = cursor.fetchone()

            if not underlying_row:
                # Underlying doesn't exist - log and skip
                self.logger.warning(
                    f"Skipping {item.get('broker_symbol')}: underlying '{underlying_symbol}' not found"
                )
                continue

            underlying_instrument_id = underlying_row[0]

            # Add to broker_instruments with underlying_instrument_id
            broker_data.append(
                (
                    underlying_instrument_id,  # underlying_instrument_id
                    item["broker_symbol"],
                    item["broker_token"],
                    item["tick_size"],
                    item["lot_size"],
                    item.get("expiry_date"),
                    item.get("strike_price"),
                    item.get("option_type"),
                )
            )

        # Bulk insert into broker_instruments only
        if broker_data:
            broker_sql = """
            INSERT OR REPLACE INTO broker_instruments
            (underlying_instrument_id, broker_name, broker_symbol, broker_token, tick_size, lot_size,
             expiry_date, strike_price, option_type, created_at, updated_at)
            VALUES (?, 'angelone', ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            conn.executemany(broker_sql, broker_data)

        return len(broker_data)

    def upsert_commodities(self, rows: List[Dict[str, Any]]) -> int:
        """Bulk upsert commodity instruments"""
        if not rows:
            return 0

        with self._db_lock:
            try:
                with self._connect() as conn:
                    conn.execute("BEGIN IMMEDIATE TRANSACTION")

                    # Pre-fetch exchange and category IDs
                    exchange_ids = {}
                    for item in rows:
                        exchange_code = item["exchange_code"]
                        if exchange_code not in exchange_ids:
                            exchange_ids[exchange_code] = DatabaseUtils.get_exchange_id(
                                exchange_code, str(self.db_path)
                            )

                    category_id = DatabaseUtils.get_category_id(
                        "COM", str(self.db_path)
                    )

                    # Bulk insert instruments
                    instrument_sql = """
                    INSERT OR REPLACE INTO instruments
                    (standardized_symbol, instrument_name, exchange_id, category_id, subcategory_id,
                     commodity_type, commodity_unit, is_active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """

                    instrument_data = []
                    for item in rows:
                        exchange_id = exchange_ids.get(item["exchange_code"])

                        if not exchange_id or not category_id:
                            continue

                        instrument_data.append(
                            (
                                item["standardized_symbol"],
                                item["instrument_name"],
                                exchange_id,
                                category_id,
                                None,  # subcategory_id
                                item.get("commodity_type"),
                                item.get("commodity_unit"),
                            )
                        )

                    if instrument_data:
                        # Insert instruments one by one to get IDs
                        instrument_ids = []
                        for item_data in instrument_data:
                            cursor = conn.execute(instrument_sql, item_data)
                            instrument_ids.append(cursor.lastrowid)

                        # Get the first inserted ID to calculate range
                        # Note: executemany doesn't return lastrowid, so we need a different approach
                        # For now, we'll use a simpler approach without bulk inserts
                        instrument_ids = []
                        for item_data in instrument_data:
                            cursor = conn.execute(instrument_sql, item_data)
                            instrument_ids.append(cursor.lastrowid)

                        # Bulk insert broker instruments
                        broker_sql = """
                        INSERT OR REPLACE INTO broker_instruments
                        (instrument_id, broker_name, broker_symbol, broker_token, tick_size, lot_size,
                         created_at, updated_at)
                        VALUES (?, 'angelone', ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """

                        broker_data = []
                        for i, item in enumerate(rows):
                            if i < len(instrument_ids):
                                broker_data.append(
                                    (
                                        instrument_ids[i],
                                        item["broker_symbol"],
                                        item["broker_token"],
                                        item["tick_size"],
                                        item["lot_size"],
                                    )
                                )

                        if broker_data:
                            conn.executemany(broker_sql, broker_data)

                    conn.commit()
                    return len(instrument_data)

            except Exception as e:
                self.logger.error(f"Error in bulk commodity upsert: {e}")
                return 0

    def upsert_currencies(self, rows: List[Dict[str, Any]]) -> int:
        """Bulk upsert currency instruments"""
        if not rows:
            return 0

        with self._db_lock:
            try:
                with self._connect() as conn:
                    conn.execute("BEGIN IMMEDIATE TRANSACTION")

                    # Pre-fetch exchange and category IDs
                    exchange_ids = {}
                    for item in rows:
                        exchange_code = item["exchange_code"]
                        if exchange_code not in exchange_ids:
                            exchange_ids[exchange_code] = DatabaseUtils.get_exchange_id(
                                exchange_code, str(self.db_path)
                            )

                    category_id = DatabaseUtils.get_category_id(
                        "CUR", str(self.db_path)
                    )

                    # Bulk insert instruments
                    instrument_sql = """
                    INSERT OR REPLACE INTO instruments
                    (standardized_symbol, instrument_name, exchange_id, category_id, subcategory_id,
                     base_currency, quote_currency, is_active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """

                    instrument_data = []
                    for item in rows:
                        exchange_id = exchange_ids.get(item["exchange_code"])

                        if not exchange_id or not category_id:
                            continue

                        instrument_data.append(
                            (
                                item["standardized_symbol"],
                                item["instrument_name"],
                                exchange_id,
                                category_id,
                                None,  # subcategory_id
                                item.get("base_currency"),
                                item.get("quote_currency"),
                            )
                        )

                    if instrument_data:
                        # Insert instruments one by one to get IDs
                        instrument_ids = []
                        for item_data in instrument_data:
                            cursor = conn.execute(instrument_sql, item_data)
                            instrument_ids.append(cursor.lastrowid)

                        # Get the first inserted ID to calculate range
                        # Note: executemany doesn't return lastrowid, so we need a different approach
                        # For now, we'll use a simpler approach without bulk inserts
                        instrument_ids = []
                        for item_data in instrument_data:
                            cursor = conn.execute(instrument_sql, item_data)
                            instrument_ids.append(cursor.lastrowid)

                        # Bulk insert broker instruments
                        broker_sql = """
                        INSERT OR REPLACE INTO broker_instruments
                        (instrument_id, broker_name, broker_symbol, broker_token, tick_size, lot_size,
                         created_at, updated_at)
                        VALUES (?, 'angelone', ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """

                        broker_data = []
                        for i, item in enumerate(rows):
                            if i < len(instrument_ids):
                                broker_data.append(
                                    (
                                        instrument_ids[i],
                                        item["broker_symbol"],
                                        item["broker_token"],
                                        item["tick_size"],
                                        item["lot_size"],
                                    )
                                )

                        if broker_data:
                            conn.executemany(broker_sql, broker_data)

                    conn.commit()
                    return len(instrument_data)

            except Exception as e:
                self.logger.error(f"Error in bulk currency upsert: {e}")
                return 0

    def upsert_indices(self, rows: List[Dict[str, Any]]) -> int:
        """Bulk upsert index instruments"""
        if not rows:
            return 0

        with self._db_lock:
            try:
                with self._connect() as conn:
                    conn.execute("BEGIN IMMEDIATE TRANSACTION")

                    # Pre-fetch exchange and category IDs
                    exchange_ids = {}
                    for item in rows:
                        exchange_code = item["exchange_code"]
                        if exchange_code not in exchange_ids:
                            exchange_ids[exchange_code] = DatabaseUtils.get_exchange_id(
                                exchange_code, str(self.db_path)
                            )

                    category_id = DatabaseUtils.get_category_id(
                        "INDEX", str(self.db_path)
                    )

                    if not category_id:
                        self.logger.error("INDEX category not found in database")
                        return 0

                    # Bulk insert instruments
                    instrument_sql = """
                    INSERT OR IGNORE INTO instruments
                    (standardized_symbol, instrument_name, exchange_id, category_id,
                     is_active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """

                    instrument_data = []
                    for item in rows:
                        exchange_id = exchange_ids.get(item["exchange_code"])

                        if not category_id or not exchange_id:
                            continue

                        instrument_data.append(
                            (
                                item["standardized_symbol"],
                                item["instrument_name"],
                                exchange_id,
                                category_id,
                            )
                        )

                    if instrument_data:
                        # Insert instruments one by one to get IDs
                        instrument_ids = []
                        for item_data in instrument_data:
                            cursor = conn.execute(instrument_sql, item_data)
                            instrument_ids.append(cursor.lastrowid)

                        # Bulk insert broker instruments
                        broker_sql = """
                        INSERT OR REPLACE INTO broker_instruments
                        (instrument_id, broker_name, broker_symbol, broker_token, tick_size, lot_size,
                         created_at, updated_at)
                        VALUES (?, 'angelone', ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """

                        broker_data = []
                        for i, item in enumerate(rows):
                            if i < len(instrument_ids):
                                broker_data.append(
                                    (
                                        instrument_ids[i],
                                        item["broker_symbol"],
                                        item["broker_token"],
                                        item.get("tick_size", 0.05),
                                        item.get("lot_size", 1),
                                    )
                                )

                        if broker_data:
                            conn.executemany(broker_sql, broker_data)

                    conn.commit()
                    return len(instrument_data)

            except Exception as e:
                self.logger.error(f"Error in bulk index upsert: {e}")
                return 0

    def resolve_instrument(
        self,
        standardized_symbol: str,
        exchange: Exchange,
        category: InstrumentCategory,
        broker_name: str,
        expiry_date: Optional[str] = None,
        strike_price: Optional[float] = None,
        option_type: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Resolve standardized symbol to broker-specific format"""

        with self._connect() as conn:
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
