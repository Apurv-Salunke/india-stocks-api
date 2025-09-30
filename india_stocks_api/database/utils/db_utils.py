"""
Database utility functions
"""

import sqlite3
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class DatabaseUtils:
    """Utility functions for database operations"""

    @staticmethod
    def get_exchange_id(exchange_code: str, db_path: str) -> Optional[int]:
        """Get exchange ID by exchange code"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    "SELECT id FROM exchanges WHERE exchange_code = ?", (exchange_code,)
                )
                result = cursor.fetchone()
                return result[0] if result else None
        except sqlite3.Error as e:
            logger.error(f"Error getting exchange ID for {exchange_code}: {e}")
            return None

    @staticmethod
    def get_category_id(category_code: str, db_path: str) -> Optional[int]:
        """Get category ID by category code"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    "SELECT id FROM instrument_categories WHERE category_code = ?",
                    (category_code,),
                )
                result = cursor.fetchone()
                return result[0] if result else None
        except sqlite3.Error as e:
            logger.error(f"Error getting category ID for {category_code}: {e}")
            return None

    @staticmethod
    def get_subcategory_id(
        subcategory_code: str, category_id: int, db_path: str
    ) -> Optional[int]:
        """Get subcategory ID by subcategory code and category ID"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    "SELECT id FROM instrument_subcategories WHERE subcategory_code = ? AND category_id = ?",
                    (subcategory_code, category_id),
                )
                result = cursor.fetchone()
                return result[0] if result else None
        except sqlite3.Error as e:
            logger.error(f"Error getting subcategory ID for {subcategory_code}: {e}")
            return None

    @staticmethod
    def get_instrument_id(
        standardized_symbol: str, exchange_id: int, category_id: int, db_path: str
    ) -> Optional[int]:
        """Get instrument ID by standardized symbol, exchange, and category"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    "SELECT id FROM instruments WHERE standardized_symbol = ? AND exchange_id = ? AND category_id = ?",
                    (standardized_symbol, exchange_id, category_id),
                )
                result = cursor.fetchone()
                return result[0] if result else None
        except sqlite3.Error as e:
            logger.error(f"Error getting instrument ID for {standardized_symbol}: {e}")
            return None

    @staticmethod
    def convert_to_nse_format(broker_symbol: str, exchange_code: str) -> str:
        """Convert broker symbol to standardized NSE format"""
        if exchange_code == "NSE":
            # Remove -EQ suffix for standardization
            if broker_symbol.endswith("-EQ"):
                return broker_symbol[:-3]
            return broker_symbol
        elif exchange_code == "BSE":
            # Keep BSE symbols as-is for now
            return broker_symbol
        return broker_symbol

    @staticmethod
    def determine_commodity_type(symbol: str, name: str) -> Optional[str]:
        """Determine commodity type based on symbol and name"""
        symbol_upper = symbol.upper()
        name_upper = name.upper()

        # Precious metals
        if any(
            metal in symbol_upper or metal in name_upper
            for metal in ["GOLD", "SILVER", "PLATINUM", "PALLADIUM"]
        ):
            return "PRECIOUS_METALS"

        # Industrial metals
        if any(
            metal in symbol_upper or metal in name_upper
            for metal in ["COPPER", "ZINC", "LEAD", "NICKEL", "ALUMINIUM", "ALUMINUM"]
        ):
            return "INDUSTRIAL_METALS"

        # Energy
        if any(
            energy in symbol_upper or energy in name_upper
            for energy in ["CRUDE", "OIL", "GAS", "COAL", "NATURAL"]
        ):
            return "ENERGY"

        # Agriculture
        if any(
            agri in symbol_upper or agri in name_upper
            for agri in ["WHEAT", "RICE", "CORN", "SOYBEAN", "COTTON", "SUGAR"]
        ):
            return "AGRICULTURE"

        # Soft commodities
        if any(
            soft in symbol_upper or soft in name_upper
            for soft in ["COFFEE", "TEA", "COCOA", "RUBBER"]
        ):
            return "SOFT_COMMODITIES"

        return None

    @staticmethod
    def determine_instrument_type(
        category_code: str, underlying_type: Optional[str] = None
    ) -> str:
        """Determine detailed instrument type"""
        if category_code == "EQ":
            return "EQUITY"
        elif category_code == "FUT":
            if underlying_type == "INDEX":
                return "INDEX_FUTURE"
            elif underlying_type == "EQUITY":
                return "STOCK_FUTURE"
            elif underlying_type == "COMMODITY":
                return "COMMODITY_FUTURE"
            else:
                return "FUTURE"
        elif category_code == "OPT":
            if underlying_type == "INDEX":
                return "INDEX_OPTION"
            elif underlying_type == "EQUITY":
                return "STOCK_OPTION"
            elif underlying_type == "COMMODITY":
                return "COMMODITY_OPTION"
            else:
                return "OPTION"
        elif category_code == "COM":
            return "COMMODITY"
        elif category_code == "CUR":
            return "CURRENCY"
        else:
            return "UNKNOWN"

    @staticmethod
    def validate_symbol_data(data: Dict[str, Any]) -> bool:
        """Validate symbol data before insertion"""
        required_fields = [
            "standardized_symbol",
            "instrument_name",
            "broker_symbol",
            "broker_token",
        ]

        for field in required_fields:
            if field not in data or not data[field]:
                logger.error(f"Missing required field: {field}")
                return False

        # Validate numeric fields
        numeric_fields = ["tick_size", "lot_size"]
        for field in numeric_fields:
            if field in data and data[field] is not None:
                try:
                    float(data[field])
                    if field == "lot_size" and int(data[field]) <= 0:
                        logger.error(f"Lot size must be positive: {data[field]}")
                        return False
                except (ValueError, TypeError):
                    logger.error(f"Invalid numeric value for {field}: {data[field]}")
                    return False

        return True

    @staticmethod
    def backup_database(db_path: str, backup_path: Optional[str] = None) -> bool:
        """Create backup of database"""
        try:
            if backup_path is None:
                backup_path = f"{db_path}.backup"

            # Copy database file
            import shutil

            shutil.copy2(db_path, backup_path)
            logger.info(f"Database backed up to: {backup_path}")
            return True

        except Exception as e:
            logger.error(f"Error creating database backup: {e}")
            return False

    @staticmethod
    def get_database_size(db_path: str) -> int:
        """Get database file size in bytes"""
        try:
            return Path(db_path).stat().st_size
        except Exception as e:
            logger.error(f"Error getting database size: {e}")
            return 0
