"""
Database module for India Stocks API
Manages broker instruments and token mappings using SQLAlchemy
"""

from .broker_instruments_db import (
    BrokerInstrument,
    initialize_broker_database,
    clear_broker_instruments,
    store_broker_instruments,
    get_broker_token,
    get_broker_symbol,
    get_standardized_symbol,
    get_instrument_details,
    get_database_statistics,
    search_instruments,
    DATABASE_PATH,
    DATABASE_URL,
)

__all__ = [
    "BrokerInstrument",
    "initialize_broker_database",
    "clear_broker_instruments",
    "store_broker_instruments",
    "get_broker_token",
    "get_broker_symbol",
    "get_standardized_symbol",
    "get_instrument_details",
    "get_database_statistics",
    "search_instruments",
    "DATABASE_PATH",
    "DATABASE_URL",
]

__version__ = "1.0.0"
