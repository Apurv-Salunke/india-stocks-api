"""
Database module for India Stocks API

This module provides a comprehensive SQLite-based instrument database system
that supports multiple brokers and instrument types across Indian exchanges.

Key Components:
- Models: Data models for instruments, exchanges, and broker mappings
- Providers: Data providers for different brokers (AngelOne, etc.)
- Services: High-level services for instrument operations
- Migrations: Database schema management
"""

from .services.instrument_service import InstrumentService
from .models.enums import Exchange, InstrumentCategory, OptionType, CommodityType
from .models.instrument import Instrument
from .models.broker_instrument import BrokerInstrument
from .providers import AngelOneTokensManager

__all__ = [
    "InstrumentService",
    "Exchange",
    "InstrumentCategory",
    "OptionType",
    "CommodityType",
    "Instrument",
    "BrokerInstrument",
    "AngelOneTokensManager",
]

__version__ = "0.1.0"
