"""
Domain Objects for Indian Stocks API.
This module defines the objects used to represent tradable financial instruments.
Uses slots=True for memory efficiency.
"""
from dataclasses import dataclass
from datetime import date
from ..constants import OptionType

@dataclass(slots=True, frozen=True)
class Equity:
    """
    Represents a simple Tradable Stock or ETF.
    Example: Equity("RELIANCE")
    """
    symbol: str
    exchange: str = "NSE"

@dataclass(slots=True, frozen=True)
class Index:
    """
    Represents a Non-Tradable Market Index.
    Used for quotes/charts only.
    Example: Index("NIFTY 50")
    """
    symbol: str
    exchange: str = "NSE"

@dataclass(slots=True, frozen=True)
class Future:
    """
    Represents a Futures Contract.
    Example: Future("NIFTY", date(2024, 1, 25))
    """
    symbol: str
    expiry: date
    exchange: str = "NFO"

@dataclass(slots=True, frozen=True)
class Option:
    """
    Represents an Options Contract.
    Example: Option("BANKNIFTY", date(2024, 1, 25), 48000.0, OptionType.CE)
    """
    symbol: str
    expiry: date
    strike: float
    opt_type: OptionType
    exchange: str = "NFO"
