from abc import ABC
from datetime import date
from enum import Enum

class SecType(str, Enum):
    EQ = "EQ"
    FUT = "FUT"
    OPT = "OPT"
    IDX = "IDX"  # New type for Indices

class Instrument(ABC):
    """Base class for all financial instruments"""
    symbol: str  # The Standardized OpenAlgo Symbol (e.g., "NIFTY", "RELIANCE")
    exchange: str
    currency: str = "INR"

class Equity(Instrument):
    """Represents a simple Tradable Stock, ETF, MF"""
    def __init__(self, symbol: str, exchange: str = "NSE"):
        self.symbol = symbol  # e.g., "RELIANCE"
        self.exchange = exchange
        self.sec_type = SecType.EQ

class Index(Instrument):
    """
    Represents a Non-Tradable Market Index (e.g., NIFTY 50).
    Used for getting quotes/charts, NOT for placing orders.
    """
    def __init__(self, symbol: str, exchange: str = "NSE"):
        self.symbol = symbol  # e.g., "NIFTY 50"
        self.exchange = exchange
        self.sec_type = SecType.IDX

class Future(Instrument):
    """Represents a Futures Contract"""
    def __init__(self, symbol: str, expiry: date, exchange: str = "NFO"):
        self.symbol = symbol  # e.g., "NIFTY"
        self.expiry = expiry
        self.exchange = exchange
        self.sec_type = SecType.FUT

class OptionType(str, Enum):
    CE = "CE"
    PE = "PE"

class Option(Instrument):
    """Represents an Options Contract"""
    def __init__(self, symbol: str, expiry: date, strike: float, opt_type: OptionType, exchange: str = "NFO"):
        self.symbol = symbol  # e.g., "BANKNIFTY"
        self.expiry = expiry
        self.strike = strike
        self.opt_type = opt_type # CE/PE
        self.exchange = exchange
        self.sec_type = SecType.OPT
