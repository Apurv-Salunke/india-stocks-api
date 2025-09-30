"""
Enums for instrument database models
"""

from enum import Enum


class Exchange(Enum):
    """Indian stock exchanges"""

    NSE = "NSE"  # National Stock Exchange
    BSE = "BSE"  # Bombay Stock Exchange
    MCX = "MCX"  # Multi Commodity Exchange
    NCDEX = "NCDEX"  # National Commodity & Derivatives Exchange
    ICEX = "ICEX"  # Indian Commodity Exchange


class InstrumentCategory(Enum):
    """Instrument categories"""

    EQUITY = "EQ"  # Equity stocks
    FUTURES = "FUT"  # Futures contracts
    OPTIONS = "OPT"  # Options contracts
    COMMODITY = "COM"  # Commodity instruments
    CURRENCY = "CUR"  # Currency instruments
    DEBT = "DEBT"  # Debt instruments
    ETF = "ETF"  # Exchange Traded Funds
    REIT = "REIT"  # Real Estate Investment Trusts
    INVIT = "INVIT"  # Infrastructure Investment Trusts
    MUTUAL_FUND = "MF"  # Mutual Funds
    BOND = "BOND"  # Bonds


class OptionType(Enum):
    """Option types"""

    CALL = "CE"  # Call option
    PUT = "PE"  # Put option


class CommodityType(Enum):
    """Commodity categories"""

    METALS = "METALS"  # Metals (Gold, Silver, etc.)
    ENERGY = "ENERGY"  # Energy (Crude Oil, Natural Gas, etc.)
    AGRICULTURE = "AGRICULTURE"  # Agricultural commodities
    PRECIOUS_METALS = "PRECIOUS_METALS"  # Gold, Silver, Platinum
    INDUSTRIAL_METALS = "INDUSTRIAL_METALS"  # Copper, Zinc, etc.
    SOFT_COMMODITIES = "SOFT_COMMODITIES"  # Cotton, Sugar, etc.


class Segment(Enum):
    """Trading segments"""

    EQUITY = "EQ"  # Equity segment
    FUTURES = "FUT"  # Futures segment
    OPTIONS = "OPT"  # Options segment
    CURRENCY = "CUR"  # Currency segment
    COMMODITY = "COM"  # Commodity segment


class InstrumentType(Enum):
    """Detailed instrument types"""

    STOCK = "STOCK"  # Individual stocks
    INDEX = "INDEX"  # Market indices
    FUTURE = "FUTURE"  # Future contracts
    OPTION = "OPTION"  # Option contracts
    COMMODITY_FUTURE = "COMMODITY_FUTURE"  # Commodity futures
    COMMODITY_OPTION = "COMMODITY_OPTION"  # Commodity options
    CURRENCY_FUTURE = "CURRENCY_FUTURE"  # Currency futures
    CURRENCY_OPTION = "CURRENCY_OPTION"  # Currency options
    ETF = "ETF"  # Exchange Traded Funds
    BOND = "BOND"  # Government/Corporate bonds


class OrderSide(Enum):
    """Order sides"""

    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    """Order types"""

    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_LOSS = "SL"
    STOP_LOSS_MARKET = "SLM"


class ProductType(Enum):
    """Product types for trading"""

    INTRADAY = "MIS"  # Intraday/Margin
    DELIVERY = "CNC"  # Cash and Carry
    CARRY_FORWARD = "NRML"  # Normal/Carry Forward
    MARGIN = "MARGIN"  # Margin trading
    BRACKET_ORDER = "BO"  # Bracket Order
    COVER_ORDER = "CO"  # Cover Order
