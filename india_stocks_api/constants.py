"""
Standard Enums for the Indian Stocks API.
These enums ensure type safety and prevent string typos in the public API.
"""
from enum import Enum

class OrderType(str, Enum):
    """Supported Order Types"""
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"       # Stop Loss Limit
    SLM = "SL-M"    # Stop Loss Market

class TransactionType(str, Enum):
    """Buy or Sell action"""
    BUY = "BUY"
    SELL = "SELL"

class ProductType(str, Enum):
    """Product types for orders"""
    INTRADAY = "MIS"          # Margin Intraday Squareoff
    DELIVERY = "CNC"          # Cash N Carry (Equity Delivery)
    CARRYFORWARD = "NRML"     # Normal (F&O Carry Forward)
    COVER_ORDER = "CO"        # Cover Order
    BRACKET_ORDER = "BO"      # Bracket Order

class OrderValidity(str, Enum):
    """Order validity types"""
    DAY = "DAY"
    IOC = "IOC"  # Immediate or Cancel
    EOS = "EOS"  # End of Session (some brokers)

class OptionType(str, Enum):
    """Option Types"""
    CE = "CE"  # Call Option
    PE = "PE"  # Put Option

class CandleInterval(str, Enum):
    """Supported Candle Intervals"""
    ONE_MINUTE = "1m"
    THREE_MINUTE = "3m"
    FIVE_MINUTE = "5m"
    TEN_MINUTE = "10m"
    FIFTEEN_MINUTE = "15m"
    THIRTY_MINUTE = "30m"
    ONE_HOUR = "1h"
    ONE_DAY = "D"
