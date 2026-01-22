from enum import Enum


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SL = "SL"
    SLM = "SL-M"

class TransactionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

class ProductType(str, Enum):
    INTRADAY = "MIS"
    DELIVERY = "CNC"
    CARRYFORWARD = "NRML"

class OrderValidity(str, Enum):
    DAY = "DAY"
    IOC = "IOC"

class TimeFrame(str, Enum):
    """Standard intervals for Historical Data (Angel One / Zerodha styles)"""
    MIN_1 = "ONE_MINUTE"
    MIN_3 = "THREE_MINUTE"
    MIN_5 = "FIVE_MINUTE"
    MIN_10 = "TEN_MINUTE"
    MIN_15 = "FIFTEEN_MINUTE"
    MIN_30 = "THIRTY_MINUTE"
    MIN_60 = "ONE_HOUR"
    DAY = "ONE_DAY"