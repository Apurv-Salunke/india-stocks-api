__all__ = [
    # Core trading constants
    "Side",
    "OrderType",
    "Product",
    "Validity",
    "Variety",
    "Status",
    # Exchange and segment constants
    "Exchange",
    "ExchangeCode",
    "Segment",
    # Instrument constants
    "InstrumentCategory",
    "OptionType",
    "CommodityType",
    "InstrumentType",
    # Market data constants
    "Interval",
    "CandleStick",
    # F&O specific constants
    "Root",
    "WeeklyExpiry",
    # Response format constants
    "Order",
    "Position",
    "Profile",
    "UniqueID",
]

# Standard format for datetime strings
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


# ============================================================================
# CORE TRADING CONSTANTS
# ============================================================================


class Side:
    """
    Order Side Constants.
    """

    BUY = "BUY"
    SELL = "SELL"


class OrderType:
    """
    Order Type Constants.
    """

    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SLM = "SLM"  # Stop Loss Market
    SL = "SL"  # Stop Loss


class Product:
    """
    Product Type Constants.
    """

    CNC = "CNC"  # Cash and Carry
    NRML = "NRML"  # Normal/Carry Forward
    MARGIN = "MARGIN"
    MIS = "MIS"  # Intraday/Margin
    BO = "BO"  # Bracket Order
    CO = "CO"  # Cover Order
    SM = "SM"  # SuperMultiple


class Validity:
    """
    Order Validity Constants.
    """

    DAY = "DAY"
    IOC = "IOC"  # Immediate or Cancel
    GTD = "GTD"  # Good Till Date
    GTC = "GTC"  # Good Till Cancel
    FOK = "FOK"  # Fill or Kill
    TTL = "TTL"  # Time to Live


class Variety:
    """
    Order Variety Constants.
    """

    REGULAR = "REGULAR"
    STOPLOSS = "STOPLOSS"
    AMO = "AMO"  # After Market Order
    BO = "BO"  # Bracket Order
    CO = "CO"  # Cover Order
    ICEBERG = "ICEBERG"
    AUCTION = "AUCTION"


class Status:
    """
    Order Status Constants.
    """

    PENDING = "PENDING"
    OPEN = "OPEN"
    PARTIALLYFILLED = "PARTIALLYFILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    MODIFIED = "MODIFIED"


# ============================================================================
# EXCHANGE AND SEGMENT CONSTANTS
# ============================================================================


class Exchange:
    """
    Indian Stock Exchanges (Database/Model)
    """

    NSE = "NSE"  # National Stock Exchange
    BSE = "BSE"  # Bombay Stock Exchange
    MCX = "MCX"  # Multi Commodity Exchange
    NCDEX = "NCDEX"  # National Commodity & Derivatives Exchange
    ICEX = "ICEX"  # Indian Commodity Exchange


class ExchangeCode:
    """
    Exchange Code Constants (Broker API)
    """

    NSE = "NSE"  # NSE Equity
    NFO = "NFO"  # NSE F&O
    BSE = "BSE"  # BSE Equity
    BFO = "BFO"  # BSE F&O
    NCO = "NCO"  # NSE Commodities
    BCO = "BCO"  # BSE Commodities
    BCD = "BCD"  # BSE Currency Derivatives
    MCX = "MCX"  # Multi Commodity Exchange
    NCDEX = "NCDEX"  # National Commodity & Derivatives Exchange
    ICEX = "ICEX"  # Indian Commodity Exchange
    CDS = "CDS"  # Currency Derivatives Segment


class Segment:
    """
    Trading Segment Constants.
    """

    EQ = "EQ"  # Equity
    FUT = "FUT"  # Futures
    OPT = "OPT"  # Options


# ============================================================================
# INSTRUMENT CONSTANTS
# ============================================================================


class InstrumentCategory:
    """
    Instrument Categories
    """

    EQUITY = "EQ"  # Equity stocks
    FUTURES = "FUT"  # Futures contracts
    OPTIONS = "OPT"  # Options contracts
    INDEX = "INDEX"  # Market indices
    COMMODITY = "COM"  # Commodity instruments
    CURRENCY = "CUR"  # Currency instruments
    DEBT = "DEBT"  # Debt instruments
    ETF = "ETF"  # Exchange Traded Funds
    REIT = "REIT"  # Real Estate Investment Trusts
    INVIT = "INVIT"  # Infrastructure Investment Trusts
    MUTUAL_FUND = "MF"  # Mutual Funds
    BOND = "BOND"  # Bonds


class OptionType:
    """
    Option Types
    """

    CALL = "CE"  # Call option
    PUT = "PE"  # Put option


class CommodityType:
    """
    Commodity Categories
    """

    METALS = "METALS"  # Metals (Gold, Silver, etc.)
    ENERGY = "ENERGY"  # Energy (Crude Oil, Natural Gas, etc.)
    AGRICULTURE = "AGRICULTURE"  # Agricultural commodities
    PRECIOUS_METALS = "PRECIOUS_METALS"  # Gold, Silver, Platinum
    INDUSTRIAL_METALS = "INDUSTRIAL_METALS"  # Copper, Zinc, etc.
    SOFT_COMMODITIES = "SOFT_COMMODITIES"  # Cotton, Sugar, etc.


class InstrumentType:
    """
    Detailed Instrument Types
    """

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


# ============================================================================
# MARKET DATA CONSTANTS
# ============================================================================


class Interval:
    """
    Time Interval Constants for Historical Data
    """

    ONE_MINUTE = "1m"
    THREE_MINUTE = "3m"
    FIVE_MINUTE = "5m"
    TEN_MINUTE = "10m"
    FIFTEEN_MINUTE = "15m"
    THIRTY_MINUTE = "30m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"


class CandleStick:
    """
    CandleStick Data Keys
    """

    DATETIME = "datetime"
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOLUME = "volume"
    OI = "oi"  # Open Interest


# ============================================================================
# F&O SPECIFIC CONSTANTS
# ============================================================================


class Root:
    """
    F&O Root Symbols
    """

    BNF = "BANKNIFTY"
    NF = "NIFTY"
    FNF = "FINNIFTY"
    MIDCPNF = "MIDCPNIFTY"
    SENSEX = "SENSEX"
    BANKEX = "BANKEX"


class WeeklyExpiry:
    """
    Weekly Expiry Constants
    """

    CURRENT = "CURRENT"
    NEXT = "NEXT"
    FAR = "FAR"
    EXPIRY = "Expiry"
    LOTSIZE = "LotSize"


# ============================================================================
# RESPONSE FORMAT CONSTANTS
# ============================================================================


class Order:
    """
    Unified Order Response Dictionary Keys
    """

    ID = "id"
    USERID = "userOrderId"
    CLIENTID = "clientId"
    TIMESTAMP = "timestamp"
    SYMBOL = "symbol"
    TOKEN = "token"
    SIDE = "side"
    TYPE = "type"
    AVGPRICE = "avgPrice"
    PRICE = "price"
    TRIGGERPRICE = "triggerPrice"
    TARGETPRICE = "targetPrice"
    STOPLOSSPRICE = "stoplossPrice"
    TRAILINGSTOPLOSS = "trailingStoploss"
    QUANTITY = "quantity"
    FILLEDQTY = "filled"
    REMAININGQTY = "remaining"
    CANCELLEDQTY = "cancelleldQty"
    STATUS = "status"
    REJECTREASON = "rejectReason"
    DISCLOSEDQUANTITY = "disclosedQuantity"
    PRODUCT = "product"
    SEGMENT = "segment"
    EXCHANGE = "exchange"
    VALIDITY = "validity"
    VARIETY = "variety"
    INFO = "info"


class Position:
    """
    Unified Account Positions Response Dictionary Keys
    """

    SYMBOL = "symbol"
    TOKEN = "token"
    NETQTY = "netQty"
    AVGPRICE = "avgPrice"
    MTM = "mtm"
    PNL = "pnl"
    BUYQTY = "buyQty"
    BUYPRICE = "buyPrice"
    SELLQTY = "sellQty"
    SELLPRICE = "sellPrice"
    LTP = "ltp"
    PRODUCT = "product"
    EXCHANGE = "exchange"
    INFO = "info"


class Profile:
    """
    Unified Account Profile Response Dictionary Keys
    """

    CLIENTID = "clientId"
    NAME = "name"
    EMAILID = "emailId"
    MOBILENO = "mobileNo"
    PAN = "pan"
    ADDRESS = "address"
    BANKNAME = "bankName"
    BANKBRANCHNAME = "bankBranchName"
    BANKACCNO = "bankAccNo"
    EXHCNAGESENABLED = "exchangesEnabled"
    ENABLED = "enabled"
    INFO = "info"


class UniqueID:
    """
    Default Unique Order ID Constants
    """

    DEFORDER = "FenixOrder"
    MARKETORDER = "MarketOrder"
    LIMITORDER = "LIMITOrder"
    SLORDER = "SLOrder"
    SLMORDER = "SLMOrder"
