from core.brokers.base.constants import (
    ExchangeCode,
    Order,
    Position,
    Product,
    Profile,
    Side,
    Root,
    Status,
    UniqueID,
    Validity,
    Variety,
    WeeklyExpiry,
    Option,
    OrderType,
)


def test_side_constants():
    assert Side.BUY == "BUY"
    assert Side.SELL == "SELL"


def test_root_constants():
    assert Root.BNF == "BANKNIFTY"
    assert Root.NF == "NIFTY"
    assert Root.FNF == "FINNIFTY"
    assert Root.MIDCPNF == "MIDCPNIFTY"
    assert Root.SENSEX == "SENSEX"
    assert Root.BANKEX == "BANKEX"


def test_weekly_expiry_constants():
    assert WeeklyExpiry.CURRENT == "CURRENT"
    assert WeeklyExpiry.NEXT == "NEXT"
    assert WeeklyExpiry.FAR == "FAR"
    assert WeeklyExpiry.EXPIRY == "Expiry"
    assert WeeklyExpiry.LOTSIZE == "LotSize"


def test_option_constants():
    assert Option.CE == "CE"
    assert Option.PE == "PE"


def test_order_type_constants():
    assert OrderType.MARKET == "MARKET"
    assert OrderType.LIMIT == "LIMIT"
    assert OrderType.SLM == "SLM"
    assert OrderType.SL == "SL"


def test_exchange_code_constants():
    assert ExchangeCode.NSE == "NSE"
    assert ExchangeCode.NFO == "NFO"
    assert ExchangeCode.BSE == "BSE"
    assert ExchangeCode.BFO == "BFO"
    assert ExchangeCode.NCO == "NCO"
    assert ExchangeCode.BCO == "BCO"
    assert ExchangeCode.BCD == "BCD"
    assert ExchangeCode.MCX == "MCX"
    assert ExchangeCode.CDS == "CDS"


def test_product_constants():
    assert Product.CNC == "CNC"
    assert Product.NRML == "NRML"
    assert Product.MARGIN == "MARGIN"
    assert Product.MIS == "MIS"
    assert Product.BO == "BO"
    assert Product.CO == "CO"
    assert Product.SM == "SM"


def test_validity_constants():
    assert Validity.DAY == "DAY"
    assert Validity.IOC == "IOC"
    assert Validity.GTD == "GTD"
    assert Validity.GTC == "GTC"
    assert Validity.FOK == "FOK"
    assert Validity.TTL == "TTL"


def test_variety_constants():
    assert Variety.REGULAR == "REGULAR"
    assert Variety.STOPLOSS == "STOPLOSS"
    assert Variety.AMO == "AMO"
    assert Variety.BO == "BO"
    assert Variety.CO == "CO"
    assert Variety.ICEBERG == "ICEBERG"
    assert Variety.AUCTION == "AUCTION"


def test_status_constants():
    assert Status.PENDING == "PENDING"
    assert Status.OPEN == "OPEN"
    assert Status.PARTIALLYFILLED == "PARTIALLYFILLED"
    assert Status.FILLED == "FILLED"
    assert Status.REJECTED == "REJECTED"
    assert Status.CANCELLED == "CANCELLED"
    assert Status.MODIFIED == "MODIFIED"


def test_order_constants():
    assert Order.ID == "id"
    assert Order.USERID == "userOrderId"
    assert Order.CLIENTID == "clientId"
    assert Order.TIMESTAMP == "timestamp"
    assert Order.SYMBOL == "symbol"
    assert Order.TOKEN == "token"
    assert Order.SIDE == "side"
    assert Order.TYPE == "type"
    assert Order.AVGPRICE == "avgPrice"
    assert Order.PRICE == "price"
    assert Order.TRIGGERPRICE == "triggerPrice"
    assert Order.TARGETPRICE == "targetPrice"
    assert Order.STOPLOSSPRICE == "stoplossPrice"
    assert Order.TRAILINGSTOPLOSS == "trailingStoploss"
    assert Order.QUANTITY == "quantity"
    assert Order.FILLEDQTY == "filled"
    assert Order.REMAININGQTY == "remaining"
    assert Order.CANCELLEDQTY == "cancelleldQty"
    assert Order.STATUS == "status"
    assert Order.REJECTREASON == "rejectReason"
    assert Order.DISCLOSEDQUANTITY == "disclosedQuantity"
    assert Order.PRODUCT == "product"
    assert Order.SEGMENT == "segment"
    assert Order.EXCHANGE == "exchange"
    assert Order.VALIDITY == "validity"
    assert Order.VARIETY == "variety"
    assert Order.INFO == "info"


def test_position_constants():
    assert Position.SYMBOL == "symbol"
    assert Position.TOKEN == "token"
    assert Position.NETQTY == "netQty"
    assert Position.AVGPRICE == "avgPrice"
    assert Position.MTM == "mtm"
    assert Position.PNL == "pnl"
    assert Position.BUYQTY == "buyQty"
    assert Position.BUYPRICE == "buyPrice"
    assert Position.SELLQTY == "sellQty"
    assert Position.SELLPRICE == "sellPrice"
    assert Position.LTP == "ltp"
    assert Position.PRODUCT == "product"
    assert Position.EXCHANGE == "exchange"
    assert Position.INFO == "info"


def test_profile_constants():
    assert Profile.CLIENTID == "clientId"
    assert Profile.NAME == "name"
    assert Profile.EMAILID == "emailId"
    assert Profile.MOBILENO == "mobileNo"
    assert Profile.PAN == "pan"
    assert Profile.ADDRESS == "address"
    assert Profile.BANKNAME == "bankName"
    assert Profile.BANKBRANCHNAME == "bankBranchName"
    assert Profile.BANKACCNO == "bankAccNo"
    assert Profile.EXHCNAGESENABLED == "exchangesEnabled"
    assert Profile.ENABLED == "enabled"
    assert Profile.INFO == "info"


def test_unique_id_constants():
    assert UniqueID.DEFORDER == "FenixOrder"
    assert UniqueID.MARKETORDER == "MarketOrder"
    assert UniqueID.LIMITORDER == "LIMITOrder"
    assert UniqueID.SLORDER == "SLOrder"
    assert UniqueID.SLMORDER == "SLMOrder"
