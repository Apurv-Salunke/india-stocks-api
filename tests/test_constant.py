"""
Tests for constant enums
"""
from india_stocks_api.constant import (
    OrderType,
    TransactionType,
    ProductType,
    OrderValidity,
    TimeFrame
)


class TestOrderType:
    """Test cases for OrderType enum"""

    def test_order_type_values(self):
        """Test that OrderType has correct values"""
        assert OrderType.MARKET == "MARKET"
        assert OrderType.LIMIT == "LIMIT"
        assert OrderType.SL == "SL"
        assert OrderType.SLM == "SL-M"

    def test_order_type_is_string_enum(self):
        """Test that OrderType values are strings"""
        assert isinstance(OrderType.MARKET, str)
        assert isinstance(OrderType.LIMIT, str)

    def test_order_type_comparison(self):
        """Test OrderType comparison"""
        assert OrderType.MARKET == "MARKET"
        assert OrderType.LIMIT != "MARKET"


class TestTransactionType:
    """Test cases for TransactionType enum"""

    def test_transaction_type_values(self):
        """Test that TransactionType has correct values"""
        assert TransactionType.BUY == "BUY"
        assert TransactionType.SELL == "SELL"

    def test_transaction_type_is_string_enum(self):
        """Test that TransactionType values are strings"""
        assert isinstance(TransactionType.BUY, str)
        assert isinstance(TransactionType.SELL, str)


class TestProductType:
    """Test cases for ProductType enum"""

    def test_product_type_values(self):
        """Test that ProductType has correct values"""
        assert ProductType.INTRADAY == "MIS"
        assert ProductType.DELIVERY == "CNC"
        assert ProductType.CARRYFORWARD == "NRML"

    def test_product_type_is_string_enum(self):
        """Test that ProductType values are strings"""
        assert isinstance(ProductType.INTRADAY, str)
        assert isinstance(ProductType.DELIVERY, str)
        assert isinstance(ProductType.CARRYFORWARD, str)


class TestOrderValidity:
    """Test cases for OrderValidity enum"""

    def test_order_validity_values(self):
        """Test that OrderValidity has correct values"""
        assert OrderValidity.DAY == "DAY"
        assert OrderValidity.IOC == "IOC"

    def test_order_validity_is_string_enum(self):
        """Test that OrderValidity values are strings"""
        assert isinstance(OrderValidity.DAY, str)
        assert isinstance(OrderValidity.IOC, str)


class TestTimeFrame:
    """Test cases for TimeFrame enum"""

    def test_timeframe_values(self):
        """Test that TimeFrame has correct values"""
        assert TimeFrame.MIN_1 == "ONE_MINUTE"
        assert TimeFrame.MIN_3 == "THREE_MINUTE"
        assert TimeFrame.MIN_5 == "FIVE_MINUTE"
        assert TimeFrame.MIN_10 == "TEN_MINUTE"
        assert TimeFrame.MIN_15 == "FIFTEEN_MINUTE"
        assert TimeFrame.MIN_30 == "THIRTY_MINUTE"
        assert TimeFrame.MIN_60 == "ONE_HOUR"
        assert TimeFrame.DAY == "ONE_DAY"

    def test_timeframe_is_string_enum(self):
        """Test that TimeFrame values are strings"""
        assert isinstance(TimeFrame.MIN_1, str)
        assert isinstance(TimeFrame.DAY, str)

    def test_timeframe_ordering(self):
        """Test TimeFrame enum members"""
        timeframes = [
            TimeFrame.MIN_1,
            TimeFrame.MIN_3,
            TimeFrame.MIN_5,
            TimeFrame.MIN_10,
            TimeFrame.MIN_15,
            TimeFrame.MIN_30,
            TimeFrame.MIN_60,
            TimeFrame.DAY
        ]
        assert len(timeframes) == 8
