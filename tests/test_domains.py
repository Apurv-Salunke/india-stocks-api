"""
Tests for instrument domain classes
"""
import pytest
from datetime import date
from india_stocks_api.instruments.domains import (
    SecType,
    Instrument,
    Equity,
    Index,
    Future,
    Option,
    OptionType
)


class TestSecType:
    """Test cases for SecType enum"""

    def test_sectype_values(self):
        """Test that SecType has correct values"""
        assert SecType.EQ == "EQ"
        assert SecType.FUT == "FUT"
        assert SecType.OPT == "OPT"
        assert SecType.IDX == "IDX"

    def test_sectype_is_string_enum(self):
        """Test that SecType values are strings"""
        assert isinstance(SecType.EQ, str)
        assert isinstance(SecType.IDX, str)


class TestInstrument:
    """Test cases for Instrument abstract base class"""

    def test_instrument_is_abstract(self):
        """Test that Instrument cannot be instantiated directly"""
        with pytest.raises(TypeError):
            Instrument()

    def test_instrument_has_attributes(self):
        """Test that Instrument subclasses have expected attributes"""
        equity = Equity("RELIANCE")
        assert hasattr(equity, 'symbol')
        assert hasattr(equity, 'exchange')
        assert hasattr(equity, 'currency')


class TestEquity:
    """Test cases for Equity class"""

    def test_equity_creation_default_exchange(self):
        """Test Equity creation with default exchange"""
        equity = Equity("RELIANCE")
        assert equity.symbol == "RELIANCE"
        assert equity.exchange == "NSE"
        assert equity.sec_type == SecType.EQ
        assert equity.currency == "INR"

    def test_equity_creation_custom_exchange(self):
        """Test Equity creation with custom exchange"""
        equity = Equity("RELIANCE", "BSE")
        assert equity.symbol == "RELIANCE"
        assert equity.exchange == "BSE"
        assert equity.sec_type == SecType.EQ

    def test_equity_is_instrument(self):
        """Test that Equity is an instance of Instrument"""
        equity = Equity("RELIANCE")
        assert isinstance(equity, Instrument)


class TestIndex:
    """Test cases for Index class"""

    def test_index_creation_default_exchange(self):
        """Test Index creation with default exchange"""
        index = Index("NIFTY 50")
        assert index.symbol == "NIFTY 50"
        assert index.exchange == "NSE"
        assert index.sec_type == SecType.IDX
        assert index.currency == "INR"

    def test_index_creation_custom_exchange(self):
        """Test Index creation with custom exchange"""
        index = Index("SENSEX", "BSE")
        assert index.symbol == "SENSEX"
        assert index.exchange == "BSE"
        assert index.sec_type == SecType.IDX

    def test_index_is_instrument(self):
        """Test that Index is an instance of Instrument"""
        index = Index("NIFTY 50")
        assert isinstance(index, Instrument)


class TestFuture:
    """Test cases for Future class"""

    def test_future_creation_default_exchange(self):
        """Test Future creation with default exchange"""
        expiry = date(2024, 12, 31)
        future = Future("NIFTY", expiry)
        assert future.symbol == "NIFTY"
        assert future.expiry == expiry
        assert future.exchange == "NFO"
        assert future.sec_type == SecType.FUT
        assert future.currency == "INR"

    def test_future_creation_custom_exchange(self):
        """Test Future creation with custom exchange"""
        expiry = date(2024, 12, 31)
        future = Future("NIFTY", expiry, "MCX")
        assert future.symbol == "NIFTY"
        assert future.expiry == expiry
        assert future.exchange == "MCX"
        assert future.sec_type == SecType.FUT

    def test_future_is_instrument(self):
        """Test that Future is an instance of Instrument"""
        expiry = date(2024, 12, 31)
        future = Future("NIFTY", expiry)
        assert isinstance(future, Instrument)

    def test_future_expiry_date(self):
        """Test Future expiry date handling"""
        expiry = date(2024, 6, 15)
        future = Future("BANKNIFTY", expiry)
        assert future.expiry.year == 2024
        assert future.expiry.month == 6
        assert future.expiry.day == 15


class TestOptionType:
    """Test cases for OptionType enum"""

    def test_option_type_values(self):
        """Test that OptionType has correct values"""
        assert OptionType.CE == "CE"
        assert OptionType.PE == "PE"

    def test_option_type_is_string_enum(self):
        """Test that OptionType values are strings"""
        assert isinstance(OptionType.CE, str)
        assert isinstance(OptionType.PE, str)


class TestOption:
    """Test cases for Option class"""

    def test_option_creation_call_default_exchange(self):
        """Test Option creation for Call option with default exchange"""
        expiry = date(2024, 12, 31)
        option = Option("BANKNIFTY", expiry, 50000, OptionType.CE)
        assert option.symbol == "BANKNIFTY"
        assert option.expiry == expiry
        assert option.strike == 50000
        assert option.opt_type == OptionType.CE
        assert option.exchange == "NFO"
        assert option.sec_type == SecType.OPT
        assert option.currency == "INR"

    def test_option_creation_put_default_exchange(self):
        """Test Option creation for Put option with default exchange"""
        expiry = date(2024, 12, 31)
        option = Option("BANKNIFTY", expiry, 50000, OptionType.PE)
        assert option.symbol == "BANKNIFTY"
        assert option.expiry == expiry
        assert option.strike == 50000
        assert option.opt_type == OptionType.PE
        assert option.exchange == "NFO"
        assert option.sec_type == SecType.OPT

    def test_option_creation_custom_exchange(self):
        """Test Option creation with custom exchange"""
        expiry = date(2024, 12, 31)
        option = Option("NIFTY", expiry, 20000, OptionType.CE, "MCX")
        assert option.symbol == "NIFTY"
        assert option.exchange == "MCX"
        assert option.strike == 20000
        assert option.opt_type == OptionType.CE

    def test_option_is_instrument(self):
        """Test that Option is an instance of Instrument"""
        expiry = date(2024, 12, 31)
        option = Option("BANKNIFTY", expiry, 50000, OptionType.CE)
        assert isinstance(option, Instrument)

    def test_option_strike_price(self):
        """Test Option strike price handling"""
        expiry = date(2024, 12, 31)
        option = Option("NIFTY", expiry, 25000.50, OptionType.CE)
        assert option.strike == 25000.50
        assert isinstance(option.strike, float)
