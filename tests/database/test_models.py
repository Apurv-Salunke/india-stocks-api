"""
Test cases for database models
"""

import pytest
from datetime import date
from india_stocks_api.database.models import (
    Instrument,
    BrokerInstrument,
    ExchangeModel,
    Exchange,
    InstrumentCategory,
    OptionType,
    CommodityType,
)


# BaseModel tests removed since we're not using inheritance anymore


class TestExchangeModel:
    """Test ExchangeModel functionality"""

    def test_exchange_model_creation(self):
        """Test exchange model creation"""
        exchange = ExchangeModel(
            exchange_code="NSE", exchange_name="National Stock Exchange"
        )

        assert exchange.exchange_code == "NSE"
        assert exchange.exchange_name == "National Stock Exchange"
        assert exchange.country == "INDIA"
        assert exchange.currency == "INR"
        assert exchange.timezone == "Asia/Kolkata"
        assert exchange.is_active is True

    def test_exchange_model_validation(self):
        """Test exchange model validation"""
        with pytest.raises(ValueError, match="Exchange code is required"):
            ExchangeModel(exchange_code="", exchange_name="Test")

        with pytest.raises(ValueError, match="Exchange name is required"):
            ExchangeModel(exchange_code="NSE", exchange_name="")


class TestInstrument:
    """Test Instrument model functionality"""

    def test_instrument_creation(self):
        """Test instrument model creation"""
        instrument = Instrument(
            standardized_symbol="RELIANCE",
            instrument_name="Reliance Industries Ltd",
            exchange_id=1,
            category_id=1,
        )

        assert instrument.standardized_symbol == "RELIANCE"
        assert instrument.instrument_name == "Reliance Industries Ltd"
        assert instrument.exchange_id == 1
        assert instrument.category_id == 1
        assert instrument.is_active is True

    def test_instrument_validation(self):
        """Test instrument model validation"""
        with pytest.raises(ValueError, match="Standardized symbol is required"):
            Instrument(
                standardized_symbol="",
                instrument_name="Test",
                exchange_id=1,
                category_id=1,
            )

        with pytest.raises(ValueError, match="Instrument name is required"):
            Instrument(
                standardized_symbol="TEST",
                instrument_name="",
                exchange_id=1,
                category_id=1,
            )

    def test_instrument_type_detection(self):
        """Test instrument type detection"""
        # Equity instrument
        equity = Instrument(
            standardized_symbol="RELIANCE",
            instrument_name="Reliance Industries Ltd",
            exchange_id=1,
            category_id=1,  # EQUITY
        )
        assert equity.get_instrument_type() == "EQUITY"
        assert equity.is_equity() is True
        assert equity.is_derivative() is False

        # Future instrument
        future = Instrument(
            standardized_symbol="NIFTY",
            instrument_name="Nifty 50 Future",
            exchange_id=1,
            category_id=2,  # FUTURES
            underlying_type="INDEX",
        )
        assert future.get_instrument_type() == "INDEX_FUTURE"
        assert future.is_derivative() is True

    def test_commodity_instrument(self):
        """Test commodity instrument"""
        commodity = Instrument(
            standardized_symbol="GOLD",
            instrument_name="Gold",
            exchange_id=3,  # MCX
            category_id=4,  # COMMODITY
            commodity_type="PRECIOUS_METALS",
            commodity_unit="KG",
        )

        assert commodity.is_commodity() is True
        assert commodity.commodity_type == "PRECIOUS_METALS"
        assert commodity.commodity_unit == "KG"


class TestBrokerInstrument:
    """Test BrokerInstrument model functionality"""

    def test_broker_instrument_creation(self):
        """Test broker instrument creation"""
        broker_instrument = BrokerInstrument(
            instrument_id=1,
            broker_name="angelone",
            broker_symbol="RELIANCE-EQ",
            broker_token="2881",
            tick_size=0.05,
            lot_size=1,
        )

        assert broker_instrument.instrument_id == 1
        assert broker_instrument.broker_name == "angelone"
        assert broker_instrument.broker_symbol == "RELIANCE-EQ"
        assert broker_instrument.broker_token == "2881"
        assert broker_instrument.tick_size == 0.05
        assert broker_instrument.lot_size == 1
        assert broker_instrument.is_tradeable is True
        assert broker_instrument.is_active is True

    def test_broker_instrument_validation(self):
        """Test broker instrument validation"""
        with pytest.raises(ValueError, match="Instrument ID is required"):
            BrokerInstrument(
                instrument_id=0,
                broker_name="angelone",
                broker_symbol="RELIANCE-EQ",
                broker_token="2881",
                tick_size=0.05,
                lot_size=1,
            )

        with pytest.raises(ValueError, match="Tick size must be positive"):
            BrokerInstrument(
                instrument_id=1,
                broker_name="angelone",
                broker_symbol="RELIANCE-EQ",
                broker_token="2881",
                tick_size=-0.05,
                lot_size=1,
            )

    def test_option_instrument(self):
        """Test option instrument"""
        option = BrokerInstrument(
            instrument_id=1,
            broker_name="angelone",
            broker_symbol="BANKNIFTY2432845000CE",
            broker_token="43048",
            tick_size=0.05,
            lot_size=25,
            expiry_date=date(2024, 3, 28),
            strike_price=45000,
            option_type="CE",
        )

        assert option.is_option() is True
        assert option.is_call_option() is True
        assert option.is_put_option() is False
        assert option.is_future() is False

    def test_future_instrument(self):
        """Test future instrument"""
        future = BrokerInstrument(
            instrument_id=1,
            broker_name="angelone",
            broker_symbol="NIFTY24328",
            broker_token="43047",
            tick_size=0.05,
            lot_size=50,
            expiry_date=date(2024, 3, 28),
        )

        assert future.is_future() is True
        assert future.is_option() is False


class TestEnums:
    """Test enum classes"""

    def test_exchange_enum(self):
        """Test Exchange enum"""
        assert Exchange.NSE.value == "NSE"
        assert Exchange.BSE.value == "BSE"
        assert Exchange.MCX.value == "MCX"

    def test_instrument_category_enum(self):
        """Test InstrumentCategory enum"""
        assert InstrumentCategory.EQUITY.value == "EQ"
        assert InstrumentCategory.FUTURES.value == "FUT"
        assert InstrumentCategory.OPTIONS.value == "OPT"
        assert InstrumentCategory.COMMODITY.value == "COM"
        assert InstrumentCategory.CURRENCY.value == "CUR"

    def test_option_type_enum(self):
        """Test OptionType enum"""
        assert OptionType.CALL.value == "CE"
        assert OptionType.PUT.value == "PE"

    def test_commodity_type_enum(self):
        """Test CommodityType enum"""
        assert CommodityType.METALS.value == "METALS"
        assert CommodityType.ENERGY.value == "ENERGY"
        assert CommodityType.AGRICULTURE.value == "AGRICULTURE"
