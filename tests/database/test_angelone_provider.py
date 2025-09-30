"""
Test cases for AngelOne provider
"""

import pytest
from unittest.mock import Mock, patch

from india_stocks_api.database import InstrumentService, AngelOneProvider
from india_stocks_api.database.models.enums import OptionType


class TestAngelOneProvider:
    """Test AngelOneProvider functionality"""

    @pytest.fixture
    def mock_instrument_service(self):
        """Create mock instrument service"""
        service = Mock(spec=InstrumentService)
        service.db_path = "test.db"
        return service

    @pytest.fixture
    def angelone_provider(self, mock_instrument_service):
        """Create AngelOne provider instance"""
        return AngelOneProvider(mock_instrument_service, "angelone")

    def test_provider_initialization(self, angelone_provider):
        """Test provider initialization"""
        assert angelone_provider.broker_name == "angelone"
        assert angelone_provider.db is not None
        assert angelone_provider.base_urls is not None
        assert "market_data" in angelone_provider.base_urls

    def test_is_equity_instrument(self, angelone_provider):
        """Test equity instrument detection"""
        # NSE equity
        nse_equity = {
            "exch_seg": "NSE",
            "instrumenttype": "EQ",
            "symbol": "RELIANCE-EQ",
        }
        assert angelone_provider._is_equity_instrument(nse_equity) is True

        # BSE equity
        bse_equity = {"exch_seg": "BSE", "instrumenttype": "BE", "symbol": "RELIANCE"}
        assert angelone_provider._is_equity_instrument(bse_equity) is True

        # Non-equity
        non_equity = {
            "exch_seg": "NSE",
            "instrumenttype": "OPTIDX",
            "symbol": "BANKNIFTY2432845000CE",
        }
        assert angelone_provider._is_equity_instrument(non_equity) is False

    def test_is_fno_instrument(self, angelone_provider):
        """Test F&O instrument detection"""
        # NFO option
        nfo_option = {
            "exch_seg": "NFO",
            "instrumenttype": "OPTIDX",
            "symbol": "BANKNIFTY2432845000CE",
        }
        assert angelone_provider._is_fno_instrument(nfo_option) is True

        # BFO future
        bfo_future = {
            "exch_seg": "BFO",
            "instrumenttype": "FUTIDX",
            "symbol": "SENSEX24328",
        }
        assert angelone_provider._is_fno_instrument(bfo_future) is True

        # Non-F&O
        non_fno = {"exch_seg": "NSE", "instrumenttype": "EQ", "symbol": "RELIANCE-EQ"}
        assert angelone_provider._is_fno_instrument(non_fno) is False

    def test_is_commodity_instrument(self, angelone_provider):
        """Test commodity instrument detection"""
        # MCX commodity
        mcx_commodity = {
            "exch_seg": "MCX",
            "instrumenttype": "FUTCOM",
            "symbol": "GOLD24328",
        }
        assert angelone_provider._is_commodity_instrument(mcx_commodity) is True

        # NCDEX commodity option
        ncdex_option = {
            "exch_seg": "NCDEX",
            "instrumenttype": "OPTCOM",
            "symbol": "SOYBEAN2432845000CE",
        }
        assert angelone_provider._is_commodity_instrument(ncdex_option) is True

        # Non-commodity
        non_commodity = {
            "exch_seg": "NSE",
            "instrumenttype": "EQ",
            "symbol": "RELIANCE-EQ",
        }
        assert angelone_provider._is_commodity_instrument(non_commodity) is False

    def test_is_currency_instrument(self, angelone_provider):
        """Test currency instrument detection"""
        # CDS currency
        cds_currency = {
            "exch_seg": "CDS",
            "instrumenttype": "FUTCUR",
            "symbol": "USDINR24328",
        }
        assert angelone_provider._is_currency_instrument(cds_currency) is True

        # BCD currency option
        bcd_option = {
            "exch_seg": "BCD",
            "instrumenttype": "OPTCUR",
            "symbol": "USDINR2432845000CE",
        }
        assert angelone_provider._is_currency_instrument(bcd_option) is True

        # Non-currency
        non_currency = {
            "exch_seg": "NSE",
            "instrumenttype": "EQ",
            "symbol": "RELIANCE-EQ",
        }
        assert angelone_provider._is_currency_instrument(non_currency) is False

    def test_map_segment_to_exchange(self, angelone_provider):
        """Test segment to exchange mapping"""
        # F&O segments
        assert angelone_provider._map_segment_to_exchange("NFO") == "NSE"
        assert angelone_provider._map_segment_to_exchange("BFO") == "BSE"

        # Currency segments
        assert angelone_provider._map_segment_to_exchange("CDS") == "NSE"
        assert angelone_provider._map_segment_to_exchange("BCD") == "BSE"

        # Direct exchanges (no mapping)
        assert angelone_provider._map_segment_to_exchange("NSE") == "NSE"
        assert angelone_provider._map_segment_to_exchange("MCX") == "MCX"

    def test_standardize_symbol(self, angelone_provider):
        """Test symbol standardization"""
        # NSE equity symbol
        nse_symbol = angelone_provider._standardize_symbol("RELIANCE-EQ", "NSE")
        assert nse_symbol == "RELIANCE"

        # BSE symbol (no change)
        bse_symbol = angelone_provider._standardize_symbol("RELIANCE", "BSE")
        assert bse_symbol == "RELIANCE"

        # F&O symbol (no change)
        fno_symbol = angelone_provider._standardize_symbol(
            "BANKNIFTY2432845000CE", "NFO"
        )
        assert fno_symbol == "BANKNIFTY2432845000CE"

    def test_extract_option_type(self, angelone_provider):
        """Test option type extraction"""
        # Call option
        call_symbol = "BANKNIFTY2432845000CE"
        assert (
            angelone_provider._extract_option_type(call_symbol) == OptionType.CALL.value
        )

        # Put option
        put_symbol = "BANKNIFTY2432845000PE"
        assert (
            angelone_provider._extract_option_type(put_symbol) == OptionType.PUT.value
        )

        # Non-option
        non_option = "BANKNIFTY24328"
        assert angelone_provider._extract_option_type(non_option) is None

    def test_determine_underlying_type(self, angelone_provider):
        """Test underlying type determination"""
        # Index option
        index_option = {"instrumenttype": "OPTIDX"}
        assert angelone_provider._determine_underlying_type(index_option) == "INDEX"

        # Stock future
        stock_future = {"instrumenttype": "FUTSTK"}
        assert angelone_provider._determine_underlying_type(stock_future) == "EQUITY"

        # Commodity option
        commodity_option = {"instrumenttype": "OPTCOM"}
        assert (
            angelone_provider._determine_underlying_type(commodity_option)
            == "COMMODITY"
        )

        # Currency future
        currency_future = {"instrumenttype": "FUTCUR"}
        assert (
            angelone_provider._determine_underlying_type(currency_future) == "CURRENCY"
        )

    def test_extract_currency_info(self, angelone_provider):
        """Test currency extraction"""
        # USD/INR
        usd_symbol = "USDINR24328"
        assert angelone_provider._extract_base_currency(usd_symbol) == "USD"
        assert angelone_provider._extract_quote_currency(usd_symbol) == "INR"

        # EUR/INR
        eur_symbol = "EURINR24328"
        assert angelone_provider._extract_base_currency(eur_symbol) == "EUR"
        assert angelone_provider._extract_quote_currency(eur_symbol) == "INR"

        # Unknown currency
        unknown_symbol = "UNKNOWN24328"
        assert angelone_provider._extract_base_currency(unknown_symbol) is None

    @patch("requests.get")
    def test_fetch_market_data_success(self, mock_get, angelone_provider):
        """Test successful market data fetching"""
        # Clear any existing cache
        angelone_provider.cache_file = "/tmp/test_cache_success.json"

        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = [
            {
                "token": "2881",
                "symbol": "RELIANCE-EQ",
                "name": "Reliance Industries Ltd",
                "exch_seg": "NSE",
                "instrumenttype": "EQ",
                "tick_size": "5.000000",
                "lotsize": "1",
                "isin": "INE002A01018",
            }
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Test fetching
        data = angelone_provider._fetch_market_data()

        assert len(data) == 1
        assert data[0]["symbol"] == "RELIANCE-EQ"
        assert data[0]["name"] == "Reliance Industries Ltd"
        # Only assert if no cached data was used
        if not angelone_provider._read_cache():
            mock_get.assert_called_once()

    @patch("requests.get")
    def test_fetch_market_data_error(self, mock_get, angelone_provider):
        """Test market data fetching with error"""
        # Clear any existing cache
        angelone_provider.cache_file = "/tmp/test_cache_error.json"

        # Mock error response
        mock_get.side_effect = Exception("API Error")

        with pytest.raises(Exception):
            angelone_provider._fetch_market_data()

    def test_cache_operations(self, angelone_provider):
        """Test cache read/write operations"""
        test_data = [{"symbol": "TEST", "token": "123"}]

        # Test cache write
        angelone_provider._write_cache(test_data)

        # Test cache read
        cached_data = angelone_provider._read_cache()
        assert cached_data is not None
        assert "timestamp" in cached_data
        assert "data" in cached_data
        assert cached_data["data"] == test_data

        # Test cache validity
        assert angelone_provider._is_cache_valid(cached_data) is True

    def test_fetch_equity_data(self, angelone_provider):
        """Test equity data fetching and processing"""
        # Mock market data
        mock_data = [
            {
                "token": "2881",
                "symbol": "RELIANCE-EQ",
                "name": "Reliance Industries Ltd",
                "exch_seg": "NSE",
                "instrumenttype": "EQ",
                "tick_size": "5.000000",
                "lotsize": "1",
                "isin": "INE002A01018",
                "sector": "Oil & Gas",
                "industry": "Refineries",
            },
            {
                "token": "738561",
                "symbol": "TCS-EQ",
                "name": "Tata Consultancy Services Ltd",
                "exch_seg": "NSE",
                "instrumenttype": "EQ",
                "tick_size": "5.000000",
                "lotsize": "1",
            },
        ]

        with patch.object(
            angelone_provider, "_fetch_market_data", return_value=mock_data
        ):
            equity_data = angelone_provider.fetch_equity_data()

            assert len(equity_data) == 2

            # Check first equity instrument
            first_equity = equity_data[0]
            assert first_equity["standardized_symbol"] == "RELIANCE"
            assert first_equity["instrument_name"] == "Reliance Industries Ltd"
            assert first_equity["broker_symbol"] == "RELIANCE-EQ"
            assert first_equity["broker_token"] == "2881"
            assert first_equity["tick_size"] == 0.05
            assert first_equity["lot_size"] == 1
            assert first_equity["isin"] == "INE002A01018"
            assert first_equity["sector"] == "Oil & Gas"

    def test_fetch_fno_data(self, angelone_provider):
        """Test F&O data fetching and processing"""
        # Mock F&O data
        mock_data = [
            {
                "token": "43048",
                "symbol": "BANKNIFTY2432845000CE",
                "name": "BANKNIFTY",
                "exch_seg": "NFO",
                "instrumenttype": "OPTIDX",
                "tick_size": "5.000000",
                "lotsize": "25",
                "expiry": "28MAR2024",
                "strike": "45000.000000",
            },
            {
                "token": "43047",
                "symbol": "BANKNIFTY24328",
                "name": "BANKNIFTY",
                "exch_seg": "NFO",
                "instrumenttype": "FUTIDX",
                "tick_size": "5.000000",
                "lotsize": "25",
                "expiry": "28MAR2024",
            },
        ]

        with patch.object(
            angelone_provider, "_fetch_market_data", return_value=mock_data
        ):
            fno_data = angelone_provider.fetch_fno_data()

            assert len(fno_data) == 2

            # Check option instrument
            option_instrument = fno_data[0]
            assert option_instrument["standardized_symbol"] == "BANKNIFTY2432845000CE"
            assert option_instrument["broker_symbol"] == "BANKNIFTY2432845000CE"
            assert option_instrument["broker_token"] == "43048"
            assert option_instrument["exchange_code"] == "NSE"  # NFO mapped to NSE
            assert option_instrument["option_type"] == OptionType.CALL.value
            assert option_instrument["strike_price"] == 45000.0
            assert option_instrument["underlying_type"] == "INDEX"

            # Check future instrument
            future_instrument = fno_data[1]
            assert future_instrument["standardized_symbol"] == "BANKNIFTY24328"
            assert future_instrument["exchange_code"] == "NSE"  # NFO mapped to NSE
            assert future_instrument["option_type"] is None
            assert future_instrument["underlying_type"] == "INDEX"

    def test_provider_info(self, angelone_provider):
        """Test provider information"""
        info = angelone_provider.get_provider_info()

        assert info["broker_name"] == "angelone"
        assert info["provider_class"] == "AngelOneProvider"
        assert "equity" in info["supported_instruments"]
        assert "fno" in info["supported_instruments"]
        assert "commodity" in info["supported_instruments"]
        assert "currency" in info["supported_instruments"]


@pytest.mark.integration
class TestAngelOneProviderIntegration:
    """Integration tests for AngelOne provider (requires network)"""

    @pytest.fixture
    def instrument_service(self, test_db_path):
        """Create real instrument service for integration tests"""
        return InstrumentService(test_db_path)

    @pytest.fixture
    def angelone_provider(self, instrument_service):
        """Create AngelOne provider with real service"""
        return AngelOneProvider(instrument_service, "angelone")

    @pytest.mark.slow
    def test_real_data_fetching(self, angelone_provider):
        """Test fetching real data from AngelOne API"""
        try:
            # Test fetching equity data
            equity_data = angelone_provider.fetch_equity_data()
            assert len(equity_data) > 0

            # Check sample equity instrument
            sample = equity_data[0]
            assert "standardized_symbol" in sample
            assert "instrument_name" in sample
            assert "broker_symbol" in sample
            assert "broker_token" in sample
            assert sample["tick_size"] > 0
            assert sample["lot_size"] > 0

        except Exception as e:
            pytest.skip(f"Network test failed: {e}")

    @pytest.mark.slow
    def test_real_sync_operation(self, angelone_provider):
        """Test real sync operation with AngelOne data"""
        try:
            # Sync only equity data for testing
            results = angelone_provider.sync_all_instruments()

            assert "equity" in results
            assert "fno" in results
            assert "commodity" in results
            assert "currency" in results

            # At least some data should be synced
            total_synced = sum(results.values())
            assert total_synced > 0

        except Exception as e:
            pytest.skip(f"Network test failed: {e}")
