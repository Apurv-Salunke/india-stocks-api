import pytest
from unittest.mock import patch, mock_open
from datetime import datetime, timedelta
import json
from core.brokers.angel_one import AngelOne
from core.brokers.base import ExchangeCode
from core.brokers.base import TokenDownloadError


@pytest.fixture
def mock_token_data():
    return [
        {
            "token": "10412",
            "symbol": "AAPL-EQ",
            "name": "APPLE",
            "expiry": "",
            "strike": "-1.000000",
            "lotsize": "1",
            "instrumenttype": "",
            "exch_seg": "NSE",
            "tick_size": "5.000000",
        },
        {
            "token": "20412",
            "symbol": "MSFT",
            "name": "MICROSOFT",
            "expiry": "",
            "strike": "-1.000000",
            "lotsize": "1",
            "instrumenttype": "",
            "exch_seg": "BSE",
            "tick_size": "10.000000",
        },
        {
            "token": "30412",
            "symbol": "GOOGL-EQ",
            "name": "ALPHABET",
            "expiry": "",
            "strike": "-1.000000",
            "lotsize": "1",
            "instrumenttype": "",
            "exch_seg": "NSE",
            "tick_size": "5.000000",
        },
        {
            "token": "40412",
            "symbol": "AMZN",
            "name": "AMAZON",
            "expiry": "",
            "strike": "-1.000000",
            "lotsize": "1",
            "instrumenttype": "",
            "exch_seg": "BSE",
            "tick_size": "10.000000",
        },
    ]


@patch("core.brokers.angel_one.AngelOne.fetch")
@patch("core.brokers.angel_one.AngelOne._json_parser")
def test_fetch_tokens_no_cache(mock_json_parser, mock_fetch, mock_token_data):
    mock_fetch.return_value.cookies = {"cookie": "value"}
    mock_json_parser.return_value = mock_token_data

    with patch("builtins.open", mock_open()) as mock_file:
        with patch("os.path.exists", return_value=False):
            result = AngelOne._fetch_tokens()

    assert result == mock_token_data
    mock_fetch.assert_called_once()
    mock_file.assert_called_once_with(AngelOne._CACHE_FILE, "w")


@patch("core.brokers.angel_one.AngelOne.fetch")
@patch("core.brokers.angel_one.AngelOne._json_parser")
def test_fetch_tokens_with_valid_cache(mock_json_parser, mock_fetch, mock_token_data):
    cache_data = {"timestamp": datetime.now().isoformat(), "data": mock_token_data}

    with patch("builtins.open", mock_open(read_data=json.dumps(cache_data))):
        with patch("os.path.exists", return_value=True):
            result = AngelOne._fetch_tokens()

    assert result == mock_token_data
    mock_fetch.assert_not_called()
    mock_json_parser.assert_not_called()


@patch("core.brokers.angel_one.AngelOne.fetch")
@patch("core.brokers.angel_one.AngelOne._json_parser")
def test_fetch_tokens_with_expired_cache(mock_json_parser, mock_fetch, mock_token_data):
    expired_cache_data = {
        "timestamp": (datetime.now() - timedelta(days=2)).isoformat(),
        "data": [{"old": "data"}],
    }
    mock_fetch.return_value.cookies = {"cookie": "value"}
    mock_json_parser.return_value = mock_token_data

    with patch(
        "builtins.open", mock_open(read_data=json.dumps(expired_cache_data))
    ) as mock_file:
        with patch("os.path.exists", return_value=True):
            result = AngelOne._fetch_tokens()

    assert result == mock_token_data
    mock_fetch.assert_called_once()
    mock_file.assert_called_with(AngelOne._CACHE_FILE, "w")


@patch("core.brokers.angel_one.AngelOne._fetch_tokens")
def test_create_eq_tokens(mock_fetch_tokens, mock_token_data):
    mock_fetch_tokens.return_value = mock_token_data

    result = AngelOne.create_eq_tokens()

    assert ExchangeCode.NSE in result
    assert ExchangeCode.BSE in result

    # Check NSE data
    assert "APPLE" in result[ExchangeCode.NSE]
    assert result[ExchangeCode.NSE]["APPLE"]["Symbol"] == "AAPL-EQ"
    assert result[ExchangeCode.NSE]["APPLE"]["Token"] == 10412
    assert result[ExchangeCode.NSE]["APPLE"]["TickSize"] == 0.05
    assert result[ExchangeCode.NSE]["APPLE"]["LotSize"] == "1"
    assert result[ExchangeCode.NSE]["APPLE"]["Exchange"] == "NSE"

    assert "ALPHABET" in result[ExchangeCode.NSE]
    assert result[ExchangeCode.NSE]["ALPHABET"]["Symbol"] == "GOOGL-EQ"

    # Check BSE data
    assert "MSFT" in result[ExchangeCode.BSE]
    assert result[ExchangeCode.BSE]["MSFT"]["Token"] == 20412
    assert result[ExchangeCode.BSE]["MSFT"]["TickSize"] == 0.10
    assert result[ExchangeCode.BSE]["MSFT"]["LotSize"] == "1"
    assert result[ExchangeCode.BSE]["MSFT"]["Exchange"] == "BSE"

    assert "AMZN" in result[ExchangeCode.BSE]


@patch("core.brokers.angel_one.AngelOne._fetch_tokens")
def test_create_eq_tokens_empty_data(mock_fetch_tokens):
    mock_fetch_tokens.return_value = []

    with pytest.raises(TokenDownloadError, match="No data fetched from AngelOne API."):
        AngelOne.create_eq_tokens()


@patch("core.brokers.angel_one.AngelOne._fetch_tokens")
def test_create_eq_tokens_missing_tick_size(mock_fetch_tokens):
    mock_fetch_tokens.return_value = [
        {"token": "10412", "symbol": "AAPL-EQ", "name": "APPLE", "exch_seg": "NSE"}
    ]

    with pytest.raises(
        TokenDownloadError,
        match="Required 'tick_size' column not found in fetched data.",
    ):
        AngelOne.create_eq_tokens()


@patch("core.brokers.angel_one.AngelOne._fetch_tokens")
def test_create_eq_tokens_duplicate_symbols(mock_fetch_tokens):
    duplicate_data = [
        {
            "token": "10412",
            "symbol": "AAPL-EQ",
            "name": "APPLE",
            "expiry": "",
            "strike": "-1.000000",
            "lotsize": "1",
            "instrumenttype": "",
            "exch_seg": "NSE",
            "tick_size": "5.000000",
        },
        {
            "token": "10413",
            "symbol": "AAPL-EQ",
            "name": "APPLE",
            "expiry": "",
            "strike": "-1.000000",
            "lotsize": "1",
            "instrumenttype": "",
            "exch_seg": "NSE",
            "tick_size": "5.000000",
        },
    ]
    mock_fetch_tokens.return_value = duplicate_data

    result = AngelOne.create_eq_tokens()

    assert len(result[ExchangeCode.NSE]) == 1
    assert (
        result[ExchangeCode.NSE]["APPLE"]["Token"] == 10412
    )  # Should keep the first occurrence


@patch("core.brokers.angel_one.AngelOne._fetch_tokens")
def test_create_eq_tokens_tick_size_conversion(mock_fetch_tokens, mock_token_data):
    mock_fetch_tokens.return_value = mock_token_data

    result = AngelOne.create_eq_tokens()

    assert result[ExchangeCode.NSE]["APPLE"]["TickSize"] == 0.05
    assert result[ExchangeCode.BSE]["MSFT"]["TickSize"] == 0.10
