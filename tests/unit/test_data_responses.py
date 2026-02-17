"""Unit tests for canonical response objects from AngelOne data APIs."""

from __future__ import annotations

import pandas as pd
import pytest

from india_stocks_api.brokers.angel import AngelOne
from india_stocks_api.constants import CandleInterval
from india_stocks_api.instruments import Equity
from india_stocks_api.models import DepthResponse, FundsResponse, HistoryResponse, ProfileResponse, QuoteResponse


@pytest.fixture(autouse=True)
def _skip_instruments(mocker):
    mocker.patch.object(AngelOne, "_ensure_instruments_ready")


@pytest.fixture
def broker(dummy_creds, mocker):
    b = AngelOne(**dummy_creds)
    mocker.patch.object(b, "_require_auth", return_value="jwt_token_xxx")
    mocker.patch.object(b, "_resolve_instrument", return_value={"symbol": "RELIANCE", "exchange": "NSE"})
    return b


def test_get_quote_returns_canonical_object(broker, mocker):
    class FakeBrokerData:
        def __init__(self, auth):
            self.auth = auth

        def get_quotes(self, symbol, exchange):
            return {
                "bid": 2501.1,
                "ask": 2501.4,
                "open": 2490.0,
                "high": 2510.0,
                "low": 2485.0,
                "ltp": 2500.0,
                "prev_close": 2480.0,
                "volume": 12345,
                "oi": 987,
            }

    mocker.patch("india_stocks_api.brokers.angel.BrokerData", FakeBrokerData)
    resp = broker.get_quote(Equity("RELIANCE"))

    assert isinstance(resp, QuoteResponse)
    assert resp.ltp == 2500.0
    assert resp.volume == 12345


def test_get_depth_returns_canonical_object(broker, mocker):
    class FakeBrokerData:
        def __init__(self, auth):
            self.auth = auth

        def get_depth(self, symbol, exchange):
            return {
                "bids": [{"price": 100.5, "quantity": 10}] * 5,
                "asks": [{"price": 101.0, "quantity": 8}] * 5,
                "high": 110,
                "low": 95,
                "ltp": 100.8,
                "ltq": 20,
                "open": 99,
                "prev_close": 98,
                "volume": 2000,
                "oi": 150,
                "totalbuyqty": 10000,
                "totalsellqty": 12000,
            }

    mocker.patch("india_stocks_api.brokers.angel.BrokerData", FakeBrokerData)
    resp = broker.get_depth(Equity("RELIANCE"))

    assert isinstance(resp, DepthResponse)
    assert len(resp.bids) == 5
    assert resp.total_buy_qty == 10000


def test_get_history_returns_canonical_object(broker, mocker):
    class FakeBrokerData:
        def __init__(self, auth):
            self.auth = auth

        def get_history(self, **kwargs):
            return pd.DataFrame(
                [
                    {"timestamp": 1710000000, "open": 10, "high": 12, "low": 9, "close": 11, "volume": 100, "oi": 5},
                    {"timestamp": 1710000900, "open": 11, "high": 13, "low": 10, "close": 12, "volume": 120, "oi": 6},
                ]
            )

    mocker.patch("india_stocks_api.brokers.angel.BrokerData", FakeBrokerData)
    resp = broker.get_history(
        Equity("RELIANCE"), start_date="2025-01-01", end_date="2025-01-02", interval=CandleInterval.FIFTEEN_MINUTE
    )

    assert isinstance(resp, HistoryResponse)
    assert len(resp.candles) == 2
    df = resp.to_dataframe()
    assert list(df.columns) == ["timestamp", "open", "high", "low", "close", "volume", "oi"]


def test_get_funds_returns_canonical_object(broker, mocker):
    mocker.patch(
        "india_stocks_api.brokers.angel.get_margin_data",
        return_value={
            "availablecash": "1000.50",
            "collateral": "250.25",
            "m2mrealized": "10.00",
            "m2munrealized": "-5.00",
            "utiliseddebits": "100.00",
        },
    )

    resp = broker.get_funds()
    assert isinstance(resp, FundsResponse)
    assert resp.available_cash == 1000.5
    assert resp.collateral == 250.25


def test_get_profile_returns_canonical_object(broker, mocker):
    mocker.patch(
        "india_stocks_api.brokers.angel.get_profile_api",
        return_value={
            "status": True,
            "data": {
                "clientcode": "A12345",
                "name": "Test User",
                "exchanges": ["NSE", "NFO"],
                "products": ["CNC", "MIS"],
                "email": "x@example.com",
                "mobileno": "9999999999",
            },
        },
    )

    resp = broker.get_profile()
    assert isinstance(resp, ProfileResponse)
    assert resp.client_code == "A12345"
    assert resp.exchanges == ("NSE", "NFO")
