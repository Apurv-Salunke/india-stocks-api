"""Live integration tests for AngelOne data-related methods.

Requires env vars:
- ANGEL_API_KEY
- ANGEL_CLIENT_ID
- ANGEL_PIN
- ANGEL_TOTP_SECRET
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import pytest

from india_stocks_api.brokers.angel import AngelOne
from india_stocks_api.constants import CandleInterval
from india_stocks_api.instruments import Equity
from india_stocks_api.models import DepthResponse, FundsResponse, HistoryResponse, ProfileResponse, QuoteResponse


def _get_creds():
    return {
        "api_key": os.getenv("ANGEL_API_KEY", ""),
        "client_code": os.getenv("ANGEL_CLIENT_ID", ""),
        "password": os.getenv("ANGEL_PIN", ""),
        "totp_key": os.getenv("ANGEL_TOTP_SECRET", ""),
    }


_missing = not all(_get_creds().values())


@pytest.fixture(scope="module")
def live_broker():
    broker = AngelOne(**_get_creds())
    broker.authenticate()
    return broker


@pytest.mark.integration
@pytest.mark.skipif(_missing, reason="Live Angel credentials not set in env")
class TestAngelDataMethodsLive:
    def test_get_quote_shape(self, live_broker):
        quote = live_broker.get_quote(Equity("RELIANCE"))
        assert isinstance(quote, QuoteResponse)
        assert quote.ltp >= 0

    def test_get_depth_shape(self, live_broker):
        depth = live_broker.get_depth(Equity("RELIANCE"))
        assert isinstance(depth, DepthResponse)
        assert len(depth.bids) == 5
        assert len(depth.asks) == 5

    def test_get_history_shape(self, live_broker):
        end = date.today()
        start = end - timedelta(days=5)
        history = live_broker.get_history(
            Equity("RELIANCE"),
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            interval=CandleInterval.FIFTEEN_MINUTE,
        )
        assert isinstance(history, HistoryResponse)
        df = history.to_dataframe()
        assert isinstance(df, pd.DataFrame)
        expected_cols = {"timestamp", "open", "high", "low", "close", "volume", "oi"}
        assert expected_cols.issubset(set(df.columns))

    def test_account_data_methods_return_consistent_container_types(self, live_broker):
        funds = live_broker.get_funds()
        profile = live_broker.get_profile()
        holdings = live_broker.get_holdings()
        orders = live_broker.get_orders()
        trades = live_broker.get_trades()
        positions = live_broker.get_positions()

        assert isinstance(funds, FundsResponse)
        assert isinstance(profile, ProfileResponse)
        assert isinstance(holdings, list)
        assert isinstance(orders, list)
        assert isinstance(trades, list)
        assert isinstance(positions, list)

    def test_get_order_details_shape_when_orders_exist(self, live_broker):
        orders = live_broker.get_orders()
        if not orders:
            pytest.skip("No orders available for order-details validation.")

        first_order_id = orders[0].get("orderid")
        if not first_order_id:
            pytest.skip("Order id missing in first order payload.")

        details = live_broker.get_order_details(first_order_id)
        assert isinstance(details, dict)
