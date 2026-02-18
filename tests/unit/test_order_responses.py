"""Unit tests for canonical response objects from AngelOne order APIs."""

from __future__ import annotations

import pytest

from india_stocks_api.brokers.angel import AngelOne
from india_stocks_api.responses import Holding, Order, OrderResponse, Position, Trade


@pytest.fixture(autouse=True)
def _skip_instruments(mocker):
    mocker.patch.object(AngelOne, "_ensure_instruments_ready")


@pytest.fixture
def broker(dummy_creds, mocker):
    b = AngelOne(**dummy_creds)
    mocker.patch.object(b, "_require_auth", return_value="jwt_token_xxx")
    mocker.patch.object(b, "_resolve_instrument", return_value={"symbol": "RELIANCE", "exchange": "NSE"})
    return b


# --- place_order ---


def test_place_order_returns_order_response(broker, mocker):
    mocker.patch(
        "india_stocks_api.brokers.angel.place_order_api",
        return_value=("OK", {"message": "Order placed"}, "241219000001"),
    )
    resp = broker.place_order(
        instrument=mocker.MagicMock(),
        transaction_type=mocker.MagicMock(value="BUY"),
        quantity=1,
    )
    assert isinstance(resp, OrderResponse)
    assert resp.order_id == "241219000001"
    assert resp.status == "success"


def test_place_order_failure_returns_order_response(broker, mocker):
    mocker.patch(
        "india_stocks_api.brokers.angel.place_order_api",
        return_value=("FAIL", {"message": "Insufficient margin"}, None),
    )
    resp = broker.place_order(
        instrument=mocker.MagicMock(),
        transaction_type=mocker.MagicMock(value="BUY"),
        quantity=1,
    )
    assert isinstance(resp, OrderResponse)
    assert resp.order_id is None
    assert resp.status == "failed"


# --- get_orders ---


_SAMPLE_ORDER_RAW = {
    "orderid": "241219000001",
    "tradingsymbol": "RELIANCE-EQ",
    "exchange": "NSE",
    "transactiontype": "BUY",
    "ordertype": "MARKET",
    "producttype": "INTRADAY",
    "quantity": "10",
    "price": "0.00",
    "triggerprice": "0.00",
    "averageprice": "2500.50",
    "status": "complete",
    "updatetime": "2025-06-10 10:30:00",
}


def test_get_orders_returns_list_of_order(broker, mocker):
    mocker.patch(
        "india_stocks_api.brokers.angel.get_orders_api",
        return_value={"data": [_SAMPLE_ORDER_RAW]},
    )
    resp = broker.get_orders()
    assert isinstance(resp, list)
    assert len(resp) == 1
    assert isinstance(resp[0], Order)
    assert resp[0].order_id == "241219000001"
    assert resp[0].quantity == 10
    assert resp[0].average_price == 2500.50
    assert resp[0].raw == _SAMPLE_ORDER_RAW


def test_get_orders_empty_returns_empty_list(broker, mocker):
    mocker.patch(
        "india_stocks_api.brokers.angel.get_orders_api",
        return_value={"data": None},
    )
    resp = broker.get_orders()
    assert resp == []


# --- get_order_details ---


def test_get_order_details_returns_order(broker, mocker):
    mocker.patch(
        "india_stocks_api.brokers.angel.get_order_details_api",
        return_value={"data": _SAMPLE_ORDER_RAW},
    )
    resp = broker.get_order_details("241219000001")
    assert isinstance(resp, Order)
    assert resp.order_id == "241219000001"
    assert resp.status == "complete"


# --- modify_order ---


def test_modify_order_returns_order_response(broker, mocker):
    mocker.patch(
        "india_stocks_api.brokers.angel.get_orders_api",
        return_value={"data": [_SAMPLE_ORDER_RAW]},
    )
    mocker.patch(
        "india_stocks_api.brokers.angel.modify_order_api",
        return_value=({"message": "Order modified"}, 200),
    )
    resp = broker.modify_order("241219000001", price=2510.0)
    assert isinstance(resp, OrderResponse)
    assert resp.order_id == "241219000001"
    assert resp.status == "success"


# --- cancel_order ---


def test_cancel_order_returns_order_response(broker, mocker):
    mocker.patch(
        "india_stocks_api.brokers.angel.cancel_order_api",
        return_value=({"message": "Order cancelled"}, 200),
    )
    resp = broker.cancel_order("241219000001")
    assert isinstance(resp, OrderResponse)
    assert resp.status == "success"
    assert resp.order_id == "241219000001"


# --- get_positions ---


def test_get_positions_returns_list_of_position(broker, mocker):
    mocker.patch(
        "india_stocks_api.brokers.angel.get_positions",
        return_value={
            "data": [
                {
                    "tradingsymbol": "RELIANCE-EQ",
                    "exchange": "NSE",
                    "producttype": "INTRADAY",
                    "buyqty": "10",
                    "sellqty": "0",
                    "netprice": "2500.00",
                    "ltp": "2510.00",
                    "pnl": "100.00",
                }
            ]
        },
    )
    resp = broker.get_positions()
    assert isinstance(resp, list)
    assert len(resp) == 1
    assert isinstance(resp[0], Position)
    assert resp[0].symbol == "RELIANCE-EQ"
    assert resp[0].quantity == 10


# --- get_holdings ---


def test_get_holdings_returns_list_of_holding(broker, mocker):
    mocker.patch(
        "india_stocks_api.brokers.angel.get_holdings_api",
        return_value={
            "data": [
                {
                    "tradingsymbol": "RELIANCE-EQ",
                    "exchange": "NSE",
                    "quantity": "5",
                    "averageprice": "2000.00",
                    "ltp": "2500.00",
                }
            ]
        },
    )
    resp = broker.get_holdings()
    assert isinstance(resp, list)
    assert len(resp) == 1
    assert isinstance(resp[0], Holding)
    assert resp[0].symbol == "RELIANCE-EQ"
    assert resp[0].quantity == 5
    assert resp[0].pnl == 2500.0  # (2500-2000)*5
    assert resp[0].pnl_percent == 25.0


# --- get_trades ---


def test_get_trades_returns_list_of_trade(broker, mocker):
    mocker.patch(
        "india_stocks_api.brokers.angel.get_trades_api",
        return_value={
            "data": [
                {
                    "orderid": "241219000001",
                    "tradingsymbol": "RELIANCE-EQ",
                    "exchange": "NSE",
                    "transactiontype": "BUY",
                    "fillquantity": "10",
                    "fillprice": "2500.00",
                    "filltime": "2025-06-10 10:30:00",
                }
            ]
        },
    )
    resp = broker.get_trades()
    assert isinstance(resp, list)
    assert len(resp) == 1
    assert isinstance(resp[0], Trade)
    assert resp[0].order_id == "241219000001"
    assert resp[0].quantity == 10
    assert resp[0].trade_value == 25000.0
