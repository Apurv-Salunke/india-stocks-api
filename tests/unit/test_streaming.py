"""Unit tests for websocket streaming internals and AngelOne wrapper behavior."""

import json
from types import SimpleNamespace

import pytest

from india_stocks_api.brokers.angel import AngelOne
from india_stocks_api.constants import StreamMode
from india_stocks_api.instruments import Equity
from india_stocks_api.internal import context
from india_stocks_api.internal.angel.streaming.smartWebSocketV2 import SmartWebSocketV2
from india_stocks_api.responses import WebSocketTick


@pytest.fixture(autouse=True)
def _skip_instruments(mocker):
    mocker.patch.object(AngelOne, "_ensure_instruments_ready")


@pytest.fixture(autouse=True)
def _disable_file_logging(mocker):
    mocker.patch("india_stocks_api.internal.angel.streaming.smartWebSocketV2.logzero.logfile")


class DummyWS:
    def __init__(self):
        self.sent = []
        self.closed = False

    def send(self, payload):
        self.sent.append(json.loads(payload))

    def close(self):
        self.closed = True


def _new_ws():
    ws = SmartWebSocketV2("jwt", "key", "code", "feed")
    ws.wsapp = DummyWS()
    return ws


def test_smartwebsocket_state_is_instance_scoped():
    ws1 = _new_ws()
    ws2 = _new_ws()

    ws1.subscribe("a1", 2, [{"exchangeType": 1, "tokens": ["100"]}])

    assert ws1.input_request_dict != {}
    assert ws2.input_request_dict == {}


def test_unsubscribe_updates_in_memory_subscription_state():
    ws = _new_ws()
    ws.subscribe("a1", 2, [{"exchangeType": 1, "tokens": ["100", "101"]}])
    ws.unsubscribe("a2", 2, [{"exchangeType": 1, "tokens": ["100"]}])

    assert ws.input_request_dict == {2: {1: ["101"]}}
    assert ws.wsapp.sent[-1]["action"] == ws.UNSUBSCRIBE_ACTION


def test_resubscribe_uses_mode_to_exchange_mapping_shape():
    ws = _new_ws()
    ws.subscribe("a1", 1, [{"exchangeType": 1, "tokens": ["100"]}])
    ws.subscribe("a2", 2, [{"exchangeType": 2, "tokens": ["200"]}])

    ws.wsapp.sent.clear()
    ws.resubscribe()

    assert len(ws.wsapp.sent) == 2
    assert {msg["params"]["mode"] for msg in ws.wsapp.sent} == {1, 2}


def test_close_connection_can_preserve_resubscribe_flag():
    ws = _new_ws()
    ws.RESUBSCRIBE_FLAG = True
    ws.close_connection(clear_resubscribe=False)

    assert ws.RESUBSCRIBE_FLAG is True
    assert ws.wsapp.closed is True


def test_broker_subscribe_rejects_unresolved_token(mocker, dummy_creds):
    broker = AngelOne(**dummy_creds)
    mocker.patch.object(broker, "_resolve_instrument", return_value={"exchange": "NSE", "token": "DUMMY"})

    with pytest.raises(ValueError, match="instrument token not found"):
        broker.subscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)


def test_broker_subscribe_buffers_and_sends_when_connected(mocker, dummy_creds):
    broker = AngelOne(**dummy_creds)
    mocker.patch.object(broker, "_resolve_instrument", return_value={"exchange": "NSE", "token": "2885"})

    fake_client = SimpleNamespace(wsapp=object())
    fake_client.subscribe = mocker.Mock()
    broker._ws_client = fake_client

    broker.subscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)

    assert len(broker._pending_subscriptions) == 1
    fake_client.subscribe.assert_called_once()
    kwargs = fake_client.subscribe.call_args.kwargs
    assert kwargs["mode"] == StreamMode.QUOTE.value
    assert kwargs["correlation_id"].startswith("sub_")


def test_broker_unsubscribe_calls_wsclient_and_removes_pending(mocker, dummy_creds):
    broker = AngelOne(**dummy_creds)
    mocker.patch.object(broker, "_resolve_instrument", return_value={"exchange": "NSE", "token": "2885"})

    fake_client = SimpleNamespace(wsapp=object())
    fake_client.subscribe = mocker.Mock()
    fake_client.unsubscribe = mocker.Mock()
    broker._ws_client = fake_client

    broker.subscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)
    assert len(broker._pending_subscriptions) == 1

    broker.unsubscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)

    fake_client.unsubscribe.assert_called_once()
    assert broker._pending_subscriptions == []


def test_start_streaming_skips_pending_replay_when_resubscribe_flag_set(mocker, dummy_creds):
    broker = AngelOne(**dummy_creds)
    broker._pending_subscriptions = [([{"exchange": "NSE", "token": "2885"}], StreamMode.QUOTE.value)]
    replay_spy = mocker.patch.object(broker, "_send_subscription")
    mocker.patch.object(broker, "_require_auth", return_value="jwt_token_xxx")
    mocker.patch.object(context, "get_feed_token", return_value="feed_token_xxx")

    class FakeWSClient:
        def __init__(self, *args, **kwargs):
            self.RESUBSCRIBE_FLAG = True

        def connect(self):
            self.on_open(None)

    mocker.patch("india_stocks_api.internal.angel.streaming.SmartWebSocketV2", FakeWSClient)

    broker.start_streaming()

    replay_spy.assert_not_called()


def test_start_streaming_emits_canonical_tick_object(mocker, dummy_creds):
    broker = AngelOne(**dummy_creds)
    mocker.patch.object(broker, "_require_auth", return_value="jwt_token_xxx")
    mocker.patch.object(context, "get_feed_token", return_value="feed_token_xxx")
    mocker.patch.object(broker, "_resolve_instrument", return_value={"exchange": "NSE", "token": "2885"})

    captured = []
    broker.on_tick = lambda tick: captured.append(tick)
    broker.subscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)

    class FakeWSClient:
        def __init__(self, *args, **kwargs):
            self.RESUBSCRIBE_FLAG = False
            self.wsapp = object()

        def subscribe(self, **kwargs):
            return None

        def connect(self):
            self.on_open(None)
            self.on_data(
                None,
                {
                    "subscription_mode": 2,
                    "subscription_mode_val": "QUOTE",
                    "exchange_type": 1,
                    "token": "2885",
                    "exchange_timestamp": 1710000000000,
                    "last_traded_price": 250050,
                    "last_traded_quantity": 7,
                    "open_price_of_the_day": 249000,
                    "high_price_of_the_day": 251000,
                    "low_price_of_the_day": 248500,
                    "closed_price": 248000,
                    "volume_trade_for_the_day": 123456,
                    "open_interest": 5555,
                },
            )

    mocker.patch("india_stocks_api.internal.angel.streaming.SmartWebSocketV2", FakeWSClient)

    broker.start_streaming()

    assert captured
    assert isinstance(captured[0], WebSocketTick)
    assert captured[0].symbol == "RELIANCE"
    assert captured[0].exchange == "NSE"
