"""Live integration tests for Angel websocket streaming.

Requires env vars:
- ANGEL_API_KEY
- ANGEL_CLIENT_ID
- ANGEL_PIN
- ANGEL_TOTP_SECRET
"""

from __future__ import annotations

import os
import threading
import time

import pytest

from india_stocks_api.brokers.angel import AngelOne
from india_stocks_api.constants import StreamMode
from india_stocks_api.instruments import Equity


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
    creds = _get_creds()
    broker = AngelOne(**creds)
    broker.authenticate()
    yield broker
    broker.stop_streaming()


@pytest.mark.integration
@pytest.mark.skipif(_missing, reason="Live Angel credentials not set in env")
class TestAngelStreamingLive:
    def test_stream_connects_and_receives_ticks(self, live_broker):
        broker = live_broker
        open_event = threading.Event()
        tick_event = threading.Event()
        ticks = []
        stream_errors = []
        stream_exceptions = []

        def on_open():
            open_event.set()

        def on_tick(tick):
            ticks.append(tick)
            tick_event.set()

        def on_error(error_type, error_msg):
            stream_errors.append((error_type, error_msg))

        broker.on_open = on_open
        broker.on_tick = on_tick
        broker.on_error = on_error
        broker.subscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)

        def run_stream():
            try:
                broker.start_streaming()
            except Exception as exc:  # pragma: no cover - guard for live-run diagnostics
                stream_exceptions.append(exc)

        t = threading.Thread(target=run_stream, daemon=True)
        t.start()
        try:
            assert open_event.wait(20), "WebSocket open callback was not triggered within timeout"
            assert tick_event.wait(40), "No market ticks received within timeout"
            assert not stream_exceptions, f"Streaming thread raised exception: {stream_exceptions}"
            assert isinstance(ticks[0], dict), "Tick payload is expected to be a dictionary"
            assert "token" in ticks[0], "Tick payload missing token field"
        finally:
            broker.stop_streaming()
            t.join(timeout=10)

    def test_stop_streaming_is_idempotent(self, live_broker):
        # Should not raise even if stream is already stopped/closed.
        live_broker.stop_streaming()
        live_broker.stop_streaming()
        # Give the internal websocket close path a moment and ensure no exceptions were thrown.
        time.sleep(0.2)
