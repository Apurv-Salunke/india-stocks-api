# India Stocks API v2.0

Type-safe Python client for Indian brokers with canonical responses and auto-provisioned instruments.

## What this solves
- Single, typed surface for orders, market data, history, streaming, funds, profile.
- Auto-downloads and normalizes the instruments DB; you always code with NSE-style symbols.
- Canonical response objects so downstream code stays broker-agnostic.

## Supported today
- Broker: Angel One (live). Zerodha in progress.
- Python: 3.10, 3.11, 3.12.
- CI: ruff, mypy, unit tests. Integration tests are opt-in (require creds).

## Install
```bash
git clone https://github.com/Apurv-Salunke/india-stocks-api.git
cd india-stocks-api
poetry install
# or: pip install india-stocks-api
```

## Quick start
```python
from india_stocks_api.brokers import AngelOne
from india_stocks_api.constants import TransactionType, OrderType, StreamMode
from india_stocks_api.instruments import Equity

broker = AngelOne(api_key, client_code, password, totp_key)
broker.authenticate()

# Market data (canonical QuoteResponse)
quote = broker.get_quote(Equity("RELIANCE"))
print("LTP:", quote.ltp)

# Place order
broker.place_order(
    instrument=Equity("RELIANCE"),
    transaction_type=TransactionType.BUY,
    quantity=1,
    order_type=OrderType.MARKET,
)

# Streaming (canonical WebSocketTick)
broker.on_tick = lambda tick: print("tick", tick.symbol, tick.ltp)
broker.subscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)
broker.start_streaming()  # blocking; run in a thread if needed
```

## Canonical responses
Returned from `india_stocks_api.responses`:
- `QuoteResponse`, `DepthResponse`, `HistoryResponse`
- `FundsResponse`, `ProfileResponse`
- `WebSocketTick` (streaming, normalized depth levels)

## Streaming behavior (Angel One)
- Subscriptions are buffered; sent on connect.
- Reconnects auto-resubscribe via SmartWebSocketV2; adapter maps frames → `WebSocketTick`.
- `unsubscribe(...)` and `stop_streaming()` are idempotent.

## Architecture (short)
1. Public adapter (`brokers/angel.py`) exposes typed methods and maps to canonical responses.
2. Shim (`internal/context.py`) supplies auth, symbols, HTTP to ported OpenAlgo code.
3. Ported internals (`internal/angel/*`) stay close to upstream for easier syncs.

## Testing
- Unit: `poetry run pytest tests/unit`
- Integration (live Angel creds in `.env`): `poetry run pytest tests/integration/test_data_methods_live.py -v`
- Lint/type: `poetry run ruff check .` and `poetry run mypy india_stocks_api`

## Contributing
- Install hooks: `pre-commit install`
- Before push: run unit tests + ruff + mypy.
- PRs target `dev`; include what changed, tests run, and any live-cred needs.

## Docs
A dedicated docs site is planned. Until then, this README + `tests/` and `internal/` act as reference.
