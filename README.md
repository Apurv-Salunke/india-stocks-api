# India Stocks API v2.0

**A type-safe Python client for trading and market data across Indian brokers.**

## Why this library exists

1. **Unified broker surface** — tangent clients only interact with typed domain objects (`Equity`, `Future`, `Option`, `StreamMode`), even though the underlying code reuses OpenAlgo implementations.
2. **Zero config provisioning** — instruments DB downloads automatically and smart symbol resolution keeps NSE-style names in your code.
3. **Canonical responses** — market data, historical candles, funds, and profile APIs expose structured objects so downstream logic is broker-agnostic.

## Status at a glance

| Area | Status |
| --- | --- |
| Brokers | Angel One (live), Zerodha (in progress) |
| Streaming | WebSocket (Angel) with reconnect + canonical tick) |
| Python | 3.10, 3.11, 3.12 |
| Tests | Unit suite in CI, integration scripts gated by env vars |

## Install

```bash
git clone https://github.com/Apurv-Salunke/india-stocks-api.git
cd india-stocks-api
poetry install
```

Alternatively: `pip install india-stocks-api`.

## Quick start

```python
from india_stocks_api.brokers import AngelOne
from india_stocks_api.constants import OrderType, TransactionType, StreamMode
from india_stocks_api.instruments import Equity

broker = AngelOne(api_key, client_code, password, totp_key)
broker.authenticate()

# place a quote-safe order
quote = broker.get_quote(Equity("RELIANCE"))
print("LTP:", quote.ltp)

# subscribe to streaming ticks
broker.on_tick = lambda tick: print("tick", tick.symbol, tick.ltp)
broker.subscribe([Equity("RELIANCE")], mode=StreamMode.QUOTE)
broker.start_streaming()  # blocking, so do this in a thread
```

## Canonical response contracts

All public market-data/funds/profile APIs return typed objects from `india_stocks_api.responses`. Example:

- `QuoteResponse`, `DepthResponse`, `HistoryResponse`
- `FundsResponse`, `ProfileResponse`
- `WebSocketTick` for streaming (with normalized depth levels)

Each object exposes explicit fields (`ltp`, `volume`, `high`, `low`, `exchanges`, `client_code`, …) so callers never need to parse raw broker payload keys.

## Streaming overview

- `AngelOne.subscribe(...)` resolves domain instruments to tokens and buffers subscriptions.
- `SmartWebSocketV2` handles reconnect/resubscribe while the adapter maps binary frames into `WebSocketTick`.
- Streaming clients consume the normalized tick object (no raw JSON) and receive consistent `symbol`, `mode`, `ltp`, `volume`, `oi`, plus depth levels.
- `stop_streaming()` and `unsubscribe(...)` cleanly drop the session with idempotent behavior.

## Architecture snapshot

1. **Public adapter** (`india_stocks_api/brokers/angel.py`) exposes typed methods and canonical responses.
2. **Shim/context** (`india_stocks_api/internal/context.py`) provides shared auth, symbol, and HTTP helpers to the ported OpenAlgo code.
3. **Ported internals** under `internal/angel` remain close to OpenAlgo to simplify syncing/upstream updates.

## Testing & quality

- `poetry run pytest tests/unit` (runs in CI and covers adapters, streaming helpers, and response mappers).
- Integration scripts under `tests/integration/` require live credentials and are skipped in CI; run locally with `.env` containing Angel API secrets.
- `ruff`, `mypy`, and `pre-commit` enforce formatting and typing.

## Contributing

- Use `pre-commit install` before committing.
- Run `poetry run pytest tests/unit` and `poetry run ruff check .` before pushing.
- Open a PR targeting `dev`; describe the change, tests, and any live dependencies (e.g., credentials required).

## Next steps / docs

1. Expand docs under `docs/` or the upcoming dedicated site for API reference, streaming guide, and error handling.
2. Link to this README after the docs site is live so it remains a concise landing page.
