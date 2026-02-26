# Broker Adapter Guide

This guide explains how to implement a new broker adapter.

---

## Overview

Adding a new broker involves:

1. Porting broker-specific code to `internal/<broker>/`
2. Creating a public adapter in `brokers/<broker>.py`
3. Implementing the `BaseBroker` interface
4. Setting up instrument resolution

---

## Step 1: Project Structure

Create the directory structure:

```
india_stocks_api/
├── brokers/
│   └── newbroker.py           # Public adapter (you create)
└── internal/
    └── newbroker/
        ├── __init__.py
        ├── api/
        │   ├── __init__.py
        │   ├── auth_api.py    # Authentication
        │   ├── order_api.py   # Order operations
        │   └── data.py        # Market data
        ├── database/
        │   ├── __init__.py
        │   └── master_contract.py  # Instrument download
        ├── mapping/
        │   ├── __init__.py
        │   └── transform_data.py   # Value mappings
        └── streaming/
            └── __init__.py    # WebSocket (optional)
```

---

## Step 2: Base Class Implementation

### Minimal Implementation

```python
# brokers/newbroker.py
from typing import Any
from datetime import date

from ..constants import (
    CandleInterval, OrderType, OrderValidity, 
    ProductType, TransactionType
)
from ..exceptions import AuthenticationError, BrokerError
from ..instruments.models import Equity, Future, Index, Option
from ..responses import (
    FundsResponse, Holding, HistoryResponse, Order,
    OrderResponse, Position, ProfileResponse, 
    QuoteResponse, Trade
)
from .base import BaseBroker


class NewBroker(BaseBroker, broker_name="newbroker"):
    """
    NewBroker adapter.
    
    Args:
        api_key: API key from broker
        secret: API secret
        # Add broker-specific credentials
    """
    
    def __init__(self, api_key: str, secret: str):
        self.api_key = api_key
        self.secret = secret
    
    def _download_master_contract(self):
        """Download broker's instrument master."""
        from ..internal.newbroker.database import master_contract_download
        master_contract_download()
    
    def authenticate(self) -> bool:
        """Authenticate with broker."""
        # Implement authentication logic
        raise NotImplementedError
    
    def place_order(
        self,
        instrument: Equity | Future | Option,
        transaction_type: TransactionType,
        quantity: int,
        order_type: OrderType = OrderType.MARKET,
        product_type: ProductType = ProductType.INTRADAY,
        price: float = 0.0,
        trigger_price: float = 0.0,
        validity: OrderValidity = OrderValidity.DAY,
        **kwargs,
    ) -> OrderResponse:
        """Place order."""
        raise NotImplementedError
    
    # ... implement all abstract methods from BaseBroker
```

### Required Methods

You must implement all abstract methods from `BaseBroker`:

| Method | Return Type | Purpose |
|--------|-------------|---------|
| `authenticate()` | `bool` | Login to broker |
| `place_order(...)` | `OrderResponse` | Place new order |
| `get_positions()` | `list[Position]` | Get open positions |
| `get_holdings()` | `list[Holding]` | Get demat holdings |
| `get_orders()` | `list[Order]` | Get order book |
| `get_trades()` | `list[Trade]` | Get executed trades |
| `modify_order(...)` | `OrderResponse` | Modify pending order |
| `cancel_order(...)` | `OrderResponse` | Cancel pending order |
| `get_quote(...)` | `QuoteResponse` | Get real-time quote |
| `get_depth(...)` | `DepthResponse` | Get market depth |
| `get_history(...)` | `HistoryResponse` | Get historical data |
| `get_funds()` | `FundsResponse` | Get margin info |
| `get_profile()` | `ProfileResponse` | Get user profile |
| `_download_master_contract()` | `None` | Download instruments |

---

## Step 3: Instrument Resolution

### Add to Resolver

The instrument resolver needs to know how to query your broker's database schema.

```python
# instruments/resolver.py

def _resolve_for_newbroker(instrument, db_session):
    """Resolve instrument for NewBroker."""
    if isinstance(instrument, Equity):
        query = db_session.query(Instrument).filter(
            Instrument.symbol == instrument.symbol,
            Instrument.exchange == instrument.exchange,
            Instrument.instrument_type == "EQ"
        )
    elif isinstance(instrument, Future):
        query = db_session.query(Instrument).filter(
            Instrument.symbol == instrument.symbol,
            Instrument.exchange == instrument.exchange,
            Instrument.expiry == instrument.expiry,
            Instrument.instrument_type == "FUT"
        )
    # ... handle Option, Index
    
    return query.first()
```

---

## Step 4: Value Mappings

### Enum Translation

Create mappings from canonical enums to broker-specific values:

```python
# internal/newbroker/mapping/transform_data.py

ORDER_TYPE_MAP = {
    "MARKET": "MKT",      # Broker uses "MKT"
    "LIMIT": "LMT",       # Broker uses "LMT"
    "SL": "SL",
    "SL-M": "SL-M",
}

PRODUCT_TYPE_MAP = {
    "MIS": "I",           # Broker uses "I" for intraday
    "CNC": "D",           # Broker uses "D" for delivery
    "NRML": "M",          # Broker uses "M" for margin
}

def map_order_type(value: str) -> str:
    return ORDER_TYPE_MAP.get(value, value)

def map_product_type(value: str) -> str:
    return PRODUCT_TYPE_MAP.get(value, value)

def reverse_map_product_type(value: str) -> str:
    reverse = {v: k for k, v in PRODUCT_TYPE_MAP.items()}
    return reverse.get(value, value)
```

---

## Step 5: Response Mapping

### Map Broker Response to Canonical

```python
# In brokers/newbroker.py

def _map_quote_response(self, raw: dict) -> QuoteResponse:
    """Map broker's quote response to canonical QuoteResponse."""
    return QuoteResponse(
        ltp=float(raw.get("lastPrice", 0)),
        bid=float(raw.get("buyPrice", 0)),
        ask=float(raw.get("sellPrice", 0)),
        open=float(raw.get("openPrice", 0)),
        high=float(raw.get("highPrice", 0)),
        low=float(raw.get("lowPrice", 0)),
        prev_close=float(raw.get("closePrice", 0)),
        volume=int(raw.get("totalTradedVolume", 0)),
        oi=int(raw.get("openInterest", 0)),
    )

def _map_position(self, raw: dict) -> Position:
    """Map broker's position to canonical Position."""
    return Position(
        symbol=raw.get("tradingSymbol", ""),
        exchange=raw.get("exchange", ""),
        product_type=reverse_map_product_type(raw.get("product", "")),
        quantity=int(raw.get("netQty", 0)),
        average_price=float(raw.get("avgPrice", 0)),
        ltp=float(raw.get("ltp", 0)),
        pnl=float(raw.get("pnl", 0)),
        raw=raw,
    )
```

---

## Step 6: Authentication

### Implement Authentication

```python
def authenticate(self) -> bool:
    """
    Authenticate with broker.
    
    Returns:
        True on success.
    
    Raises:
        AuthenticationError: If authentication fails.
    """
    from ..internal.newbroker.api.auth_api import login
    from ..internal import context
    
    # Call broker's login API
    response = login(
        api_key=self.api_key,
        secret=self.secret,
    )
    
    if not response.get("success"):
        raise AuthenticationError(
            f"Login failed: {response.get('message')}",
            details={"raw": response}
        )
    
    # Store tokens in context
    context.set_auth_token(response["access_token"])
    
    # Persist session
    context.save_session("newbroker", {
        "access_token": response["access_token"],
        "api_key": self.api_key,
        "authenticated_at": datetime.now().isoformat(),
        "expires_at": response.get("expires_at"),
    })
    
    return True
```

---

## Step 7: Master Contract Download

### Implement Download

```python
# internal/newbroker/database/master_contract.py

import httpx
import pandas as pd
from sqlalchemy import create_engine

from ...context import get_instruments_db_path


def master_contract_download():
    """Download and populate instrument database."""
    db_path = get_instruments_db_path()
    
    # Download raw data from broker
    response = httpx.get("https://api.newbroker.com/instruments")
    data = response.json()
    
    # Transform to DataFrame
    df = pd.DataFrame(data)
    
    # Normalize columns
    df = df.rename(columns={
        "symbolName": "symbol",
        "tokenId": "token",
        "exchangeSegment": "exchange",
        # ... map broker columns to standard schema
    })
    
    # Write to SQLite
    engine = create_engine(f"sqlite:///{db_path}")
    df.to_sql("instruments", engine, if_exists="replace", index=False)
```

---

## Step 8: Internal API Calls

### Structure API Module

```python
# internal/newbroker/api/order_api.py

import httpx
from ..context import get_auth_token, get_httpx_client

BASE_URL = "https://api.newbroker.com"


def place_order(data: dict, token: str) -> tuple[dict, int]:
    """Place order via broker API."""
    client = get_httpx_client()
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    
    response = client.post(
        f"{BASE_URL}/orders",
        json=data,
        headers=headers,
    )
    
    return response.json(), response.status_code


def get_positions(token: str) -> dict:
    """Fetch positions from broker."""
    client = get_httpx_client()
    
    headers = {"Authorization": f"Bearer {token}"}
    
    response = client.get(
        f"{BASE_URL}/positions",
        headers=headers,
    )
    
    return response.json()
```

---

## Step 9: WebSocket Streaming (Optional)

### Implement Streaming

```python
# In brokers/newbroker.py

def subscribe(self, instruments, mode):
    """Subscribe to instruments."""
    # Buffer subscriptions
    for inst in instruments:
        resolved = self._resolve_instrument(inst)
        self._pending_subscriptions.append({
            "token": resolved["token"],
            "mode": mode.value,
        })

def start_streaming(self):
    """Start WebSocket connection."""
    from ..internal.newbroker.streaming import BrokerWebSocket
    
    self._ws = BrokerWebSocket(
        auth_token=self._require_auth(),
        on_tick=self._handle_tick,
    )
    
    # Send buffered subscriptions
    for sub in self._pending_subscriptions:
        self._ws.subscribe(sub["token"], sub["mode"])
    
    self._ws.connect()  # Blocking
```

---

## Step 10: Testing

### Write Unit Tests

```python
# tests/unit/test_newbroker.py

import pytest
from india_stocks_api.brokers import NewBroker
from india_stocks_api.instruments import Equity


def test_instantiation():
    broker = NewBroker(api_key="test", secret="test")
    assert broker.api_key == "test"


def test_resolve_equity(mocker):
    # Mock database lookup
    mocker.patch(...)
    
    broker = NewBroker(...)
    result = broker._resolve_instrument(Equity("TEST"))
    
    assert result["token"] == "12345"
```

### Write Integration Tests

```python
# tests/integration/test_newbroker_live.py

import pytest
from india_stocks_api.brokers import NewBroker


@pytest.mark.integration
def test_authentication():
    broker = NewBroker(
        api_key=os.getenv("NEWBROKER_API_KEY"),
        secret=os.getenv("NEWBROKER_SECRET"),
    )
    
    result = broker.authenticate()
    assert result is True
```

---

## Checklist

Before submitting your broker adapter:

- [ ] All `BaseBroker` abstract methods implemented
- [ ] Instrument resolution working for Equity, Future, Option
- [ ] All responses mapped to canonical dataclasses
- [ ] Error handling with proper exception types
- [ ] Unit tests for mapping functions
- [ ] Integration tests (require credentials)
- [ ] Documentation for required credentials
- [ ] Master contract download implemented

---

## Reference: Angel One Adapter

Study `brokers/angel.py` (~940 lines) as a complete reference implementation.

Key patterns:
- `_resolve_instrument()` for domain object → token
- `_require_auth()` for session validation
- `_map_*_response()` for response normalization
- Internal imports from `../internal/angel/`

---

## Next Steps

- [Instrument System](instrument-system.md) - Database schema details
- [Session & Auth](session-auth.md) - Token persistence
- [Testing Strategy](testing.md) - Test organization
