# Production Guide

This guide covers patterns and best practices for production trading systems.

---

## Session Management

### Handle Session Expiry

Sessions expire at midnight IST. Always handle `SessionExpiredError`:

```python
from india_stocks_api import SessionExpiredError, AuthenticationError

def safe_get_quote(broker, instrument):
    """Get quote with automatic re-authentication."""
    try:
        return broker.get_quote(instrument)
    except SessionExpiredError:
        broker.authenticate()
        return broker.get_quote(instrument)
```

### Wrapper Pattern

Create a wrapper for automatic session handling:

```python
import functools
from india_stocks_api import SessionExpiredError

def with_session(method):
    """Decorator to handle session expiry."""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except SessionExpiredError:
            self.broker.authenticate()
            return method(self, *args, **kwargs)
    return wrapper

class TradingClient:
    def __init__(self, broker):
        self.broker = broker
    
    @with_session
    def get_quote(self, instrument):
        return self.broker.get_quote(instrument)
    
    @with_session
    def place_order(self, **kwargs):
        return self.broker.place_order(**kwargs)
```

---

## Error Handling Patterns

### Comprehensive Error Handling

```python
from india_stocks_api import (
    ISAError,
    AuthenticationError,
    SessionExpiredError,
    ValidationError,
    BrokerError,
    NetworkError,
    RateLimitError,
)

def place_order_safe(broker, **kwargs):
    """Place order with comprehensive error handling."""
    try:
        return broker.place_order(**kwargs)
    
    except AuthenticationError as e:
        log.error(f"Auth failed: {e}")
        # Credentials issue - alert operator
        raise
    
    except SessionExpiredError:
        log.info("Session expired, re-authenticating")
        broker.authenticate()
        return broker.place_order(**kwargs)
    
    except ValidationError as e:
        log.error(f"Invalid order: {e}")
        # Bad parameters - fix code
        raise
    
    except BrokerError as e:
        log.error(f"Broker rejected: {e}")
        # Check e.details for broker error code
        raise
    
    except NetworkError as e:
        log.warning(f"Network error: {e}")
        # Retry may help
        raise
    
    except RateLimitError as e:
        log.warning(f"Rate limited: {e}")
        # Back off and retry
        time.sleep(1)
        return broker.place_order(**kwargs)
```

### Retry Pattern

```python
import time
from india_stocks_api import NetworkError, RateLimitError

def with_retry(func, max_retries=3, backoff=1.0):
    """Execute function with retry on transient errors."""
    last_error = None
    
    for attempt in range(max_retries):
        try:
            return func()
        except (NetworkError, RateLimitError) as e:
            last_error = e
            wait = backoff * (2 ** attempt)  # Exponential backoff
            time.sleep(wait)
    
    raise last_error

# Usage
quote = with_retry(lambda: broker.get_quote(Equity("RELIANCE")))
```

---

## Rate Limiting

### Throttle Requests

```python
import time
from threading import Lock

class RateLimiter:
    def __init__(self, calls_per_second: float):
        self.min_interval = 1.0 / calls_per_second
        self.last_call = 0
        self.lock = Lock()
    
    def wait(self):
        with self.lock:
            elapsed = time.time() - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_call = time.time()

# Usage
quote_limiter = RateLimiter(1)  # 1 call per second

def get_quote_throttled(broker, instrument):
    quote_limiter.wait()
    return broker.get_quote(instrument)
```

### Batch Operations

```python
def get_quotes_batch(broker, instruments, delay=1.0):
    """Fetch quotes with rate limiting."""
    quotes = {}
    for inst in instruments:
        quotes[inst.symbol] = broker.get_quote(inst)
        time.sleep(delay)  # Respect rate limits
    return quotes
```

---

## Order Safety

### Pre-Trade Validation

```python
def validate_order(broker, instrument, quantity, transaction_type):
    """Validate order before submission."""
    # Check funds
    funds = broker.get_funds()
    quote = broker.get_quote(instrument)
    required = quote.ltp * quantity
    
    if transaction_type == TransactionType.BUY:
        if funds.available_cash < required:
            raise ValueError(f"Insufficient funds: {funds.available_cash} < {required}")
    
    # Check holdings for sell
    if transaction_type == TransactionType.SELL:
        holdings = broker.get_holdings()
        held = next((h.quantity for h in holdings if h.symbol == instrument.symbol), 0)
        if held < quantity:
            raise ValueError(f"Insufficient holdings: {held} < {quantity}")
    
    return True
```

### Order Confirmation Pattern

```python
def place_order_confirmed(broker, **kwargs):
    """Place order and verify execution."""
    response = broker.place_order(**kwargs)
    
    if response.status != "success":
        raise BrokerError(f"Order failed: {response.message}")
    
    # Wait and verify
    time.sleep(2)
    orders = broker.get_orders()
    order = next((o for o in orders if o.order_id == response.order_id), None)
    
    if not order:
        raise BrokerError(f"Order {response.order_id} not found")
    
    return order
```

---

## Logging

### Structured Logging

```python
import logging
import json

# Configure logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    level=logging.INFO
)
log = logging.getLogger('trading')

def log_order(response, **kwargs):
    """Log order for audit trail."""
    log.info(json.dumps({
        "event": "order_placed",
        "order_id": response.order_id,
        "status": response.status,
        "instrument": kwargs.get('instrument').symbol,
        "transaction_type": kwargs.get('transaction_type').value,
        "quantity": kwargs.get('quantity'),
        "price": kwargs.get('price', 0),
    }))
```

### Trade Journal

```python
import csv
from datetime import datetime

def record_trade(response, **kwargs):
    """Append trade to CSV journal."""
    with open('trades.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().isoformat(),
            response.order_id,
            response.status,
            kwargs['instrument'].symbol,
            kwargs['transaction_type'].value,
            kwargs['quantity'],
            kwargs.get('price', 0),
        ])
```

---

## Graceful Shutdown

### Handle Signals

```python
import signal
import sys

class TradingSystem:
    def __init__(self, broker):
        self.broker = broker
        self.running = True
        
        # Register signal handlers
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
    
    def _shutdown(self, signum, frame):
        print("Shutdown requested...")
        self.running = False
        
        # Stop streaming
        if hasattr(self.broker, '_ws_client'):
            self.broker.stop_streaming()
        
        # Log final state
        positions = self.broker.get_positions()
        if positions:
            print(f"Open positions: {len(positions)}")
        
        sys.exit(0)
    
    def run(self):
        while self.running:
            # Main trading loop
            pass
```

### Close Positions on Exit

```python
def close_intraday_positions(broker):
    """Close all intraday positions before shutdown."""
    positions = broker.get_positions()
    
    for pos in positions:
        if pos.product_type == "MIS" and pos.quantity != 0:
            trans = TransactionType.SELL if pos.quantity > 0 else TransactionType.BUY
            qty = abs(pos.quantity)
            
            broker.place_order(
                instrument=Equity(pos.symbol, exchange=pos.exchange),
                transaction_type=trans,
                quantity=qty,
                order_type=OrderType.MARKET,
                product_type=ProductType.INTRADAY
            )
```

---

## Monitoring

### Health Checks

```python
def health_check(broker):
    """Verify broker connection is healthy."""
    try:
        # Check authentication
        broker._require_auth()
        
        # Check API connectivity
        quote = broker.get_quote(Equity("SBIN"))
        
        return {"status": "healthy", "ltp": quote.ltp}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

### Heartbeat Pattern

```python
import threading
import time

def start_heartbeat(broker, interval=60):
    """Periodic health check."""
    def heartbeat():
        while True:
            status = health_check(broker)
            if status["status"] != "healthy":
                log.error(f"Unhealthy: {status['error']}")
                # Alert operator
            time.sleep(interval)
    
    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
```

---

## Configuration

### Environment-Based Config

```python
import os
from dataclasses import dataclass

@dataclass
class TradingConfig:
    api_key: str
    client_code: str
    password: str
    totp_key: str
    max_position_size: int = 100
    max_loss_per_trade: float = 1000.0
    enable_trading: bool = False

config = TradingConfig(
    api_key=os.getenv("ANGEL_API_KEY"),
    client_code=os.getenv("ANGEL_CLIENT_ID"),
    password=os.getenv("ANGEL_PIN"),
    totp_key=os.getenv("ANGEL_TOTP_SECRET"),
    max_position_size=int(os.getenv("MAX_POSITION", 100)),
    enable_trading=os.getenv("ENABLE_TRADING", "false").lower() == "true",
)
```

### Safety Guards

```python
def place_order_guarded(broker, config, **kwargs):
    """Place order with safety guards."""
    # Check trading enabled
    if not config.enable_trading:
        log.warning("Trading disabled - skipping order")
        return None
    
    # Check position size
    if kwargs['quantity'] > config.max_position_size:
        raise ValueError(f"Order exceeds max position: {kwargs['quantity']} > {config.max_position_size}")
    
    return broker.place_order(**kwargs)
```

---

## Checklist

### Before Production

- [ ] Error handling for all exception types
- [ ] Session expiry handling
- [ ] Rate limiting implemented
- [ ] Logging configured
- [ ] Graceful shutdown handling
- [ ] Health checks in place
- [ ] Position limits configured
- [ ] Paper testing completed
- [ ] Credentials in environment variables
- [ ] `.env` excluded from git

### Daily Operations

- [ ] Check session status at market open
- [ ] Monitor rate limit errors
- [ ] Review trade logs
- [ ] Verify positions closed at day end
- [ ] Check for API notifications from broker

---

## Next Steps

- Review [Error Handling](../developer/error-handling.md) for exception details
- See [Broker Support](broker-support.md) for broker-specific limits
