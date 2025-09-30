# Broker Integration Guide

## Overview

This guide explains how to add support for new brokers to the Indian Stock API. The system is designed to make broker integration straightforward while maintaining consistency across all brokers.

## Integration Architecture

### Base Provider Pattern
All brokers extend the `BaseProvider` class which provides:
- Common caching functionality
- Database storage operations
- Utility methods for data transformation
- Threading support for concurrent operations

### Broker Interface
All brokers implement the `Broker` interface which provides:
- Authentication methods
- Trading operations (orders, quotes, positions)
- Data fetching (candles, portfolio)
- Instrument resolution

## Step-by-Step Integration

### 1. Create Broker Class

Create a new broker class in `india_stocks_api/brokers/`:

```python
# india_stocks_api/brokers/zerodha.py
from india_stocks_api.brokers.base.broker import Broker
from india_stocks_api.brokers.base.errors import AuthenticationError, OrderError

class Zerodha(Broker):
    def __init__(self):
        super().__init__()
        self.api_key = None
        self.access_token = None
        self.base_url = "https://api.kite.trade"

    def login(self, api_key: str, username: str, password: str):
        """Authenticate with Zerodha API"""
        # Implement Zerodha-specific authentication
        pass

    def get_candles(self, symbol: str, exchange: str, interval: str,
                   from_date: str, to_date: str, limit: int = 100):
        """Fetch historical candles"""
        # Implement Zerodha-specific candle fetching
        pass

    def place_order(self, symbol: str, exchange: str, transaction_type: str,
                   quantity: int, order_type: str, product: str, price: float = None):
        """Place an order"""
        # Implement Zerodha-specific order placement
        pass

    def get_quote(self, symbol: str, exchange: str):
        """Get current quote"""
        # Implement Zerodha-specific quote fetching
        pass

    def get_positions(self):
        """Get current positions"""
        # Implement Zerodha-specific position fetching
        pass

    def get_holdings(self):
        """Get current holdings"""
        # Implement Zerodha-specific holdings fetching
        pass

    def get_pnl(self):
        """Get P&L information"""
        # Implement Zerodha-specific P&L fetching
        pass
```

### 2. Create Data Provider

Create a data provider class in `india_stocks_api/database/providers/`:

```python
# india_stocks_api/database/providers/zerodha_tokens_manager.py
from india_stocks_api.database.providers.base_provider import BaseProvider
from india_stocks_api.database.services import InstrumentService

class ZerodhaTokensManager(BaseProvider):
    def __init__(self, service: InstrumentService, broker_name: str, max_workers: int = 4):
        super().__init__(max_workers)
        self.service = service
        self.broker_name = broker_name
        self.cache_file = get_cache_file_path(f"{broker_name}_tokens_cache.json")
        self.cache_validity_hours = 24
        self.base_url = "https://api.kite.trade"

    def _fetch_market_data(self, force_refresh: bool = False):
        """Fetch market data from Zerodha API"""
        # Implement Zerodha-specific data fetching
        pass

    def _is_equity_instrument(self, instrument_data: dict) -> bool:
        """Check if instrument is equity"""
        # Implement Zerodha-specific equity detection
        pass

    def _is_fno_instrument(self, instrument_data: dict) -> bool:
        """Check if instrument is F&O"""
        # Implement Zerodha-specific F&O detection
        pass

    def _is_commodity_instrument(self, instrument_data: dict) -> bool:
        """Check if instrument is commodity"""
        # Implement Zerodha-specific commodity detection
        pass

    def _is_currency_instrument(self, instrument_data: dict) -> bool:
        """Check if instrument is currency"""
        # Implement Zerodha-specific currency detection
        pass

    def _transform_equity_data(self, raw_data: list) -> list:
        """Transform raw equity data to standard format"""
        # Implement Zerodha-specific equity transformation
        pass

    def _transform_fno_data(self, raw_data: list) -> list:
        """Transform raw F&O data to standard format"""
        # Implement Zerodha-specific F&O transformation
        pass

    def _transform_commodity_data(self, raw_data: list) -> list:
        """Transform raw commodity data to standard format"""
        # Implement Zerodha-specific commodity transformation
        pass

    def _transform_currency_data(self, raw_data: list) -> list:
        """Transform raw currency data to standard format"""
        # Implement Zerodha-specific currency transformation
        pass
```

### 3. Implement Instrument Resolution

Add instrument resolution methods to your broker class:

```python
class Zerodha(Broker):
    # ... existing methods ...

    @classmethod
    def resolve_equity_instrument(cls, symbol: str, exchange: str):
        """Resolve equity instrument to broker-specific token"""
        # Implement Zerodha-specific equity resolution
        pass

    @classmethod
    def resolve_fno_instrument(cls, symbol: str, exchange: str,
                              instrument_type: str, expiry_date: str = None,
                              strike_price: float = None, option_type: str = None):
        """Resolve F&O instrument to broker-specific token"""
        # Implement Zerodha-specific F&O resolution
        pass

    @classmethod
    def resolve_commodity_instrument(cls, symbol: str, exchange: str):
        """Resolve commodity instrument to broker-specific token"""
        # Implement Zerodha-specific commodity resolution
        pass

    @classmethod
    def resolve_currency_instrument(cls, symbol: str, exchange: str):
        """Resolve currency instrument to broker-specific token"""
        # Implement Zerodha-specific currency resolution
        pass
```

### 4. Update Package Exports

Add your broker to the package exports:

```python
# india_stocks_api/brokers/__init__.py
from .zerodha import Zerodha

__all__ = ["AngelOne", "Zerodha"]
```

```python
# india_stocks_api/database/providers/__init__.py
from .zerodha_tokens_manager import ZerodhaTokensManager

__all__ = ["AngelOneTokensManager", "ZerodhaTokensManager"]
```

### 5. Add Tests

Create comprehensive tests for your broker:

```python
# tests/core/brokers/test_zerodha.py
import pytest
from india_stocks_api.brokers.zerodha import Zerodha

class TestZerodha:
    def test_login_success(self):
        """Test successful login"""
        broker = Zerodha()
        result = broker.login("api_key", "username", "password")
        assert result is True

    def test_login_failure(self):
        """Test login failure"""
        broker = Zerodha()
        with pytest.raises(AuthenticationError):
            broker.login("invalid", "credentials", "here")

    def test_get_candles(self):
        """Test candle fetching"""
        broker = Zerodha()
        broker.login("api_key", "username", "password")
        candles = broker.get_candles("RELIANCE", "NSE", "1minute", limit=10)
        assert len(candles) <= 10
        assert "open" in candles[0]
        assert "high" in candles[0]
        assert "low" in candles[0]
        assert "close" in candles[0]

    def test_place_order(self):
        """Test order placement"""
        broker = Zerodha()
        broker.login("api_key", "username", "password")
        order = broker.place_order("RELIANCE", "NSE", "BUY", 10, "MARKET", "INTRADAY")
        assert "order_id" in order
        assert order["status"] in ["OPEN", "COMPLETE"]

    def test_get_quote(self):
        """Test quote fetching"""
        broker = Zerodha()
        broker.login("api_key", "username", "password")
        quote = broker.get_quote("RELIANCE", "NSE")
        assert "last_price" in quote
        assert "open" in quote
        assert "high" in quote
        assert "low" in quote
        assert "close" in quote
```

## Data Transformation Guidelines

### Standard Data Format

All brokers must transform their data to the following standard format:

#### Equity Instruments
```python
{
    "standardized_symbol": "RELIANCE",
    "instrument_name": "Reliance Industries Ltd",
    "exchange": "NSE",
    "category": "EQ",
    "broker_symbol": "RELIANCE",
    "broker_token": "2881",
    "tick_size": 0.05,
    "lot_size": 1
}
```

#### F&O Instruments
```python
{
    "standardized_symbol": "BANKNIFTY",
    "instrument_name": "Bank Nifty",
    "exchange": "NFO",
    "category": "FUT",  # or "OPT"
    "underlying_symbol": "BANKNIFTY",
    "expiry_date": "2024-01-25",
    "strike_price": 45000.0,  # For options only
    "option_type": "CE",  # For options only
    "broker_symbol": "BANKNIFTY24JAN45000CE",
    "broker_token": "12345",
    "tick_size": 0.05,
    "lot_size": 25
}
```

#### Commodity Instruments
```python
{
    "standardized_symbol": "GOLD",
    "instrument_name": "Gold",
    "exchange": "MCX",
    "category": "COM",
    "broker_symbol": "GOLDM24JAN",
    "broker_token": "67890",
    "tick_size": 1.0,
    "lot_size": 1
}
```

#### Currency Instruments
```python
{
    "standardized_symbol": "USDINR",
    "instrument_name": "USD/INR",
    "exchange": "CDS",
    "category": "CUR",
    "broker_symbol": "USDINR24JAN",
    "broker_token": "54321",
    "tick_size": 0.0025,
    "lot_size": 1000
}
```

### Exchange Mapping

Map broker-specific exchanges to standard exchanges:

```python
EXCHANGE_MAPPING = {
    "NSE": "NSE",
    "BSE": "BSE",
    "NFO": "NSE",  # NSE F&O
    "BFO": "BSE",  # BSE F&O
    "MCX": "MCX",
    "NCDEX": "NCDEX",
    "CDS": "NSE",  # Currency derivatives
    "BCD": "BSE"   # BSE currency derivatives
}
```

### Category Mapping

Map broker-specific categories to standard categories:

```python
CATEGORY_MAPPING = {
    "EQ": "EQ",
    "FUT": "FUT",
    "OPT": "OPT",
    "COM": "COM",
    "CUR": "CUR"
}
```

## API Integration Patterns

### Authentication Pattern
```python
def login(self, api_key: str, username: str, password: str):
    """Standard authentication pattern"""
    try:
        # 1. Validate credentials
        if not self._validate_credentials(api_key, username, password):
            raise AuthenticationError("Invalid credentials")

        # 2. Get access token
        token = self._get_access_token(api_key, username, password)

        # 3. Store credentials
        self.api_key = api_key
        self.access_token = token

        # 4. Test connection
        self._test_connection()

        return True
    except Exception as e:
        raise AuthenticationError(f"Login failed: {e}")
```

### Data Fetching Pattern
```python
def get_candles(self, symbol: str, exchange: str, interval: str,
               from_date: str, to_date: str, limit: int = 100):
    """Standard data fetching pattern"""
    try:
        # 1. Validate parameters
        self._validate_candle_params(symbol, exchange, interval, from_date, to_date, limit)

        # 2. Resolve instrument
        instrument = self.resolve_equity_instrument(symbol, exchange)

        # 3. Make API call
        response = self._make_api_call("candles", {
            "instrument_token": instrument["broker_token"],
            "interval": interval,
            "from_date": from_date,
            "to_date": to_date,
            "limit": limit
        })

        # 4. Transform response
        candles = self._transform_candles(response)

        return candles
    except Exception as e:
        raise DataError(f"Failed to fetch candles: {e}")
```

### Order Placement Pattern
```python
def place_order(self, symbol: str, exchange: str, transaction_type: str,
               quantity: int, order_type: str, product: str, price: float = None):
    """Standard order placement pattern"""
    try:
        # 1. Validate parameters
        self._validate_order_params(symbol, exchange, transaction_type, quantity, order_type, product, price)

        # 2. Resolve instrument
        instrument = self.resolve_equity_instrument(symbol, exchange)

        # 3. Prepare order data
        order_data = {
            "instrument_token": instrument["broker_token"],
            "transaction_type": transaction_type,
            "quantity": quantity,
            "order_type": order_type,
            "product": product
        }

        if price:
            order_data["price"] = price

        # 4. Place order
        response = self._make_api_call("place_order", order_data)

        # 5. Return order details
        return {
            "order_id": response["order_id"],
            "status": response["status"],
            "message": response["message"]
        }
    except Exception as e:
        raise OrderError(f"Failed to place order: {e}")
```

## Error Handling

### Standard Error Classes
```python
from india_stocks_api.brokers.base.errors import (
    AuthenticationError,
    OrderError,
    DataError,
    InstrumentNotFoundError,
    NetworkError
)
```

### Error Response Format
```python
{
    "error": "Error message",
    "error_code": "ERROR_CODE",
    "details": "Additional error details"
}
```

## Testing Guidelines

### Unit Tests
- Test all public methods
- Test error conditions
- Test data transformation
- Test authentication flows

### Integration Tests
- Test with real API (use test credentials)
- Test end-to-end workflows
- Test error recovery
- Test performance

### Mock Tests
- Mock external API calls
- Test offline scenarios
- Test error conditions
- Test edge cases

## Performance Considerations

### Caching
- Use the base provider's caching mechanism
- Cache frequently accessed data
- Implement proper cache invalidation
- Use date-based cache invalidation

### Rate Limiting
- Respect broker API rate limits
- Implement exponential backoff
- Use connection pooling
- Batch requests where possible

### Error Recovery
- Implement retry logic for transient errors
- Fallback to cached data when available
- Graceful degradation for non-critical operations
- Comprehensive logging for debugging

## Security Considerations

### Credential Management
- Never log sensitive credentials
- Use secure credential storage
- Implement credential rotation
- Validate all inputs

### API Security
- Use HTTPS for all API calls
- Implement proper authentication
- Validate all responses
- Handle errors securely

## Documentation Requirements

### Code Documentation
- Document all public methods
- Include parameter descriptions
- Include return value descriptions
- Include example usage

### API Documentation
- Document all API endpoints
- Include request/response formats
- Include error codes
- Include rate limits

### User Documentation
- Provide usage examples
- Document configuration options
- Include troubleshooting guide
- Include best practices

## Deployment Checklist

### Pre-deployment
- [ ] All tests pass
- [ ] Code is documented
- [ ] Error handling is comprehensive
- [ ] Performance is acceptable
- [ ] Security review completed

### Post-deployment
- [ ] Monitor error rates
- [ ] Monitor performance metrics
- [ ] Monitor API usage
- [ ] Collect user feedback
- [ ] Update documentation

## Support and Maintenance

### Monitoring
- Monitor API response times
- Monitor error rates
- Monitor cache hit rates
- Monitor database performance

### Updates
- Keep up with broker API changes
- Update tests regularly
- Update documentation
- Handle user feedback

### Troubleshooting
- Maintain error logs
- Provide debugging tools
- Document common issues
- Provide support channels
