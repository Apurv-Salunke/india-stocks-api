# API Reference

## Broker Interface

### AngelOne

#### Authentication
```python
from india_stocks_api import brokers

broker = brokers.AngelOne()
broker.login(
    api_key="your_api_key",
    username="your_username",
    password="your_password"
)
```

#### Fetch Candles
```python
candles = broker.get_candles(
    symbol="RELIANCE",
    exchange="NSE",
    interval="1minute",
    from_date="2024-01-01",
    to_date="2024-01-31",
    limit=100
)
```

**Parameters**:
- `symbol` (str): Standard symbol (RELIANCE, BANKNIFTY)
- `exchange` (str): Exchange code (NSE, BSE, MCX, NCDEX)
- `interval` (str): Timeframe (1minute, 5minute, 1day)
- `from_date` (str): Start date (YYYY-MM-DD)
- `to_date` (str): End date (YYYY-MM-DD)
- `limit` (int): Maximum number of candles

**Returns**: List of candle dictionaries
```python
[
    {
        "date": "2024-01-01T09:15:00+05:30",
        "open": 2500.0,
        "high": 2520.0,
        "low": 2490.0,
        "close": 2510.0,
        "volume": 1000000
    }
]
```

#### Place Order
```python
order = broker.place_order(
    symbol="RELIANCE",
    exchange="NSE",
    transaction_type="BUY",
    quantity=10,
    order_type="MARKET",
    product="INTRADAY",
    price=2500.0  # Required for LIMIT orders
)
```

**Parameters**:
- `symbol` (str): Standard symbol
- `exchange` (str): Exchange code
- `transaction_type` (str): BUY or SELL
- `quantity` (int): Number of shares/lots
- `order_type` (str): MARKET or LIMIT
- `product` (str): INTRADAY, DELIVERY, MIS
- `price` (float): Price for LIMIT orders

**Returns**: Order dictionary
```python
{
    "order_id": "123456789",
    "status": "OPEN",
    "message": "Order placed successfully"
}
```

#### Get Quote
```python
quote = broker.get_quote("RELIANCE", "NSE")
```

**Returns**: Quote dictionary
```python
{
    "symbol": "RELIANCE",
    "last_price": 2510.0,
    "open": 2500.0,
    "high": 2520.0,
    "low": 2490.0,
    "close": 2495.0,
    "volume": 1000000,
    "change": 15.0,
    "change_percent": 0.6
}
```

#### Get Multiple Quotes
```python
quotes = broker.get_quotes([
    {"symbol": "RELIANCE", "exchange": "NSE"},
    {"symbol": "TCS", "exchange": "NSE"},
    {"symbol": "INFY", "exchange": "NSE"}
])
```

#### Get Positions
```python
positions = broker.get_positions()
```

**Returns**: List of position dictionaries
```python
[
    {
        "symbol": "RELIANCE",
        "quantity": 10,
        "average_price": 2500.0,
        "current_price": 2510.0,
        "pnl": 100.0,
        "pnl_percent": 0.4
    }
]
```

#### Get Holdings
```python
holdings = broker.get_holdings()
```

**Returns**: List of holding dictionaries
```python
[
    {
        "symbol": "RELIANCE",
        "quantity": 100,
        "average_price": 2400.0,
        "current_price": 2510.0,
        "pnl": 11000.0,
        "pnl_percent": 4.58
    }
]
```

#### Get P&L
```python
pnl = broker.get_pnl()
```

**Returns**: P&L dictionary
```python
{
    "total_pnl": 15000.0,
    "day_pnl": 1000.0,
    "unrealized_pnl": 5000.0,
    "realized_pnl": 10000.0
}
```

## Data Provider Interface

### AngelOneTokensManager

#### Fetch Equity Data
```python
from india_stocks_api.database.providers import AngelOneTokensManager
from india_stocks_api.database import InstrumentService

service = InstrumentService()
provider = AngelOneTokensManager(service, "angelone")

equity_data = provider.fetch_equity_data(force_refresh=False)
```

**Parameters**:
- `force_refresh` (bool): Force fresh data fetch

**Returns**: List of equity instrument dictionaries

#### Fetch F&O Data
```python
fno_data = provider.fetch_fno_data(force_refresh=False)
```

#### Fetch Commodity Data
```python
commodity_data = provider.fetch_commodity_data(force_refresh=False)
```

#### Fetch Currency Data
```python
currency_data = provider.fetch_currency_data(force_refresh=False)
```

#### Sync All Instruments
```python
results = provider.sync_all_instruments()
```

**Returns**: Dictionary with sync results
```python
{
    "equity": 1500,
    "fno": 5000,
    "commodity": 200,
    "currency": 50
}
```

#### Clear Cache
```python
success = provider.clear_cache()
```

**Returns**: Boolean indicating success

#### Check New Trading Day
```python
is_new_day = provider.is_new_trading_day()
```

**Returns**: Boolean indicating if it's a new trading day

## Database Interface

### InstrumentService

#### Initialize Database
```python
from india_stocks_api.database import InstrumentService

service = InstrumentService()  # Uses default database path
# or
service = InstrumentService("/path/to/database.db")
```

#### Resolve Instrument
```python
instrument = service.resolve_instrument(
    standardized_symbol="RELIANCE",
    broker_name="angelone",
    exchange=Exchange.NSE,
    category=InstrumentCategory.EQUITY
)
```

**Returns**: Instrument dictionary with broker-specific data

#### Get Database Stats
```python
stats = service.get_database_stats()
```

**Returns**: Statistics dictionary
```python
{
    "total_instruments": 10000,
    "total_broker_instruments": 15000,
    "total_exchanges": 4,
    "total_categories": 5
}
```

#### Auto Populate Data
```python
service.auto_populate_data()
```

Populates database with initial instrument data if empty.

### InstrumentStore

#### Upsert Equities
```python
from india_stocks_api.database.services import InstrumentStore

store = InstrumentStore("/path/to/database.db")
count = store.upsert_equities(equity_data)
```

#### Upsert F&O
```python
count = store.upsert_fno(fno_data)
```

#### Upsert Commodities
```python
count = store.upsert_commodities(commodity_data)
```

#### Upsert Currencies
```python
count = store.upsert_currencies(currency_data)
```

### MigrationManager

#### Create Database
```python
from india_stocks_api.database.migrations import MigrationManager

manager = MigrationManager("/path/to/database.db")
manager.create_database()
```

#### Run All Migrations
```python
manager.run_all_migrations()
```

#### Get Pending Migrations
```python
pending = manager.get_pending_migrations()
```

**Returns**: List of pending migration filenames

#### Get Database Info
```python
info = manager.get_database_info()
```

**Returns**: Database information dictionary

## Enums

### Exchange
```python
from india_stocks_api.database.models.enums import Exchange

Exchange.NSE      # National Stock Exchange
Exchange.BSE      # Bombay Stock Exchange
Exchange.MCX      # Multi Commodity Exchange
Exchange.NCDEX    # National Commodity & Derivatives Exchange
```

### InstrumentCategory
```python
from india_stocks_api.database.models.enums import InstrumentCategory

InstrumentCategory.EQUITY     # Equity instruments
InstrumentCategory.FUTURES    # Futures instruments
InstrumentCategory.OPTIONS    # Options instruments
InstrumentCategory.COMMODITY  # Commodity instruments
InstrumentCategory.CURRENCY   # Currency instruments
```

### OptionType
```python
from india_stocks_api.database.models.enums import OptionType

OptionType.CALL  # Call options
OptionType.PUT   # Put options
```

## Error Handling

### Common Exceptions

#### AuthenticationError
```python
from india_stocks_api.brokers.base.errors import AuthenticationError

try:
    broker.login("invalid", "credentials")
except AuthenticationError as e:
    print(f"Login failed: {e}")
```

#### InstrumentNotFoundError
```python
from india_stocks_api.brokers.base.errors import InstrumentNotFoundError

try:
    broker.get_quote("INVALID", "NSE")
except InstrumentNotFoundError as e:
    print(f"Instrument not found: {e}")
```

#### OrderError
```python
from india_stocks_api.brokers.base.errors import OrderError

try:
    broker.place_order("RELIANCE", "NSE", "BUY", 0, "MARKET", "INTRADAY")
except OrderError as e:
    print(f"Order failed: {e}")
```

### Error Response Format
```python
{
    "error": "Error message",
    "error_code": "ERROR_CODE",
    "details": "Additional error details"
}
```

## Configuration

### Network Configuration
```python
from india_stocks_api.config.network import NetworkConfig

# Configure retry strategy
NetworkConfig.max_retries = 3
NetworkConfig.retry_delay = 1.0
NetworkConfig.timeout = 30.0
```

### Cache Configuration
```python
from india_stocks_api.utils.cache_utils import get_cache_file_path

# Get cache file path
cache_path = get_cache_file_path("custom_cache.json")
```

### Database Configuration
```python
from india_stocks_api.utils.cache_utils import get_database_path

# Get database path
db_path = get_database_path()
```

## Best Practices

### 1. Error Handling
Always wrap API calls in try-catch blocks:
```python
try:
    order = broker.place_order("RELIANCE", "NSE", "BUY", 10, "MARKET", "INTRADAY")
    print(f"Order placed: {order['order_id']}")
except Exception as e:
    print(f"Order failed: {e}")
```

### 2. Caching
Use caching to avoid unnecessary API calls:
```python
# Check if it's a new trading day
if provider.is_new_trading_day():
    # Fetch fresh data
    data = provider.fetch_equity_data(force_refresh=True)
else:
    # Use cached data
    data = provider.fetch_equity_data(force_refresh=False)
```

### 3. Database Management
Let the system handle database initialization:
```python
# Database is automatically created and migrated
service = InstrumentService()
# No manual setup required
```

### 4. Broker Switching
Use the same code pattern for all brokers:
```python
# Easy broker switching
brokers = [brokers.AngelOne(), brokers.Zerodha()]

for broker in brokers:
    broker.login("credentials...")
    # Same trading logic works for all brokers
    candles = broker.get_candles("RELIANCE", "NSE", "1minute", limit=100)
```
