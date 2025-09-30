# Architecture Overview

## System Architecture

The Indian Stock API follows a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    User Application                         │
├─────────────────────────────────────────────────────────────┤
│                    Broker Interface                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   AngelOne  │  │   Zerodha   │  │   Upstox    │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
├─────────────────────────────────────────────────────────────┤
│                  Base Provider Layer                        │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                BaseProvider                             │ │
│  │  • Caching • Storage • Utilities • Common Methods      │ │
│  └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                  Data Processing Layer                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │InstrumentStore│ │MigrationMgr│ │DatabaseUtils│        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
├─────────────────────────────────────────────────────────────┤
│                    Database Layer                           │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                    SQLite Database                      │ │
│  │  • Instruments • Broker Instruments • Exchanges        │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Broker Interface Layer
**Purpose**: Provides unified API for trading operations

**Components**:
- `AngelOne`: AngelOne broker implementation
- `Zerodha`: Zerodha broker implementation (planned)
- `Upstox`: Upstox broker implementation (planned)

**Responsibilities**:
- User authentication
- Order placement
- Data fetching (candles, quotes)
- Portfolio management

### 2. Base Provider Layer
**Purpose**: Common functionality shared across all brokers

**Components**:
- `BaseProvider`: Abstract base class
- `AngelOneTokensManager`: AngelOne-specific data provider

**Responsibilities**:
- Date-based caching
- Instrument data fetching
- Data transformation
- Common utilities

### 3. Data Processing Layer
**Purpose**: Handles data storage, retrieval, and schema management

**Components**:
- `InstrumentStore`: Database operations
- `MigrationManager`: Schema versioning
- `DatabaseUtils`: Database utilities

**Responsibilities**:
- Instrument storage and retrieval
- Database migrations
- Schema validation
- Data consistency

### 4. Database Layer
**Purpose**: Persistent storage for instrument data

**Components**:
- SQLite database
- Tables: `instruments`, `broker_instruments`, `exchanges`, `categories`

**Responsibilities**:
- Instrument data persistence
- Fast lookups
- Data integrity
- Concurrent access

## Data Flow

### 1. Instrument Resolution Flow
```
User Request → Broker Interface → Base Provider → InstrumentStore → Database
     ↓              ↓                ↓              ↓              ↓
"RELIANCE" → resolve_equity() → _get_token() → upsert_equities() → SQLite
```

### 2. Data Fetching Flow
```
User Request → Broker Interface → External API → Data Transform → Cache → User
     ↓              ↓                ↓              ↓            ↓       ↓
get_candles() → fetch_data() → AngelOne API → standardize() → Cache → Response
```

### 3. Order Placement Flow
```
User Request → Broker Interface → Token Resolution → Order API → Response
     ↓              ↓                ↓              ↓          ↓
place_order() → resolve_token() → get_token() → Broker API → Order ID
```

## Key Design Patterns

### 1. Base Provider Pattern
```python
class BaseProvider(ABC):
    # Common functionality
    def _read_cache(self): ...
    def _write_cache(self): ...
    def _store_equity_data(self): ...

class AngelOneTokensManager(BaseProvider):
    # AngelOne-specific implementation
    def _fetch_market_data(self): ...
    def _is_equity_instrument(self): ...
```

### 2. Repository Pattern
```python
class InstrumentStore:
    def upsert_equities(self, data): ...
    def upsert_fno(self, data): ...
    def resolve_instrument(self, symbol, broker, exchange): ...
```

### 3. Migration Pattern
```python
class MigrationManager:
    def create_database(self): ...
    def run_all_migrations(self): ...
    def get_pending_migrations(self): ...
```

## Database Schema

### Core Tables

#### `exchanges`
- `id`: Primary key
- `exchange_code`: NSE, BSE, MCX, NCDEX
- `exchange_name`: Full name
- `country`, `currency`, `timezone`: Metadata

#### `instruments`
- `id`: Primary key
- `standardized_symbol`: RELIANCE, BANKNIFTY
- `instrument_name`: Full name
- `exchange_id`: Foreign key to exchanges
- `category_id`: Foreign key to categories
- `underlying_symbol`, `expiry_date`: For derivatives

#### `broker_instruments`
- `id`: Primary key
- `instrument_id`: Foreign key to instruments
- `broker_name`: angelone, zerodha
- `broker_symbol`: Broker-specific symbol
- `broker_token`: Broker-specific token
- `tick_size`, `lot_size`: Trading parameters

#### `categories`
- `id`: Primary key
- `category_code`: EQ, FUT, OPT, COM, CUR
- `category_name`: Equity, Futures, Options, Commodity, Currency

## Caching Strategy

### Date-Based Cache Invalidation
- Cache invalidates at midnight IST (Indian Standard Time)
- Valid for entire trading day
- Automatic refresh on new trading day
- UTC timestamps for consistency

### Cache Structure
```json
{
  "timestamp": "2024-01-01T02:14:42.066641+00:00",
  "data": [
    {
      "token": "2881",
      "symbol": "RELIANCE-EQ",
      "name": "Reliance Industries Ltd",
      "exch_seg": "NSE",
      "instrumenttype": "EQ"
    }
  ]
}
```

## Error Handling

### Hierarchical Error Handling
1. **API Level**: Network errors, rate limits
2. **Data Level**: Invalid responses, missing fields
3. **Database Level**: Connection errors, constraint violations
4. **Application Level**: Business logic errors

### Error Recovery
- Automatic retry for transient errors
- Fallback to cached data when available
- Graceful degradation for non-critical operations
- Comprehensive logging for debugging

## Performance Considerations

### Database Optimization
- WAL mode for concurrent access
- Indexes on frequently queried columns
- Bulk operations for data loading
- Connection pooling

### Caching Strategy
- In-memory caching for hot data
- File-based caching for persistence
- Date-based invalidation
- Lazy loading for large datasets

### API Optimization
- Request batching where possible
- Rate limiting compliance
- Connection reuse
- Timeout handling

## Security Considerations

### Data Protection
- No sensitive data in logs
- Secure credential storage
- API key rotation support
- Input validation

### Access Control
- Broker-specific authentication
- Session management
- Rate limiting
- Error message sanitization

## Monitoring and Observability

### Logging
- Structured logging with levels
- Request/response logging
- Performance metrics
- Error tracking

### Metrics
- API response times
- Cache hit rates
- Database query performance
- Error rates

### Health Checks
- Database connectivity
- API availability
- Cache status
- Migration status
