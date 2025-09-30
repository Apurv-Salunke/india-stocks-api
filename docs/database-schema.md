# Database Schema Documentation

## Overview

The Indian Stock API uses SQLite as its database engine with a carefully designed schema to store instrument data, broker mappings, and exchange information. The database is automatically created and migrated when the package is imported.

## Database Configuration

### SQLite Settings
- **Journal Mode**: WAL (Write-Ahead Logging)
- **Foreign Keys**: Enabled
- **Busy Timeout**: 30 seconds
- **Synchronous**: NORMAL
- **Cache Size**: 2000 pages

### File Structure
```
instruments.db          # Main database file
instruments.db-wal      # Write-ahead log file
instruments.db-shm      # Shared memory file
```

## Schema Overview

### Core Tables
1. **exchanges** - Exchange information
2. **categories** - Instrument categories
3. **instruments** - Standardized instrument data
4. **broker_instruments** - Broker-specific instrument mappings
5. **schema_version** - Migration tracking

## Table Definitions

### 1. exchanges

Stores information about supported exchanges.

```sql
CREATE TABLE exchanges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange_code TEXT NOT NULL UNIQUE,
    exchange_name TEXT NOT NULL,
    country TEXT NOT NULL,
    currency TEXT NOT NULL,
    timezone TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### Columns
- **id**: Primary key
- **exchange_code**: Exchange code (NSE, BSE, MCX, NCDEX)
- **exchange_name**: Full exchange name
- **country**: Country code (INDIA)
- **currency**: Currency code (INR)
- **timezone**: Timezone (Asia/Kolkata)
- **created_at**: Record creation timestamp
- **updated_at**: Record update timestamp

#### Sample Data
```sql
INSERT INTO exchanges (exchange_code, exchange_name, country, currency, timezone) VALUES
('NSE', 'National Stock Exchange of India Ltd', 'INDIA', 'INR', 'Asia/Kolkata'),
('BSE', 'BSE Ltd', 'INDIA', 'INR', 'Asia/Kolkata'),
('MCX', 'Multi Commodity Exchange of India Ltd', 'INDIA', 'INR', 'Asia/Kolkata'),
('NCDEX', 'National Commodity & Derivatives Exchange Ltd', 'INDIA', 'INR', 'Asia/Kolkata');
```

### 2. categories

Stores instrument categories.

```sql
CREATE TABLE categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_code TEXT NOT NULL UNIQUE,
    category_name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### Columns
- **id**: Primary key
- **category_code**: Category code (EQ, FUT, OPT, COM, CUR)
- **category_name**: Full category name
- **created_at**: Record creation timestamp
- **updated_at**: Record update timestamp

#### Sample Data
```sql
INSERT INTO categories (category_code, category_name) VALUES
('EQ', 'Equity'),
('FUT', 'Futures'),
('OPT', 'Options'),
('COM', 'Commodity'),
('CUR', 'Currency');
```

### 3. instruments

Stores standardized instrument information.

```sql
CREATE TABLE instruments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    standardized_symbol TEXT NOT NULL,
    instrument_name TEXT NOT NULL,
    exchange_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    underlying_symbol TEXT,
    expiry_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (exchange_id) REFERENCES exchanges(id),
    FOREIGN KEY (category_id) REFERENCES categories(id),
    UNIQUE(standardized_symbol, exchange_id, category_id, underlying_symbol, expiry_date)
);
```

#### Columns
- **id**: Primary key
- **standardized_symbol**: Standardized symbol (RELIANCE, BANKNIFTY)
- **instrument_name**: Full instrument name
- **exchange_id**: Foreign key to exchanges table
- **category_id**: Foreign key to categories table
- **underlying_symbol**: Underlying symbol for derivatives
- **expiry_date**: Expiry date for derivatives
- **created_at**: Record creation timestamp
- **updated_at**: Record update timestamp

#### Sample Data
```sql
INSERT INTO instruments (standardized_symbol, instrument_name, exchange_id, category_id) VALUES
('RELIANCE', 'Reliance Industries Ltd', 1, 1),
('TCS', 'Tata Consultancy Services Ltd', 1, 1),
('BANKNIFTY', 'Bank Nifty', 1, 2);
```

### 4. broker_instruments

Stores broker-specific instrument mappings.

```sql
CREATE TABLE broker_instruments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER NOT NULL,
    broker_name TEXT NOT NULL,
    broker_symbol TEXT NOT NULL,
    broker_token TEXT NOT NULL,
    tick_size REAL,
    lot_size INTEGER,
    strike_price REAL,
    option_type TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrument_id) REFERENCES instruments(id),
    UNIQUE(instrument_id, broker_name)
);
```

#### Columns
- **id**: Primary key
- **instrument_id**: Foreign key to instruments table
- **broker_name**: Broker name (angelone, zerodha)
- **broker_symbol**: Broker-specific symbol
- **broker_token**: Broker-specific token
- **tick_size**: Minimum price movement
- **lot_size**: Minimum trading quantity
- **strike_price**: Strike price for options
- **option_type**: Option type (CE, PE)
- **created_at**: Record creation timestamp
- **updated_at**: Record update timestamp

#### Sample Data
```sql
INSERT INTO broker_instruments (instrument_id, broker_name, broker_symbol, broker_token, tick_size, lot_size) VALUES
(1, 'angelone', 'RELIANCE-EQ', '2881', 0.05, 1),
(2, 'angelone', 'TCS-EQ', '2882', 0.05, 1),
(3, 'angelone', 'BANKNIFTY24JAN45000CE', '12345', 0.05, 25);
```

### 5. schema_version

Tracks database schema version for migrations.

```sql
CREATE TABLE schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);
```

#### Columns
- **version**: Schema version number
- **applied_at**: When the migration was applied
- **description**: Migration description

#### Sample Data
```sql
INSERT INTO schema_version (version, description) VALUES
(1, 'Initial schema'),
(2, 'Add missing columns');
```

## Indexes

### Performance Indexes
```sql
-- Index on standardized_symbol for fast lookups
CREATE INDEX idx_instruments_symbol ON instruments(standardized_symbol);

-- Index on exchange_id for filtering by exchange
CREATE INDEX idx_instruments_exchange ON instruments(exchange_id);

-- Index on category_id for filtering by category
CREATE INDEX idx_instruments_category ON instruments(category_id);

-- Index on broker_name for filtering by broker
CREATE INDEX idx_broker_instruments_broker ON broker_instruments(broker_name);

-- Index on broker_token for fast token lookups
CREATE INDEX idx_broker_instruments_token ON broker_instruments(broker_token);

-- Composite index for complex queries
CREATE INDEX idx_instruments_composite ON instruments(standardized_symbol, exchange_id, category_id);
```

## Relationships

### Foreign Key Relationships
```
exchanges (1) ←→ (many) instruments
categories (1) ←→ (many) instruments
instruments (1) ←→ (many) broker_instruments
```

### Relationship Diagram
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  exchanges  │    │ categories  │    │ instruments │
├─────────────┤    ├─────────────┤    ├─────────────┤
│ id (PK)     │    │ id (PK)     │    │ id (PK)     │
│ exchange_   │    │ category_   │    │ standard_   │
│ code        │    │ code        │    │ ized_symbol │
│ exchange_   │    │ category_   │    │ instrument_ │
│ name        │    │ name        │    │ name        │
│ country     │    │ created_at  │    │ exchange_id │
│ currency    │    │ updated_at  │    │ category_id │
│ timezone    │    └─────────────┘    │ underlying_ │
│ created_at  │                       │ symbol      │
│ updated_at  │                       │ expiry_date │
└─────────────┘                       │ created_at  │
        │                             │ updated_at  │
        │                             └─────────────┘
        │                                     │
        │                                     │
        │                             ┌─────────────┐
        │                             │broker_      │
        │                             │instruments  │
        │                             ├─────────────┤
        │                             │ id (PK)     │
        │                             │ instrument_ │
        │                             │ id (FK)     │
        │                             │ broker_name │
        │                             │ broker_     │
        │                             │ symbol      │
        │                             │ broker_token│
        │                             │ tick_size   │
        │                             │ lot_size    │
        │                             │ strike_price│
        │                             │ option_type │
        │                             │ created_at  │
        │                             │ updated_at  │
        │                             └─────────────┘
        │
        └─────────────────────────────────────┘
```

## Data Types

### Supported Data Types
- **INTEGER**: Primary keys, foreign keys, quantities
- **TEXT**: Symbols, names, codes, descriptions
- **REAL**: Prices, tick sizes, strike prices
- **DATE**: Expiry dates
- **TIMESTAMP**: Creation and update times

### Data Validation
- **NOT NULL**: Required fields
- **UNIQUE**: Unique constraints
- **FOREIGN KEY**: Referential integrity
- **CHECK**: Value constraints

## Migration System

### Migration Files
Migration files are stored in `india_stocks_api/database/migrations/` and follow the naming convention:
- `001_initial_schema.sql`
- `002_add_missing_columns.sql`
- `003_remove_use_exchange.sql`

### Migration Process
1. Check current schema version
2. Identify pending migrations
3. Apply migrations in order
4. Update schema version
5. Commit transaction

### Migration Example
```sql
-- 002_add_missing_columns.sql
ALTER TABLE broker_instruments ADD COLUMN tick_size REAL;
ALTER TABLE broker_instruments ADD COLUMN lot_size INTEGER;
ALTER TABLE broker_instruments ADD COLUMN strike_price REAL;
ALTER TABLE broker_instruments ADD COLUMN option_type TEXT;
```

## Query Patterns

### Common Queries

#### 1. Resolve Instrument
```sql
SELECT
    i.standardized_symbol,
    i.instrument_name,
    e.exchange_code,
    c.category_code,
    bi.broker_symbol,
    bi.broker_token,
    bi.tick_size,
    bi.lot_size
FROM instruments i
JOIN exchanges e ON i.exchange_id = e.id
JOIN categories c ON i.category_id = c.id
JOIN broker_instruments bi ON i.id = bi.instrument_id
WHERE i.standardized_symbol = 'RELIANCE'
  AND e.exchange_code = 'NSE'
  AND c.category_code = 'EQ'
  AND bi.broker_name = 'angelone';
```

#### 2. Get All Instruments for Broker
```sql
SELECT
    i.standardized_symbol,
    i.instrument_name,
    e.exchange_code,
    c.category_code,
    bi.broker_symbol,
    bi.broker_token
FROM instruments i
JOIN exchanges e ON i.exchange_id = e.id
JOIN categories c ON i.category_id = c.id
JOIN broker_instruments bi ON i.id = bi.instrument_id
WHERE bi.broker_name = 'angelone'
ORDER BY i.standardized_symbol;
```

#### 3. Get F&O Instruments
```sql
SELECT
    i.standardized_symbol,
    i.underlying_symbol,
    i.expiry_date,
    c.category_code,
    bi.broker_symbol,
    bi.broker_token,
    bi.strike_price,
    bi.option_type
FROM instruments i
JOIN categories c ON i.category_id = c.id
JOIN broker_instruments bi ON i.id = bi.instrument_id
WHERE c.category_code IN ('FUT', 'OPT')
  AND bi.broker_name = 'angelone'
ORDER BY i.standardized_symbol, i.expiry_date;
```

#### 4. Get Database Statistics
```sql
SELECT
    (SELECT COUNT(*) FROM instruments) as total_instruments,
    (SELECT COUNT(*) FROM broker_instruments) as total_broker_instruments,
    (SELECT COUNT(*) FROM exchanges) as total_exchanges,
    (SELECT COUNT(*) FROM categories) as total_categories;
```

## Performance Optimization

### Indexing Strategy
- Index frequently queried columns
- Use composite indexes for complex queries
- Monitor query performance
- Update statistics regularly

### Query Optimization
- Use appropriate WHERE clauses
- Limit result sets
- Use JOINs instead of subqueries
- Avoid SELECT * in production

### Database Maintenance
- Regular VACUUM operations
- Analyze query plans
- Monitor database size
- Backup regularly

## Backup and Recovery

### Backup Strategy
- Regular full backups
- Incremental backups
- Test restore procedures
- Store backups securely

### Recovery Procedures
- Restore from backup
- Re-run migrations
- Verify data integrity
- Update application

## Security Considerations

### Access Control
- Limit database access
- Use connection pooling
- Implement proper authentication
- Monitor access logs

### Data Protection
- Encrypt sensitive data
- Use secure connections
- Implement audit trails
- Regular security reviews

## Monitoring and Maintenance

### Performance Monitoring
- Query execution times
- Database size growth
- Index usage statistics
- Connection pool status

### Maintenance Tasks
- Regular VACUUM operations
- Update statistics
- Check for corruption
- Monitor disk space

### Troubleshooting
- Check error logs
- Analyze slow queries
- Monitor resource usage
- Review configuration
