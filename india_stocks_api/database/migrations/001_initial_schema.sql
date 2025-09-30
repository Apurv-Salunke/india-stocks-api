-- Initial schema for India Stocks API instrument database
-- Version: 1.0
-- Created: 2024-01-XX

-- Create version tracking table
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

-- Insert initial version
INSERT OR IGNORE INTO schema_version (version, description)
VALUES (1, 'Initial schema creation');

-- Exchanges table
CREATE TABLE IF NOT EXISTS exchanges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange_code TEXT NOT NULL UNIQUE,     -- NSE, BSE, MCX, NCDEX, etc.
    exchange_name TEXT NOT NULL,            -- Full name
    country TEXT DEFAULT 'INDIA',
    currency TEXT DEFAULT 'INR',
    timezone TEXT DEFAULT 'Asia/Kolkata',
    trading_hours TEXT,                     -- JSON format
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Instrument categories
CREATE TABLE IF NOT EXISTS instrument_categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_code TEXT NOT NULL UNIQUE,     -- EQ, FUT, OPT, COM, CUR, etc.
    category_name TEXT NOT NULL,            -- Equity, Futures, Options, Commodity, Currency
    parent_category_id INTEGER,             -- For hierarchical categories
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (parent_category_id) REFERENCES instrument_categories(id)
);

-- Sub-categories for detailed classification
CREATE TABLE IF NOT EXISTS instrument_subcategories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_id INTEGER NOT NULL,
    subcategory_code TEXT NOT NULL,         -- METALS, ENERGY, AGRICULTURE, etc.
    subcategory_name TEXT NOT NULL,         -- Precious Metals, Energy, Agriculture
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES instrument_categories(id)
);

-- Main instruments table (standardized format)
CREATE TABLE IF NOT EXISTS instruments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    standardized_symbol TEXT NOT NULL,      -- RELIANCE, GOLD, USDINR, etc.
    instrument_name TEXT NOT NULL,          -- Full instrument name
    exchange_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    subcategory_id INTEGER,

    -- Common fields
    isin TEXT,                              -- For equity instruments
    sector TEXT,                            -- For equity
    industry TEXT,                          -- For equity
    market_cap REAL,                        -- For equity
    face_value REAL,                        -- For equity/bonds

    -- Commodity specific
    commodity_type TEXT,                    -- METAL, ENERGY, AGRICULTURE
    commodity_unit TEXT,                    -- KG, TONNE, BARREL, etc.
    delivery_center TEXT,                   -- For commodities

    -- Currency specific
    base_currency TEXT,                     -- USD, EUR, etc.
    quote_currency TEXT,                    -- INR, USD, etc.

    -- Derivative specific
    underlying_symbol TEXT,                 -- For F&O instruments
    underlying_type TEXT,                   -- EQUITY, INDEX, COMMODITY, CURRENCY

    -- Status and metadata
    is_active BOOLEAN DEFAULT 1,
    listing_date DATE,
    expiry_date DATE,                       -- For derivatives
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (exchange_id) REFERENCES exchanges(id),
    FOREIGN KEY (category_id) REFERENCES instrument_categories(id),
    FOREIGN KEY (subcategory_id) REFERENCES instrument_subcategories(id),
    UNIQUE(standardized_symbol, exchange_id, category_id)
);

-- Broker-specific instrument mappings
CREATE TABLE IF NOT EXISTS broker_instruments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER NOT NULL,
    broker_name TEXT NOT NULL,              -- angelone, zerodha, etc.

    -- Broker-specific identifiers
    broker_symbol TEXT NOT NULL,            -- Broker's symbol format
    broker_token TEXT NOT NULL,             -- Broker's internal token
    broker_instrument_key TEXT,             -- Alternative identifier

    -- Trading specifications
    tick_size REAL NOT NULL,                -- Minimum price movement
    lot_size INTEGER NOT NULL,              -- Minimum trading quantity
    margin_percentage REAL,                 -- Margin requirement

    -- Derivative specific
    expiry_date DATE,                       -- For derivatives
    strike_price REAL,                      -- For options
    option_type TEXT,                       -- CE/PE for options

    -- Trading status
    is_tradeable BOOLEAN DEFAULT 1,         -- Can be traded
    is_active BOOLEAN DEFAULT 1,            -- Active in broker system

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (instrument_id) REFERENCES instruments(id),
    UNIQUE(broker_name, broker_symbol, expiry_date, strike_price, option_type)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_instruments_standardized ON instruments(standardized_symbol);
CREATE INDEX IF NOT EXISTS idx_instruments_exchange ON instruments(exchange_id);
CREATE INDEX IF NOT EXISTS idx_instruments_category ON instruments(category_id);
CREATE INDEX IF NOT EXISTS idx_instruments_underlying ON instruments(underlying_symbol);
CREATE INDEX IF NOT EXISTS idx_instruments_expiry ON instruments(expiry_date);
CREATE INDEX IF NOT EXISTS idx_instruments_isin ON instruments(isin);

CREATE INDEX IF NOT EXISTS idx_broker_instruments_broker ON broker_instruments(broker_name);
CREATE INDEX IF NOT EXISTS idx_broker_instruments_symbol ON broker_instruments(broker_symbol);
CREATE INDEX IF NOT EXISTS idx_broker_instruments_token ON broker_instruments(broker_token);
CREATE INDEX IF NOT EXISTS idx_broker_instruments_expiry ON broker_instruments(expiry_date);
CREATE INDEX IF NOT EXISTS idx_broker_instruments_strike ON broker_instruments(strike_price);
CREATE INDEX IF NOT EXISTS idx_broker_instruments_option_type ON broker_instruments(option_type);
CREATE INDEX IF NOT EXISTS idx_broker_instruments_instrument_id ON broker_instruments(instrument_id);

-- Insert reference data for exchanges
INSERT OR IGNORE INTO exchanges (exchange_code, exchange_name, country, currency, timezone) VALUES
('NSE', 'National Stock Exchange', 'INDIA', 'INR', 'Asia/Kolkata'),
('BSE', 'Bombay Stock Exchange', 'INDIA', 'INR', 'Asia/Kolkata'),
('MCX', 'Multi Commodity Exchange', 'INDIA', 'INR', 'Asia/Kolkata'),
('NCDEX', 'National Commodity & Derivatives Exchange', 'INDIA', 'INR', 'Asia/Kolkata'),
('ICEX', 'Indian Commodity Exchange', 'INDIA', 'INR', 'Asia/Kolkata'),
('BSE_CDS', 'BSE Currency Derivatives', 'INDIA', 'INR', 'Asia/Kolkata'),
('USE', 'United Stock Exchange', 'INDIA', 'INR', 'Asia/Kolkata');

-- Insert reference data for instrument categories
INSERT OR IGNORE INTO instrument_categories (category_code, category_name, description) VALUES
('EQ', 'Equity', 'Equity stocks and shares'),
('FUT', 'Futures', 'Future contracts'),
('OPT', 'Options', 'Option contracts'),
('COM', 'Commodity', 'Commodity instruments'),
('CUR', 'Currency', 'Currency instruments'),
('DEBT', 'Debt', 'Debt instruments and bonds'),
('ETF', 'Exchange Traded Fund', 'Exchange traded funds'),
('REIT', 'Real Estate Investment Trust', 'Real estate investment trusts'),
('INVIT', 'Infrastructure Investment Trust', 'Infrastructure investment trusts'),
('MF', 'Mutual Fund', 'Mutual funds'),
('BOND', 'Bond', 'Government and corporate bonds');

-- Insert reference data for commodity subcategories
INSERT OR IGNORE INTO instrument_subcategories (category_id, subcategory_code, subcategory_name, description) VALUES
(4, 'METALS', 'Metals', 'All metal commodities'),
(4, 'PRECIOUS_METALS', 'Precious Metals', 'Gold, Silver, Platinum, Palladium'),
(4, 'INDUSTRIAL_METALS', 'Industrial Metals', 'Copper, Zinc, Lead, Nickel, Aluminum'),
(4, 'ENERGY', 'Energy', 'Crude Oil, Natural Gas, Coal, etc.'),
(4, 'AGRICULTURE', 'Agriculture', 'Agricultural commodities'),
(4, 'SOFT_COMMODITIES', 'Soft Commodities', 'Cotton, Sugar, Coffee, etc.');
