-- Migration 004: Make instrument_id nullable in broker_instruments
-- Since derivatives now use underlying_instrument_id instead of instrument_id

-- Record migration
INSERT INTO schema_version (version, description)
VALUES (4, 'Make instrument_id nullable for derivatives');

-- SQLite doesn't support ALTER COLUMN, so we need to recreate the table
-- Step 1: Create new broker_instruments table with nullable instrument_id
CREATE TABLE broker_instruments_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id INTEGER,  -- Now nullable
    underlying_instrument_id INTEGER,  -- For derivatives
    broker_name TEXT NOT NULL,
    broker_symbol TEXT NOT NULL,
    broker_token TEXT,
    broker_instrument_key TEXT,
    tick_size REAL,
    lot_size INTEGER DEFAULT 1,
    margin_percentage REAL,
    expiry_date TEXT,
    strike_price REAL,
    option_type TEXT,
    is_tradeable BOOLEAN DEFAULT 1,
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (instrument_id) REFERENCES instruments(id) ON DELETE CASCADE,
    FOREIGN KEY (underlying_instrument_id) REFERENCES instruments(id) ON DELETE CASCADE,
    UNIQUE(broker_name, broker_symbol)
);

-- Step 2: Copy data from old table
INSERT INTO broker_instruments_new
SELECT * FROM broker_instruments;

-- Step 3: Drop old table
DROP TABLE broker_instruments;

-- Step 4: Rename new table
ALTER TABLE broker_instruments_new RENAME TO broker_instruments;

-- Step 5: Recreate indices
CREATE INDEX IF NOT EXISTS idx_broker_instruments_broker_symbol
    ON broker_instruments(broker_name, broker_symbol);
CREATE INDEX IF NOT EXISTS idx_broker_instruments_instrument_id
    ON broker_instruments(instrument_id);
CREATE INDEX IF NOT EXISTS idx_broker_instruments_underlying
    ON broker_instruments(underlying_instrument_id);
CREATE INDEX IF NOT EXISTS idx_broker_instruments_expiry
    ON broker_instruments(expiry_date);
