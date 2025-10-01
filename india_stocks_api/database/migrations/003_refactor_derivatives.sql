-- Migration 003: Refactor derivatives to use underlying_instrument_id
-- Purpose: Move futures and options to broker_instruments only, link to underlying instruments
-- Date: 2025-10-01

-- Step 1: Add underlying_instrument_id to broker_instruments
ALTER TABLE broker_instruments ADD COLUMN underlying_instrument_id INTEGER
    REFERENCES instruments(id) ON DELETE CASCADE;

-- Step 2: Create index for performance
CREATE INDEX IF NOT EXISTS idx_broker_instruments_underlying
    ON broker_instruments(underlying_instrument_id);

-- Step 3: Populate underlying_instrument_id for existing derivatives
-- This links each derivative to its underlying instrument (stock/index)
UPDATE broker_instruments
SET underlying_instrument_id = (
    SELECT i2.id
    FROM instruments i1
    JOIN instruments i2 ON i1.underlying_symbol = i2.standardized_symbol
    JOIN instrument_categories ic1 ON i1.category_id = ic1.id
    JOIN instrument_categories ic2 ON i2.category_id = ic2.id
    WHERE broker_instruments.instrument_id = i1.id
    AND ic1.category_code IN ('FUT', 'OPT')
    AND ic2.category_code IN ('EQ', 'INDEX', 'COM')
    AND i1.exchange_id = i2.exchange_id
    LIMIT 1
)
WHERE EXISTS (
    SELECT 1 FROM instruments i
    JOIN instrument_categories ic ON i.category_id = ic.id
    WHERE broker_instruments.instrument_id = i.id
    AND ic.category_code IN ('FUT', 'OPT')
);

-- Step 4: Delete futures and options from instruments table
-- (Keep them only in broker_instruments)
DELETE FROM instruments
WHERE category_id IN (
    SELECT id FROM instrument_categories WHERE category_code IN ('FUT', 'OPT')
);

-- Step 5: Update schema version
INSERT INTO schema_version (version, description, applied_at)
VALUES (3, 'Refactor derivatives to use underlying_instrument_id', CURRENT_TIMESTAMP);
