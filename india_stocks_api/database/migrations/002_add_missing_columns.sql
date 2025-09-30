-- Add missing columns for F&O and trading instruments
-- Version: 2.0
-- Created: 2024-01-XX

-- Insert version tracking
INSERT OR IGNORE INTO schema_version (version, description)
VALUES (2, 'Add missing columns for F&O instruments');

-- Add missing columns to instruments table
ALTER TABLE instruments ADD COLUMN strike_price REAL;
ALTER TABLE instruments ADD COLUMN option_type TEXT;  -- CE, PE
ALTER TABLE instruments ADD COLUMN tick_size REAL;
ALTER TABLE instruments ADD COLUMN lot_size INTEGER;

-- Add indexes for new columns
CREATE INDEX IF NOT EXISTS idx_instruments_strike_price ON instruments(strike_price);
CREATE INDEX IF NOT EXISTS idx_instruments_option_type ON instruments(option_type);
CREATE INDEX IF NOT EXISTS idx_instruments_tick_size ON instruments(tick_size);
CREATE INDEX IF NOT EXISTS idx_instruments_lot_size ON instruments(lot_size);
