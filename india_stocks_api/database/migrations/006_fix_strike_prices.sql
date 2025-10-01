-- Migration 006: Fix strike prices - divide by 100
-- Strike prices from AngelOne API are in paisa (like tick_size)
-- They were stored without dividing by 100, so they're 100x too large

-- Record migration
INSERT INTO schema_version (version, description)
VALUES (6, 'Fix strike prices (divide by 100)');

-- Update strike prices in broker_instruments
UPDATE broker_instruments
SET strike_price = strike_price / 100
WHERE strike_price IS NOT NULL
AND strike_price != -1;  -- Don't modify the -1 sentinel value

-- Also update instruments table if any strike prices exist there
UPDATE instruments
SET strike_price = strike_price / 100
WHERE strike_price IS NOT NULL
AND strike_price != -1;
