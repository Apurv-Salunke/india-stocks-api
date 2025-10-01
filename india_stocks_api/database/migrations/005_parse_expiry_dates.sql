-- Migration 005: Parse expiry dates from DDMMMYYYY format to ISO YYYY-MM-DD format
-- This makes dates sortable, comparable, and easier to query

-- Record migration
INSERT INTO schema_version (version, description)
VALUES (5, 'Parse expiry dates to ISO format');

-- Update expiry dates from DDMMMYYYY to YYYY-MM-DD format
-- Format: 01OCT2025 → 2025-10-01
UPDATE broker_instruments
SET expiry_date =
    substr(expiry_date, 6, 4) || '-' ||  -- Year (2025)
    CASE substr(expiry_date, 3, 3)        -- Month name to number
        WHEN 'JAN' THEN '01'
        WHEN 'FEB' THEN '02'
        WHEN 'MAR' THEN '03'
        WHEN 'APR' THEN '04'
        WHEN 'MAY' THEN '05'
        WHEN 'JUN' THEN '06'
        WHEN 'JUL' THEN '07'
        WHEN 'AUG' THEN '08'
        WHEN 'SEP' THEN '09'
        WHEN 'OCT' THEN '10'
        WHEN 'NOV' THEN '11'
        WHEN 'DEC' THEN '12'
    END || '-' ||
    substr('00' || substr(expiry_date, 1, 2), -2, 2)  -- Day (01)
WHERE expiry_date IS NOT NULL
AND length(expiry_date) >= 9  -- Only convert DDMMMYYYY format
AND substr(expiry_date, 3, 3) IN ('JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC');

-- Also update instruments table if any expiry dates exist there
UPDATE instruments
SET expiry_date =
    substr(expiry_date, 6, 4) || '-' ||  -- Year
    CASE substr(expiry_date, 3, 3)
        WHEN 'JAN' THEN '01'
        WHEN 'FEB' THEN '02'
        WHEN 'MAR' THEN '03'
        WHEN 'APR' THEN '04'
        WHEN 'MAY' THEN '05'
        WHEN 'JUN' THEN '06'
        WHEN 'JUL' THEN '07'
        WHEN 'AUG' THEN '08'
        WHEN 'SEP' THEN '09'
        WHEN 'OCT' THEN '10'
        WHEN 'NOV' THEN '11'
        WHEN 'DEC' THEN '12'
    END || '-' ||
    substr('00' || substr(expiry_date, 1, 2), -2, 2)
WHERE expiry_date IS NOT NULL
AND length(expiry_date) >= 9
AND substr(expiry_date, 3, 3) IN ('JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC');
