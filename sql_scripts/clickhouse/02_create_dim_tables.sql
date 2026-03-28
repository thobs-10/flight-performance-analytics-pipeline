-- =============================================================================
-- 02_create_dim_tables.sql
-- =============================================================================
-- Purpose : Create the three gold dimension tables in ClickHouse.
--
-- Run after 01_create_databases.sql.  Safe to re-run (IF NOT EXISTS guards).
--
-- Engine choice: ReplacingMergeTree
--   Dimensions are small and change infrequently.  ReplacingMergeTree
--   deduplicates
--   rows sharing the same ORDER BY key, keeping the version with the highest
--   _updated_at value.  This allows Dagster gold assets to truncate+reload
--   without leaving stale rows, while ClickHouse merges happen asynchronously
--   in the background.
--
-- Tables:
--   gold.dim_carrier  — one row per IATA airline carrier
--   gold.dim_airport  — one row per IATA airport
--   gold.dim_date     — one row per (year, month) period with derived
--     attributes
-- =============================================================================

-- Dimension tables for the gold layer.
-- ReplacingMergeTree deduplicates on ORDER BY key, using _updated_at as the
-- version.

CREATE TABLE IF NOT EXISTS gold.dim_carrier
(
    carrier_key String,
    carrier String,
    carrier_name String,
    _updated_at Datetime DEFAULT now()
)
ENGINE = replacingmergetree(_updated_at)
ORDER BY carrier_key;


CREATE TABLE IF NOT EXISTS gold.dim_airport
(
    airport_key String,
    airport String,
    airport_name String,
    _updated_at Datetime DEFAULT now()
)
ENGINE = replacingmergetree(_updated_at)
ORDER BY airport_key;


CREATE TABLE IF NOT EXISTS gold.dim_date
(
    date_key Uint32,
    `year` Uint16,
    `month` Uint8,
    `quarter` Uint8,
    month_name String,
    `year_month` String,
    _updated_at Datetime DEFAULT now()
)
ENGINE = replacingmergetree(_updated_at)
ORDER BY date_key;
