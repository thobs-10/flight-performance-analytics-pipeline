-- =============================================================================
-- 03_create_fact_tables.sql
-- =============================================================================
-- Purpose : Create the central fact table for flight delay metrics in ClickHouse.
--
-- Run after 02_create_dim_tables.sql.  Safe to re-run (IF NOT EXISTS guard).
--
-- Engine choice: MergeTree
--   Standard MergeTree is ideal for append-heavy analytical workloads.  New data
--   is inserted by the Dagster gold asset on each pipeline run; historical data is
--   not mutated in place.
--
-- Partitioning: PARTITION BY year
--   Allows ClickHouse to skip entire partitions for year-filtered queries.
--   Also makes it easy to drop or reload a specific year if needed.
--
-- Ordering: (carrier_key, airport_key, date_key)
--   Matches the most common analytical access pattern: filter by carrier or
--   airport, then slice by date.  Rows with the same prefix are co-located,
--   reducing I/O for range scans.
--
-- Grain: one row per airline_delay_id (year + month + carrier + airport).
-- =============================================================================

-- Central fact table for flight delay metrics.
-- Partitioned by year for efficient time-range queries.
-- Ordered by (carrier_key, airport_key, date_key) to optimise common filter patterns.

CREATE TABLE IF NOT EXISTS gold.fact_flight_delays
(
    airline_delay_id String,
    date_key Uint32,
    carrier_key String,
    airport_key String,
    year Uint16,
    month Uint8,
    carrier String,
    carrier_name String,
    airport String,
    airport_name String,
    arr_flights Int32,
    arr_del15 Int32,
    arr_cancelled Int32,
    arr_diverted Int32,
    carrier_ct Decimal(10, 2),
    weather_ct Decimal(10, 2),
    nas_ct Decimal(10, 2),
    security_ct Decimal(10, 2),
    late_aircraft_ct Decimal(10, 2),
    arr_delay Decimal(10, 2),
    carrier_delay Decimal(10, 2),
    weather_delay Decimal(10, 2),
    nas_delay Decimal(10, 2),
    security_delay Decimal(10, 2),
    late_aircraft_delay Decimal(10, 2),
    _ingested_at Datetime
)
ENGINE = MERGETREE()
PARTITION BY year
ORDER BY (carrier_key, airport_key, date_key)
SETTINGS index_granularity = 8192;
