-- =============================================================================
-- 04_optimizations.sql
-- =============================================================================
-- Purpose : Add secondary indexes and aggregating materialized views to the gold
--           layer tables for improved analytical query performance.
--
-- Run after 03_create_fact_tables.sql.  Safe to re-run (IF NOT EXISTS guards).
--
-- Secondary indexes:
--   bloom_filter indexes on carrier and airport allow ClickHouse to quickly skip
--   granules that do not contain a given carrier/airport code in ad-hoc filters.
--   A minmax index on month narrows range scans within a partition.
--
-- Aggregating materialized views:
--   mv_carrier_delay_performance — pre-computes per-carrier delay totals.
--   mv_monthly_delay_trends      — pre-computes per-month delay totals.
--   Both use AggregatingMergeTree so partial aggregates are stored efficiently
--   and queried with the *Merge combinator (e.g. sumMerge).
-- =============================================================================

-- Secondary indexes on fact_flight_delays for common analytical access patterns.
ALTER TABLE gold.fact_flight_delays
    ADD INDEX IF NOT EXISTS idx_carrier carrier TYPE bloom_filter GRANULARITY 1; -- noqa: PRS

ALTER TABLE gold.fact_flight_delays
    ADD INDEX IF NOT EXISTS idx_airport airport TYPE bloom_filter GRANULARITY 1; -- noqa: PRS

ALTER TABLE gold.fact_flight_delays
    ADD INDEX IF NOT EXISTS idx_month month TYPE minmax GRANULARITY 1; -- noqa: PRS


-- Aggregating materialized view: carrier-level delay performance.
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_carrier_delay_performance
ENGINE = AggregatingMergeTree()
ORDER BY (carrier_key, carrier)
AS
SELECT
    carrier_key,
    carrier,
    sumState(arr_flights)         AS total_flights_state,
    sumState(arr_del15)           AS total_delayed_state,
    sumState(arr_delay)           AS total_delay_minutes_state,
    sumState(carrier_delay)       AS total_carrier_delay_state,
    sumState(weather_delay)       AS total_weather_delay_state,
    sumState(nas_delay)           AS total_nas_delay_state,
    sumState(security_delay)      AS total_security_delay_state,
    sumState(late_aircraft_delay) AS total_late_aircraft_delay_state
FROM gold.fact_flight_delays
GROUP BY carrier_key, carrier;


-- Aggregating materialized view: monthly delay trend rollups.
CREATE MATERIALIZED VIEW IF NOT EXISTS gold.mv_monthly_delay_trends
ENGINE = AggregatingMergeTree()
ORDER BY (date_key, year, month)
AS
SELECT
    date_key,
    year,
    month,
    sumState(arr_flights) AS total_flights_state,
    sumState(arr_del15)   AS total_delayed_state,
    sumState(arr_delay)   AS total_delay_minutes_state
FROM gold.fact_flight_delays
GROUP BY date_key, year, month;
