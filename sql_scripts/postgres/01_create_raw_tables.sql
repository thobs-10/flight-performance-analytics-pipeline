-- Bronze layer: raw airline delay data ingested from CSV
-- Source: BTS On-Time Performance (ot_delaycause1_DL)

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.airline_delay_data (
    year INTEGER,
    month INTEGER,
    carrier VARCHAR(10),
    carrier_name VARCHAR(100),
    airport VARCHAR(10),
    airport_name VARCHAR(200),
    arr_flights NUMERIC,
    arr_del15 NUMERIC,
    carrier_ct NUMERIC,
    weather_ct NUMERIC,
    nas_ct NUMERIC,
    security_ct NUMERIC,
    late_aircraft_ct NUMERIC,
    arr_cancelled NUMERIC,
    arr_diverted NUMERIC,
    arr_delay NUMERIC,
    carrier_delay NUMERIC,
    weather_delay NUMERIC,
    nas_delay NUMERIC,
    security_delay NUMERIC,
    late_aircraft_delay NUMERIC,
    _ingested_at TIMESTAMPTZ DEFAULT now()
);
