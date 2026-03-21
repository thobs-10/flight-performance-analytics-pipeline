CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.stg_airline_delay_data (
    airline_delay_id  VARCHAR(32)     NOT NULL,
    year              INTEGER         NOT NULL,
    month             INTEGER         NOT NULL,
    carrier           VARCHAR(10)     NOT NULL,
    carrier_name      VARCHAR(255),
    airport           VARCHAR(10)     NOT NULL,
    airport_name      VARCHAR(255),
    arr_flights       INTEGER,
    arr_del15         INTEGER,
    carrier_ct        NUMERIC(10, 2),
    weather_ct        NUMERIC(10, 2),
    nas_ct            NUMERIC(10, 2),
    security_ct       NUMERIC(10, 2),
    late_aircraft_ct  NUMERIC(10, 2),
    arr_cancelled     INTEGER,
    arr_diverted      INTEGER,
    arr_delay         NUMERIC(10, 2),
    carrier_delay     NUMERIC(10, 2),
    weather_delay     NUMERIC(10, 2),
    nas_delay         NUMERIC(10, 2),
    security_delay    NUMERIC(10, 2),
    late_aircraft_delay NUMERIC(10, 2),
    _ingested_at      TIMESTAMPTZ
);
