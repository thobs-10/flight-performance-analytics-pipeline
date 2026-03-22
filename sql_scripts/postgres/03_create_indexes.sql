CREATE UNIQUE INDEX IF NOT EXISTS uidx_stg_airline_delay_data_id
ON staging.stg_airline_delay_data (airline_delay_id);

CREATE INDEX IF NOT EXISTS idx_stg_airline_delay_data_key
ON staging.stg_airline_delay_data (year, month, carrier, airport);
