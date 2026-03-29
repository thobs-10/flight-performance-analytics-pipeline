/*
  fact_flight_delays — Central fact table for the gold layer.

  Joins cleaned staging data to all three dimension tables to produce a fully
  conformed fact table with every delay metric and its FK references.

  Grain    : one row per unique (year, month, carrier, airport) combination,
             identified by airline_delay_id.
  Source   : staging.stg_airline_delay_data
  Targets  : marts.fact_flight_delays  (Postgres)
             gold.fact_flight_delays   (ClickHouse, via Dagster gold asset)
*/

with

staging as (
    select * from {{ ref('stg_airline_delay_data') }}
),

dim_carrier as (
    select *
    from {{ ref('dim_carrier') }}
),

dim_airport as (
    select *
    from {{ ref('dim_airport') }}
),

dim_date as (
    select *
    from {{ ref('dim_date') }}
),

final as (
    select
        s.airline_delay_id,
        d.date_key,
        dc.carrier_key,
        da.airport_key,
        s.year,
        s.month,
        s.carrier,
        s.carrier_name,
        s.airport,
        s.airport_name,
        s.arr_flights,
        s.arr_del15,
        s.arr_cancelled,
        s.arr_diverted,
        s.carrier_ct,
        s.weather_ct,
        s.nas_ct,
        s.security_ct,
        s.late_aircraft_ct,
        s.arr_delay,
        s.carrier_delay,
        s.weather_delay,
        s.nas_delay,
        s.security_delay,
        s.late_aircraft_delay,
        s._ingested_at
    from staging as s
    inner join dim_carrier as dc on s.carrier = dc.carrier
    inner join dim_airport as da on s.airport = da.airport
    inner join dim_date as d on s.year = d.year and s.month = d.month
)

select *
from final
