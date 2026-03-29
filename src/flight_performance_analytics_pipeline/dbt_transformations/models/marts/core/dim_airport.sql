/*
  dim_airport — Airport dimension for the gold layer.

  Derives one row per unique origin airport from the staging table.
  A stable MD5 surrogate key (airport_key) is generated from the IATA airport
  code so downstream joins are key-based rather than string-based.

  Source : staging.stg_airline_delay_data
  Target : marts.dim_airport  (Postgres)
           gold.dim_airport   (ClickHouse, via Dagster gold asset)
*/

with

source as (
    select distinct
        airport,
        airport_name
    from {{ ref('stg_airline_delay_data') }}
),

final as (
    select
        md5(airport) as airport_key,
        airport,
        airport_name
    from source
)

select *
from final
