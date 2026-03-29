/*
  dim_carrier — Carrier dimension for the gold layer.

  Derives one row per unique airline carrier from the staging table.
  A stable MD5 surrogate key (carrier_key) is generated from the IATA carrier
  code so downstream joins are key-based rather than string-based.

  Source : staging.stg_airline_delay_data
  Target : marts.dim_carrier  (Postgres)
           gold.dim_carrier   (ClickHouse, via Dagster gold asset)
*/

with

source as (
    select distinct
        carrier,
        carrier_name
    from {{ ref('stg_airline_delay_data') }}
),

final as (
    select
        md5(carrier) as carrier_key,
        carrier,
        carrier_name
    from source
)

select *
from final
