/*
  dim_date — Date dimension for the gold layer.

  Derives one row per distinct (year, month) period present in the staging data
  and enriches it with a compact integer date_key (YYYYMM), quarter, full month
  name, and a zero-padded year_month string for display.

  Source : staging.stg_airline_delay_data
  Target : marts.dim_date  (Postgres)
           gold.dim_date   (ClickHouse, via Dagster gold asset)
*/

with

source as (
    select distinct
        year,
        month
    from {{ ref('stg_airline_delay_data') }}
),

final as (
    select
        (year * 100 + month)::integer as date_key,
        year,
        month,
        case
            when month between 1 and 3 then 1
            when month between 4 and 6 then 2
            when month between 7 and 9 then 3
            else 4
        end as quarter,
        to_char(to_date(month::text, 'MM'), 'Month') as month_name,
        year::text || '-' || lpad(month::text, 2, '0') as year_month
    from source
)

select * from final
