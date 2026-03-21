with

source as (
    select * from {{ source('bronze', 'airline_delay_data') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by year, month, carrier, airport
            order by _ingested_at desc
        ) as _row_num
    from source
),

cleaned as (
    select
        -- Surrogate key
        md5(
            cast(year as text)
            || '-' || cast(month as text)
            || '-' || upper(trim(carrier))
            || '-' || upper(trim(airport))
        )                                       as airline_delay_id,

        -- Identifiers
        cast(year as integer)                   as year,
        cast(month as integer)                  as month,
        upper(trim(carrier))                    as carrier,
        trim(carrier_name)                      as carrier_name,
        upper(trim(airport))                    as airport,
        trim(airport_name)                      as airport_name,

        -- Flight counts (whole numbers in source, stored as integer)
        round(cast(arr_flights as numeric))::integer    as arr_flights,
        round(cast(arr_del15 as numeric))::integer      as arr_del15,
        round(cast(arr_cancelled as numeric))::integer  as arr_cancelled,
        round(cast(arr_diverted as numeric))::integer   as arr_diverted,

        -- Weighted delay cause counts (fractional — keep as numeric)
        cast(carrier_ct as numeric(10, 2))      as carrier_ct,
        cast(weather_ct as numeric(10, 2))      as weather_ct,
        cast(nas_ct as numeric(10, 2))          as nas_ct,
        cast(security_ct as numeric(10, 2))     as security_ct,
        cast(late_aircraft_ct as numeric(10, 2)) as late_aircraft_ct,

        -- Delay minutes
        cast(arr_delay as numeric(10, 2))           as arr_delay,
        cast(carrier_delay as numeric(10, 2))       as carrier_delay,
        cast(weather_delay as numeric(10, 2))       as weather_delay,
        cast(nas_delay as numeric(10, 2))           as nas_delay,
        cast(security_delay as numeric(10, 2))      as security_delay,
        cast(late_aircraft_delay as numeric(10, 2)) as late_aircraft_delay,

        -- Audit
        _ingested_at

    from deduped
    where _row_num = 1
)

select * from cleaned
