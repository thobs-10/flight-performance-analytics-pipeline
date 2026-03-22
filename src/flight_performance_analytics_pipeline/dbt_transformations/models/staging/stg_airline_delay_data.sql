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
        year::integer as year,  -- noqa: RF04

        -- Identifiers
        month::integer as month,  -- noqa: RF04
        carrier_ct::numeric(10, 2) as carrier_ct,  -- noqa: RF04
        weather_ct::numeric(10, 2) as weather_ct,
        nas_ct::numeric(10, 2) as nas_ct,
        security_ct::numeric(10, 2) as security_ct,
        late_aircraft_ct::numeric(10, 2) as late_aircraft_ct,

        -- Flight counts (whole numbers in source, stored as integer)
        arr_delay::numeric(10, 2) as arr_delay,
        carrier_delay::numeric(10, 2) as carrier_delay,
        weather_delay::numeric(10, 2) as weather_delay,
        nas_delay::numeric(10, 2) as nas_delay,

        -- Weighted delay cause counts (fractional — keep as numeric)
        security_delay::numeric(10, 2) as security_delay,
        late_aircraft_delay::numeric(10, 2) as late_aircraft_delay,
        _ingested_at,
        md5(
            year::text
            || '-' || month::text
            || '-' || upper(trim(carrier))
            || '-' || upper(trim(airport))
        ) as airline_delay_id,
        upper(trim(carrier)) as carrier,

        -- Delay minutes
        trim(carrier_name) as carrier_name,
        upper(trim(airport)) as airport,
        trim(airport_name) as airport_name,
        round(arr_flights::numeric)::integer as arr_flights,
        round(arr_del15::numeric)::integer as arr_del15,
        round(arr_cancelled::numeric)::integer as arr_cancelled,

        -- Audit
        round(arr_diverted::numeric)::integer as arr_diverted

    from deduped
    where _row_num = 1
)

select * from cleaned
