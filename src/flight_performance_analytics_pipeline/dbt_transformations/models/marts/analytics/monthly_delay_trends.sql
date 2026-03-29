/*
  monthly_delay_trends — Monthly rollup of flight delay metrics.

  Aggregates all carriers and airports into a single row per calendar month,
  enabling time-series trend analysis across the full dataset.

  Metrics:
    - total_flights / total_delayed / total_cancelled / total_diverted
    - delay_rate_pct             : % of flights delayed ≥ 15 min
    - total_delay_minutes        : cumulative arrival delay (all causes)
    - avg_delay_minutes_per_flight : average delay burden per arriving flight
    - total_*_delay_minutes      : per-cause breakdown (carrier, weather, NAS,
                                   late_aircraft)

  Source : marts.fact_flight_delays
  Target : marts.analytics.monthly_delay_trends  (Postgres)
           Upstream of ClickHouse mv_monthly_delay_trends materialized view
*/

with

fact as (
    select * from {{ ref('fact_flight_delays') }}
),

final as (
    select
        date_key,
        year,
        month,
        sum(arr_flights) as total_flights,
        sum(arr_del15) as total_delayed,
        sum(arr_cancelled) as total_cancelled,
        sum(arr_diverted) as total_diverted,
        round(
            sum(arr_del15)::numeric / nullif(sum(arr_flights), 0) * 100, 2
        ) as delay_rate_pct,
        round(sum(arr_delay), 2) as total_delay_minutes,
        round(
            sum(arr_delay)::numeric / nullif(sum(arr_flights), 0), 2
        ) as avg_delay_minutes_per_flight,
        round(sum(carrier_delay), 2) as total_carrier_delay_minutes,
        round(sum(weather_delay), 2) as total_weather_delay_minutes,
        round(sum(nas_delay), 2) as total_nas_delay_minutes,
        round(sum(late_aircraft_delay), 2) as total_late_aircraft_delay_minutes
    from fact
    group by date_key, year, month
    order by year, month
)

select * from final
