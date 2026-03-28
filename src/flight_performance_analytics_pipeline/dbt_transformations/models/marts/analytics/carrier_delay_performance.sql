/*
  carrier_delay_performance — Carrier-level delay performance analytics.

  Aggregates all delay metrics across all years and airports to produce a
  single summary row per carrier.  Suitable for ranked carrier comparisons,
  delay cause attribution, and executive-level dashboards.

  Metrics:
    - total_flights / total_delayed / total_cancelled
    - delay_rate_pct              : % of flights delayed ≥ 15 min
    - total_delay_minutes         : cumulative arrival delay (all causes)
    - total_*_delay_minutes       : per-cause breakdown
    - avg_delay_minutes_per_delayed_flight : severity of delays when they occur

  Source : marts.fact_flight_delays
  Target : marts.analytics.carrier_delay_performance  (Postgres)
           Upstream of ClickHouse mv_carrier_delay_performance materialized view
*/

with

fact as (
    select * from {{ ref('fact_flight_delays') }}
),

final as (
    select
        carrier_key,
        carrier,
        carrier_name,
        sum(arr_flights) as total_flights,
        sum(arr_del15) as total_delayed,
        sum(arr_cancelled) as total_cancelled,
        round(
            sum(arr_del15)::numeric / nullif(sum(arr_flights), 0) * 100, 2
        ) as delay_rate_pct,
        round(sum(arr_delay), 2) as total_delay_minutes,
        round(sum(carrier_delay), 2) as total_carrier_delay_minutes,
        round(sum(weather_delay), 2) as total_weather_delay_minutes,
        round(sum(nas_delay), 2) as total_nas_delay_minutes,
        round(sum(security_delay), 2) as total_security_delay_minutes,
        round(sum(late_aircraft_delay), 2) as total_late_aircraft_delay_minutes,
        round(
            sum(arr_delay)::numeric / nullif(sum(arr_del15), 0), 2
        ) as avg_delay_minutes_per_delayed_flight
    from fact
    group by carrier_key, carrier, carrier_name
)

select * from final
