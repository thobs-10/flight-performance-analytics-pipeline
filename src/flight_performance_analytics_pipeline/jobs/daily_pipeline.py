"""Dagster asset jobs for daily automation and monthly fact backfills."""

from dagster import AssetSelection, define_asset_job

from flight_performance_analytics_pipeline.assets.gold.fact_flight_delays import monthly_partition

_CSV_PATH = "data/ot_delaycause1_DL/Airline_Delay_Cause_Full.csv"

# Reuse the same runtime config shape as run_config.yaml for bronze ingestion.
_DAILY_RUN_CONFIG = {
    "ops": {
        "read_raw_airline_delay_csv": {
            "config": {
                "csv_path": _CSV_PATH,
            }
        }
    }
}


daily_pipeline_job = define_asset_job(
    name="daily_pipeline",
    description=(
        "Run the automated daily pipeline: bronze ingestion, silver staging, "
        "gold dbt marts, ClickHouse dims, and the monthly-partitioned fact load."
    ),
    selection=AssetSelection.groups("bronze", "silver", "gold"),
    partitions_def=monthly_partition,
    config=_DAILY_RUN_CONFIG,
)


backfill_pipeline_job = define_asset_job(
    name="backfill_pipeline",
    description=(
        "Backfill monthly partitions for gold_fact_flight_delays. "
        "Use this job with explicit partition keys or a partition range."
    ),
    selection=AssetSelection.keys("gold_fact_flight_delays"),
    partitions_def=monthly_partition,
)
