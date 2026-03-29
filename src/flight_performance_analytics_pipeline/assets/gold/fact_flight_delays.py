"""Gold asset: fact_flight_delays — Postgres marts → ClickHouse gold.

The Dagster asset is partitioned by month (YYYY-MM-01 keys), but ClickHouse is
physically partitioned by year. To avoid expensive month-level DELETE mutations,
each run refreshes the entire target year partition in ClickHouse.
"""

import polars as pl
from dagster import AssetExecutionContext, MonthlyPartitionsDefinition, asset

from flight_performance_analytics_pipeline.resources.clickhouse_io_manager import ClickhouseResource
from flight_performance_analytics_pipeline.resources.postgres_resource import PostgresResource

_MART_TABLE = "marts.fact_flight_delays"
_GOLD_TABLE = "gold.fact_flight_delays"
_COLUMNS = (
    "airline_delay_id, date_key, carrier_key, airport_key, "
    "year, month, carrier, carrier_name, airport, airport_name, "
    "arr_flights, arr_del15, arr_cancelled, arr_diverted, "
    "carrier_ct, weather_ct, nas_ct, security_ct, late_aircraft_ct, "
    "arr_delay, carrier_delay, weather_delay, nas_delay, security_delay, "
    "late_aircraft_delay, _ingested_at"
)

# Covers all months present in the source data.
# Extend end_date as new months are ingested.
monthly_partition = MonthlyPartitionsDefinition(start_date="2024-01-01", end_date="2025-12-01")


@asset(
    deps=["fact_flight_delays", "gold_dim_carrier", "gold_dim_airport", "gold_dim_date"],
    group_name="gold",
    partitions_def=monthly_partition,
)
def gold_fact_flight_delays(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    clickhouse: ClickhouseResource,
) -> None:
    """Refresh one year of fact_flight_delays from Postgres marts into ClickHouse gold.

    Partitioned by month (partition key format: ``YYYY-MM-DD``). Each execution:

    1. Derives the target year from the active partition key.
    2. Reads all rows for that year from Postgres.
    3. Drops the ClickHouse year partition (cheap metadata operation).
    4. Inserts fresh rows in chunks to stay within ClickHouse memory limits.

    This keeps reruns idempotent while avoiding memory-heavy DELETE mutations.
    """
    partition_date = context.partition_key  # e.g. "2024-01-01"
    year = int(partition_date[:4])

    engine = postgres.get_engine()
    try:
        df = pl.read_database(
            query=(
                f"SELECT {_COLUMNS} FROM {_MART_TABLE} "  # nosec B608
                f"WHERE year = {year}"
            ),
            connection=engine,
        )
    finally:
        engine.dispose()

    context.log.info(f"Read {len(df)} rows for year {year} from {_MART_TABLE}.")

    # Idempotent and memory-safe for year-partitioned MergeTree tables.
    clickhouse.execute(f"ALTER TABLE {_GOLD_TABLE} DROP PARTITION {year}")  # nosec B608
    clickhouse.insert_dataframe(_GOLD_TABLE, df.to_pandas(), truncate=False, chunk_size=10_000)
    context.log.info(f"Inserted {len(df)} rows for year {year} into {_GOLD_TABLE}.")
