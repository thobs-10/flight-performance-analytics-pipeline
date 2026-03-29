"""Gold asset: dim_carrier — Postgres marts → ClickHouse gold."""

import polars as pl
from dagster import AssetExecutionContext, asset

from flight_performance_analytics_pipeline.resources.clickhouse_io_manager import ClickhouseResource
from flight_performance_analytics_pipeline.resources.postgres_resource import PostgresResource

_MART_TABLE = "marts.dim_carrier"
_GOLD_TABLE = "gold.dim_carrier"


@asset(
    deps=["dim_carrier"],
    group_name="gold",
)
def gold_dim_carrier(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    clickhouse: ClickhouseResource,
) -> None:
    """Load dim_carrier from the Postgres marts layer into ClickHouse gold."""
    engine = postgres.get_engine()
    try:
        df = pl.read_database(
            query=f"SELECT carrier_key, carrier, carrier_name FROM {_MART_TABLE}",  # nosec B608
            connection=engine,
        )
    finally:
        engine.dispose()

    context.log.info(f"Read {len(df)} rows from {_MART_TABLE}.")
    clickhouse.insert_dataframe(_GOLD_TABLE, df.to_pandas(), truncate=True, chunk_size=10_000)
    context.log.info(f"Inserted {len(df)} rows into {_GOLD_TABLE}.")
