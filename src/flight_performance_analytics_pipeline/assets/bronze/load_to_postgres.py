import polars as pl
from dagster import AssetExecutionContext, Config, asset

from flight_performance_analytics_pipeline.resources.postgres_resource import PostgresResource
from flight_performance_analytics_pipeline.utils.load_postgres_utils import (
    add_ingested_column,
    get_csv_file_path,
    load_query,
    write_to_database,
)


class BronzeIngestionConfig(Config):
    """Runtime config for the bronze ingestion pipeline."""

    csv_path: str = "data/ot_delaycause1_DL/Airline_Delay_Cause.csv"


@asset
def read_raw_airline_delay_csv(
    context: AssetExecutionContext,
    config: BronzeIngestionConfig,
) -> pl.DataFrame:
    """Read raw airline delay data from the source CSV file."""
    csv_path = get_csv_file_path(config.csv_path)
    df = pl.read_csv(csv_path)
    context.log.info(f"Read {len(df)} rows from {csv_path}.")
    return df


@asset
def add_metadata_columns_to_airline_delay_data(
    context: AssetExecutionContext,
    read_raw_airline_delay_csv: pl.DataFrame,
) -> pl.DataFrame:
    """Add an ingestion timestamp column to the raw data."""
    df = add_ingested_column(read_raw_airline_delay_csv)
    context.log.info(f"Added _ingested_at column to {len(df)} rows.")
    return df


@asset
def write_to_bronze_airline_delay_data(
    context: AssetExecutionContext,
    add_metadata_columns_to_airline_delay_data: pl.DataFrame,
    postgres: PostgresResource,
) -> None:
    """Ensure the bronze schema and table exist, then write enriched data to PostgreSQL."""
    engine = postgres.get_engine()
    context.log.info("Engine created. Starting write to bronze table.")
    try:
        load_query(engine, context)
        write_to_database(
            add_metadata_columns_to_airline_delay_data, engine, "bronze.airline_delay_data", context
        )
    finally:
        engine.dispose()
