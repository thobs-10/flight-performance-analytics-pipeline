"""Dagster asset checks for the bronze.airline_delay_data table.

Checks run after write_to_bronze_airline_delay_data materialises and query the
database directly so they reflect the actual persisted state. Validation logic
is delegated to pure functions in data_validators.py for testability.
"""

from datetime import datetime, timezone

import polars as pl
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from flight_performance_analytics_pipeline.assets.bronze.load_to_postgres import (
    write_to_bronze_airline_delay_data,
)
from flight_performance_analytics_pipeline.resources.postgres_resource import PostgresResource
from flight_performance_analytics_pipeline.utils.data_validators import (
    validate_data_freshness,
    validate_min_row_count,
    validate_no_nulls,
)

_KEY_COLUMNS = ["year", "month", "carrier", "airport"]
_BRONZE_TABLE = "bronze.airline_delay_data"


@asset_check(asset=write_to_bronze_airline_delay_data, name="no_null_key_columns")
def bronze_no_null_key_columns(postgres: PostgresResource) -> AssetCheckResult:
    """Verify that none of the key columns contain nulls in the bronze table."""
    engine = postgres.get_engine()
    try:
        null_conditions = " OR ".join(f"{col} IS NULL" for col in _KEY_COLUMNS)
        query = f"SELECT {', '.join(_KEY_COLUMNS)} FROM {_BRONZE_TABLE} WHERE {null_conditions}"  # nosec B608
        df = pl.read_database(query=query, connection=engine)
        errors = validate_no_nulls(df, _KEY_COLUMNS)
    finally:
        engine.dispose()

    passed = len(errors) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        description="; ".join(errors) if errors else "All key columns are non-null.",
    )


@asset_check(asset=write_to_bronze_airline_delay_data, name="minimum_row_count")
def bronze_minimum_row_count(postgres: PostgresResource) -> AssetCheckResult:
    """Verify that the bronze table contains at least one row."""
    engine = postgres.get_engine()
    try:
        df = pl.read_database(
            query=f"SELECT COUNT(*) AS row_count FROM {_BRONZE_TABLE}",  # nosec B608
            connection=engine,
        )
        count_df = pl.DataFrame({"_check": range(df["row_count"][0])})
        errors = validate_min_row_count(count_df, min_rows=1)
    finally:
        engine.dispose()

    passed = len(errors) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        description="; ".join(errors) if errors else f"Row count is {df['row_count'][0]}.",
    )


@asset_check(asset=write_to_bronze_airline_delay_data, name="data_freshness")
def bronze_data_freshness(postgres: PostgresResource) -> AssetCheckResult:
    """Verify that the latest ingestion timestamp is within the last 25 hours."""
    engine = postgres.get_engine()
    try:
        df = pl.read_database(
            query=f"SELECT MAX(_ingested_at) AS latest FROM {_BRONZE_TABLE}",  # nosec B608
            connection=engine,
        )
        latest = df["latest"][0]
        timestamps: list[datetime] = (
            [latest.replace(tzinfo=timezone.utc) if latest.tzinfo is None else latest]
            if latest is not None
            else []
        )
        errors = validate_data_freshness(timestamps, max_age_hours=25)
    finally:
        engine.dispose()

    passed = len(errors) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        description="; ".join(errors) if errors else "Bronze data is fresh.",
    )
