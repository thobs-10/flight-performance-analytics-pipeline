"""Dagster asset checks for the staging.stg_airline_delay_data table.

Checks run after the dbt staging model materialises and query the database
directly. Validation logic is delegated to pure functions in data_validators.py.
"""

import polars as pl
from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from flight_performance_analytics_pipeline.assets.silver.dbt_staging_assets import (
    dbt_staging_airline_delay_assets,
)
from flight_performance_analytics_pipeline.resources.postgres_resource import PostgresResource
from flight_performance_analytics_pipeline.utils.data_validators import (
    validate_column_consistency,
    validate_column_range,
    validate_no_duplicates,
    validate_no_nulls,
)

_KEY_COLUMNS = ["year", "month", "carrier", "airport", "airline_delay_id"]
_STAGING_TABLE = "staging.stg_airline_delay_data"
_DELAY_CAUSE_COLS = ["carrier_ct", "weather_ct", "nas_ct", "security_ct", "late_aircraft_ct"]


@asset_check(asset=dbt_staging_airline_delay_assets, name="unique_surrogate_key")
def staging_unique_surrogate_key(postgres: PostgresResource) -> AssetCheckResult:
    """Verify that airline_delay_id is unique across all staging rows."""
    engine = postgres.get_engine()
    try:
        df = pl.read_database(
            query=f"SELECT airline_delay_id FROM {_STAGING_TABLE}",  # nosec B608
            connection=engine,
        )
        errors = validate_no_duplicates(df, key_columns=["airline_delay_id"])
    finally:
        engine.dispose()

    passed = len(errors) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        description="; ".join(errors) if errors else "Surrogate key is unique.",
    )


@asset_check(asset=dbt_staging_airline_delay_assets, name="no_null_key_columns")
def staging_no_null_key_columns(postgres: PostgresResource) -> AssetCheckResult:
    """Verify that none of the key columns contain nulls in the staging table."""
    engine = postgres.get_engine()
    try:
        null_conditions = " OR ".join(f"{col} IS NULL" for col in _KEY_COLUMNS)
        query = f"SELECT {', '.join(_KEY_COLUMNS)} FROM {_STAGING_TABLE} WHERE {null_conditions}"  # nosec B608
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


@asset_check(asset=dbt_staging_airline_delay_assets, name="month_range")
def staging_month_range(postgres: PostgresResource) -> AssetCheckResult:
    """Verify that all month values are between 1 and 12."""
    engine = postgres.get_engine()
    try:
        df = pl.read_database(query=f"SELECT month FROM {_STAGING_TABLE}", connection=engine)  # nosec B608
        errors = validate_column_range(df, column="month", min_val=1, max_val=12)
    finally:
        engine.dispose()

    passed = len(errors) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        description="; ".join(errors) if errors else "All month values are in range [1, 12].",
    )


@asset_check(asset=dbt_staging_airline_delay_assets, name="non_negative_delays")
def staging_non_negative_delays(postgres: PostgresResource) -> AssetCheckResult:
    """Verify that all delay minute columns are non-negative."""
    delay_cols = [
        "arr_delay",
        "carrier_delay",
        "weather_delay",
        "nas_delay",
        "security_delay",
        "late_aircraft_delay",
    ]
    engine = postgres.get_engine()
    try:
        df = pl.read_database(
            query=f"SELECT {', '.join(delay_cols)} FROM {_STAGING_TABLE}",  # nosec B608
            connection=engine,
        )
        errors = [
            err for col in delay_cols for err in validate_column_range(df, column=col, min_val=0)
        ]
    finally:
        engine.dispose()

    passed = len(errors) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        description="; ".join(errors) if errors else "All delay columns are non-negative.",
    )


@asset_check(asset=dbt_staging_airline_delay_assets, name="delay_cause_consistency")
def staging_delay_cause_consistency(postgres: PostgresResource) -> AssetCheckResult:
    """Verify that the sum of delay cause counts approximately equals arr_del15."""
    engine = postgres.get_engine()
    try:
        cols = _DELAY_CAUSE_COLS + ["arr_del15"]
        df = pl.read_database(
            query=f"SELECT {', '.join(cols)} FROM {_STAGING_TABLE}",  # nosec B608
            connection=engine,
        )
        errors = validate_column_consistency(
            df,
            component_columns=_DELAY_CAUSE_COLS,
            total_column="arr_del15",
            tolerance=1.0,
        )
    finally:
        engine.dispose()

    passed = len(errors) == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        description="; ".join(errors)
        if errors
        else "Delay cause counts are consistent with arr_del15.",
    )
