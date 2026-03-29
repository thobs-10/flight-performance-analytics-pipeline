"""Dagster asset checks for the gold layer in ClickHouse.

Checks run after the gold Python assets load data into ClickHouse and validate
that dims and the fact table are populated and referentially consistent.
"""

from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from flight_performance_analytics_pipeline.resources.clickhouse_io_manager import ClickhouseResource

_FACT_ASSET = "gold_fact_flight_delays"
_DIM_CARRIER_ASSET = "gold_dim_carrier"
_DIM_AIRPORT_ASSET = "gold_dim_airport"
_DIM_DATE_ASSET = "gold_dim_date"


@asset_check(asset=_FACT_ASSET, name="fact_row_count")
def gold_fact_row_count(clickhouse: ClickhouseResource) -> AssetCheckResult:
    """Verify that fact_flight_delays contains at least one row in ClickHouse."""
    client = clickhouse.get_client()
    try:
        result = client.query("SELECT count() FROM gold.fact_flight_delays")
        row_count = result.first_row[0]
    finally:
        client.close()

    passed = row_count > 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        description=f"fact_flight_delays has {row_count} rows."
        if passed
        else "fact_flight_delays is empty.",
    )


@asset_check(asset=_DIM_CARRIER_ASSET, name="no_null_carrier_columns")
def gold_dim_carrier_no_nulls(clickhouse: ClickhouseResource) -> AssetCheckResult:
    """Verify no null carrier_key or carrier values in gold.dim_carrier."""
    client = clickhouse.get_client()
    try:
        result = client.query(
            "SELECT count() FROM gold.dim_carrier "
            "WHERE carrier_key = '' OR carrier = '' OR carrier_name = ''"
        )
        null_count = result.first_row[0]
    finally:
        client.close()

    passed = null_count == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        description="All carrier columns are populated."
        if passed
        else f"{null_count} rows have empty carrier_key, carrier, or carrier_name.",
    )


@asset_check(asset=_DIM_AIRPORT_ASSET, name="no_null_airport_columns")
def gold_dim_airport_no_nulls(clickhouse: ClickhouseResource) -> AssetCheckResult:
    """Verify no null airport_key or airport values in gold.dim_airport."""
    client = clickhouse.get_client()
    try:
        result = client.query(
            "SELECT count() FROM gold.dim_airport "
            "WHERE airport_key = '' OR airport = '' OR airport_name = ''"
        )
        null_count = result.first_row[0]
    finally:
        client.close()

    passed = null_count == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        description="All airport columns are populated."
        if passed
        else f"{null_count} rows have empty airport_key, airport, or airport_name.",
    )


@asset_check(asset=_FACT_ASSET, name="no_orphaned_carrier_keys")
def gold_fact_no_orphaned_carrier_keys(clickhouse: ClickhouseResource) -> AssetCheckResult:
    """Verify all carrier_key values in the fact table exist in dim_carrier."""
    client = clickhouse.get_client()
    try:
        result = client.query(
            "SELECT count() "
            "FROM gold.fact_flight_delays f "
            "LEFT JOIN gold.dim_carrier d ON d.carrier_key = f.carrier_key "
            "WHERE d.carrier_key IS NULL"
        )
        orphan_count = result.first_row[0]
    finally:
        client.close()

    passed = orphan_count == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        description="All carrier_key values resolve to dim_carrier."
        if passed
        else f"{orphan_count} fact rows have orphaned carrier_key values.",
    )


@asset_check(asset=_FACT_ASSET, name="no_orphaned_airport_keys")
def gold_fact_no_orphaned_airport_keys(clickhouse: ClickhouseResource) -> AssetCheckResult:
    """Verify all airport_key values in the fact table exist in dim_airport."""
    client = clickhouse.get_client()
    try:
        result = client.query(
            "SELECT count() "
            "FROM gold.fact_flight_delays f "
            "LEFT JOIN gold.dim_airport d ON d.airport_key = f.airport_key "
            "WHERE d.airport_key IS NULL"
        )
        orphan_count = result.first_row[0]
    finally:
        client.close()

    passed = orphan_count == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        description="All airport_key values resolve to dim_airport."
        if passed
        else f"{orphan_count} fact rows have orphaned airport_key values.",
    )


@asset_check(asset=_DIM_DATE_ASSET, name="no_orphaned_date_keys")
def gold_fact_no_orphaned_date_keys(clickhouse: ClickhouseResource) -> AssetCheckResult:
    """Verify all date_key values in the fact table exist in dim_date."""
    client = clickhouse.get_client()
    try:
        result = client.query(
            "SELECT count() "
            "FROM gold.fact_flight_delays f "
            "LEFT JOIN gold.dim_date d ON d.date_key = f.date_key "
            "WHERE d.date_key IS NULL"
        )
        orphan_count = result.first_row[0]
    finally:
        client.close()

    passed = orphan_count == 0
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        description="All date_key values resolve to dim_date."
        if passed
        else f"{orphan_count} fact rows have orphaned date_key values.",
    )
