"""Pure data validation functions for the flight performance analytics pipeline.

All functions accept a Polars DataFrame and return a list of error strings.
An empty list means the check passed. Functions never access the database —
they operate only on the data passed in.
"""

from datetime import datetime, timedelta, timezone

import polars as pl


def validate_no_nulls(df: pl.DataFrame, columns: list[str]) -> list[str]:
    """Check that none of the specified columns contain null values.

    Args:
        df: DataFrame to validate.
        columns: Column names to check for nulls.

    Returns:
        List of error strings, one per column that contains nulls. Empty if all pass.
    """
    errors: list[str] = []
    for col in columns:
        null_count = df[col].null_count()
        if null_count > 0:
            errors.append(f"Column '{col}' contains {null_count} null value(s).")
    return errors


def validate_min_row_count(df: pl.DataFrame, min_rows: int) -> list[str]:
    """Check that the DataFrame contains at least min_rows rows.

    Args:
        df: DataFrame to validate.
        min_rows: Minimum acceptable row count (inclusive).

    Returns:
        List with one error string if count is below threshold, else empty.
    """
    actual = len(df)
    if actual < min_rows:
        return [f"Expected at least {min_rows} row(s), but found {actual}."]
    return []


def validate_no_duplicates(df: pl.DataFrame, key_columns: list[str]) -> list[str]:
    """Check that the combination of key_columns is unique across all rows.

    Args:
        df: DataFrame to validate.
        key_columns: Columns that together form the uniqueness key.

    Returns:
        List with one error string if duplicates exist, else empty.
    """
    total = len(df)
    unique = df.select(key_columns).unique().height
    duplicates = total - unique
    if duplicates > 0:
        cols = ", ".join(key_columns)
        return [f"Found {duplicates} duplicate row(s) on key column(s): [{cols}]."]
    return []


def validate_column_range(
    df: pl.DataFrame,
    column: str,
    min_val: float | int | None = None,
    max_val: float | int | None = None,
) -> list[str]:
    """Check that all non-null values in a column fall within [min_val, max_val].

    Args:
        df: DataFrame to validate.
        column: Column name to check.
        min_val: Inclusive lower bound (None = no lower bound check).
        max_val: Inclusive upper bound (None = no upper bound check).

    Returns:
        List of error strings for each violated bound. Empty if all pass.
    """
    errors: list[str] = []
    series = df[column].drop_nulls()
    if min_val is not None:
        violations = (series < min_val).sum()
        if violations > 0:
            errors.append(f"Column '{column}' has {violations} value(s) below minimum {min_val}.")
    if max_val is not None:
        violations = (series > max_val).sum()
        if violations > 0:
            errors.append(f"Column '{column}' has {violations} value(s) above maximum {max_val}.")
    return errors


def validate_column_consistency(
    df: pl.DataFrame,
    component_columns: list[str],
    total_column: str,
    tolerance: float = 1.0,
) -> list[str]:
    """Check that the sum of component columns approximately equals a total column.

    Used to verify cross-column constraints, e.g. that the sum of individual delay
    cause counts matches the total delayed flights count.

    Args:
        df: DataFrame to validate.
        component_columns: Columns whose values should sum to the total.
        total_column: Column holding the expected total.
        tolerance: Allowed absolute difference between sum and total per row.

    Returns:
        List with one error string if any row violates the constraint, else empty.
    """
    component_sum = sum(df[col].fill_null(0) for col in component_columns)
    total = df[total_column].fill_null(0)
    violations = ((component_sum - total).abs() > tolerance).sum()
    if violations > 0:
        cols = " + ".join(component_columns)
        return [
            f"{violations} row(s) where [{cols}] differs from '{total_column}' "
            f"by more than {tolerance}."
        ]
    return []


def validate_data_freshness(
    ingested_at_values: list[datetime],
    max_age_hours: int = 25,
) -> list[str]:
    """Check that the most recent ingestion timestamp is within the allowed age window.

    Args:
        ingested_at_values: List of ingestion timestamps (timezone-aware).
        max_age_hours: Maximum acceptable age of the latest record in hours.

    Returns:
        List with one error string if data is stale, else empty.
    """
    if not ingested_at_values:
        return ["No ingestion timestamps found — table may be empty."]
    latest = max(ingested_at_values)
    age = datetime.now(timezone.utc) - latest
    if age > timedelta(hours=max_age_hours):
        hours_old = age.total_seconds() / 3600
        return [
            f"Latest ingestion is {hours_old:.1f} hour(s) old, "
            f"exceeding the {max_age_hours}-hour freshness threshold."
        ]
    return []
