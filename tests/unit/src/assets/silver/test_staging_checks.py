"""Unit tests for silver staging asset checks."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from dagster import AssetCheckSeverity
from flight_performance_analytics_pipeline.assets.silver.staging_checks import (
    staging_month_range,
    staging_no_null_key_columns,
    staging_non_negative_delays,
    staging_unique_surrogate_key,
)


def _mock_postgres_resource() -> tuple[MagicMock, MagicMock]:
    """Return a mocked postgres resource with a disposable engine."""
    engine = MagicMock()
    postgres = MagicMock()
    postgres.get_engine.return_value = engine
    return postgres, engine


@pytest.mark.unit
def test_staging_unique_surrogate_key_passes() -> None:
    """Unique key check should pass when airline_delay_id values are unique."""
    postgres, engine = _mock_postgres_resource()
    df = pl.DataFrame({"airline_delay_id": ["a", "b", "c"]})

    with patch(
        "flight_performance_analytics_pipeline.assets.silver.staging_checks.pl.read_database",
        return_value=df,
    ):
        result = staging_unique_surrogate_key(postgres=postgres)

    assert result.passed is True
    assert result.severity == AssetCheckSeverity.ERROR
    assert "unique" in (result.description or "").lower()
    engine.dispose.assert_called_once()


@pytest.mark.unit
def test_staging_unique_surrogate_key_fails_on_duplicates() -> None:
    """Unique key check should fail when airline_delay_id values are duplicated."""
    postgres, engine = _mock_postgres_resource()
    df = pl.DataFrame({"airline_delay_id": ["dup", "dup"]})

    with patch(
        "flight_performance_analytics_pipeline.assets.silver.staging_checks.pl.read_database",
        return_value=df,
    ):
        result = staging_unique_surrogate_key(postgres=postgres)

    assert result.passed is False
    assert "duplicate" in (result.description or "").lower()
    engine.dispose.assert_called_once()


@pytest.mark.unit
def test_staging_no_null_key_columns_fails_on_nulls() -> None:
    """Null key-column check should fail when any key column contains null values."""
    postgres, engine = _mock_postgres_resource()
    df = pl.DataFrame(
        {
            "year": [2025, None],
            "month": [1, 1],
            "carrier": ["AA", "AA"],
            "airport": ["JFK", "JFK"],
            "airline_delay_id": ["k1", "k2"],
        }
    )

    with patch(
        "flight_performance_analytics_pipeline.assets.silver.staging_checks.pl.read_database",
        return_value=df,
    ):
        result = staging_no_null_key_columns(postgres=postgres)

    assert result.passed is False
    assert result.severity == AssetCheckSeverity.ERROR
    assert "null" in (result.description or "").lower()
    engine.dispose.assert_called_once()


@pytest.mark.unit
def test_staging_month_range_fails_for_invalid_month() -> None:
    """Month range check should fail when values are outside [1, 12]."""
    postgres, engine = _mock_postgres_resource()
    df = pl.DataFrame({"month": [1, 12, 13]})

    with patch(
        "flight_performance_analytics_pipeline.assets.silver.staging_checks.pl.read_database",
        return_value=df,
    ):
        result = staging_month_range(postgres=postgres)

    assert result.passed is False
    assert result.severity == AssetCheckSeverity.ERROR
    assert (
        "range" in (result.description or "").lower()
        or "above" in (result.description or "").lower()
    )
    engine.dispose.assert_called_once()


@pytest.mark.unit
def test_staging_non_negative_delays_fails_for_negative_values() -> None:
    """Non-negative delay check should fail when any delay column is below zero."""
    postgres, engine = _mock_postgres_resource()
    df = pl.DataFrame(
        {
            "arr_delay": [0.0, -1.0],
            "carrier_delay": [0.0, 0.0],
            "weather_delay": [0.0, 0.0],
            "nas_delay": [0.0, 0.0],
            "security_delay": [0.0, 0.0],
            "late_aircraft_delay": [0.0, 0.0],
        }
    )

    with patch(
        "flight_performance_analytics_pipeline.assets.silver.staging_checks.pl.read_database",
        return_value=df,
    ):
        result = staging_non_negative_delays(postgres=postgres)

    assert result.passed is False
    assert result.severity == AssetCheckSeverity.WARN
    assert "below minimum" in (result.description or "").lower()
    engine.dispose.assert_called_once()
