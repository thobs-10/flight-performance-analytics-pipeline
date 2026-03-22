"""Unit tests for bronze airline delay Dagster asset checks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from dagster import AssetCheckSeverity
from flight_performance_analytics_pipeline.assets.bronze.airline_delay_checks import (
    bronze_data_freshness,
    bronze_minimum_row_count,
    bronze_no_null_key_columns,
)


def _mock_postgres_resource() -> tuple[MagicMock, MagicMock]:
    """Create a mocked postgres resource with a mocked disposable engine."""
    engine = MagicMock()
    postgres = MagicMock()
    postgres.get_engine.return_value = engine
    return postgres, engine


@pytest.mark.unit
def test_bronze_no_null_key_columns_passes_with_clean_rows() -> None:
    """No-null key check should pass when no key columns contain nulls."""
    postgres, engine = _mock_postgres_resource()
    df = pl.DataFrame(
        {
            "year": [2025],
            "month": [1],
            "carrier": ["AA"],
            "airport": ["JFK"],
        }
    )

    with patch(
        "flight_performance_analytics_pipeline.assets.bronze.airline_delay_checks.pl.read_database",
        return_value=df,
    ) as mock_read:
        result = bronze_no_null_key_columns(postgres=postgres)

    assert result.passed is True
    assert result.severity == AssetCheckSeverity.ERROR
    assert result.description == "All key columns are non-null."
    assert (
        "year IS NULL OR month IS NULL OR carrier IS NULL OR airport IS NULL"
        in mock_read.call_args.kwargs["query"]
    )
    engine.dispose.assert_called_once()


@pytest.mark.unit
def test_bronze_no_null_key_columns_fails_with_null_values() -> None:
    """No-null key check should fail when key columns include null values."""
    postgres, engine = _mock_postgres_resource()
    df = pl.DataFrame(
        {
            "year": [2025, None],
            "month": [1, 1],
            "carrier": ["AA", "AA"],
            "airport": ["JFK", "JFK"],
        }
    )

    with patch(
        "flight_performance_analytics_pipeline.assets.bronze.airline_delay_checks.pl.read_database",
        return_value=df,
    ):
        result = bronze_no_null_key_columns(postgres=postgres)

    assert result.passed is False
    assert result.severity == AssetCheckSeverity.ERROR
    assert "null" in (result.description or "").lower()
    engine.dispose.assert_called_once()


@pytest.mark.unit
def test_bronze_minimum_row_count_passes_when_rows_exist() -> None:
    """Minimum row-count check should pass when the table has at least one row."""
    postgres, engine = _mock_postgres_resource()
    df = pl.DataFrame({"row_count": [10]})

    with patch(
        "flight_performance_analytics_pipeline.assets.bronze.airline_delay_checks.pl.read_database",
        return_value=df,
    ):
        result = bronze_minimum_row_count(postgres=postgres)

    assert result.passed is True
    assert result.severity == AssetCheckSeverity.ERROR
    assert result.description == "Row count is 10."
    engine.dispose.assert_called_once()


@pytest.mark.unit
def test_bronze_minimum_row_count_fails_when_table_is_empty() -> None:
    """Minimum row-count check should fail when the table is empty."""
    postgres, engine = _mock_postgres_resource()
    df = pl.DataFrame({"row_count": [0]})

    with patch(
        "flight_performance_analytics_pipeline.assets.bronze.airline_delay_checks.pl.read_database",
        return_value=df,
    ):
        result = bronze_minimum_row_count(postgres=postgres)

    assert result.passed is False
    assert result.severity == AssetCheckSeverity.ERROR
    assert "at least 1 row" in (result.description or "").lower()
    engine.dispose.assert_called_once()


@pytest.mark.unit
def test_bronze_data_freshness_passes_with_recent_timezone_aware_timestamp() -> None:
    """Freshness check should pass when latest timestamp is recent and timezone-aware."""
    postgres, engine = _mock_postgres_resource()
    latest = datetime.now(timezone.utc) - timedelta(hours=1)
    df = pl.DataFrame({"latest": [latest]})

    with patch(
        "flight_performance_analytics_pipeline.assets.bronze.airline_delay_checks.pl.read_database",
        return_value=df,
    ):
        result = bronze_data_freshness(postgres=postgres)

    assert result.passed is True
    assert result.severity == AssetCheckSeverity.WARN
    assert result.description == "Bronze data is fresh."
    engine.dispose.assert_called_once()


@pytest.mark.unit
def test_bronze_data_freshness_passes_with_recent_naive_timestamp() -> None:
    """Freshness check should coerce naive timestamps to UTC and pass when recent."""
    postgres, engine = _mock_postgres_resource()
    latest = datetime.now() - timedelta(hours=1)
    df = pl.DataFrame({"latest": [latest]})

    with patch(
        "flight_performance_analytics_pipeline.assets.bronze.airline_delay_checks.pl.read_database",
        return_value=df,
    ):
        result = bronze_data_freshness(postgres=postgres)

    assert result.passed is True
    assert result.severity == AssetCheckSeverity.WARN
    assert result.description == "Bronze data is fresh."
    engine.dispose.assert_called_once()


@pytest.mark.unit
def test_bronze_data_freshness_fails_with_stale_data() -> None:
    """Freshness check should fail when latest timestamp is older than threshold."""
    postgres, engine = _mock_postgres_resource()
    latest = datetime.now(timezone.utc) - timedelta(hours=30)
    df = pl.DataFrame({"latest": [latest]})

    with patch(
        "flight_performance_analytics_pipeline.assets.bronze.airline_delay_checks.pl.read_database",
        return_value=df,
    ):
        result = bronze_data_freshness(postgres=postgres)

    assert result.passed is False
    assert result.severity == AssetCheckSeverity.WARN
    assert "exceeding the 25-hour freshness threshold" in (result.description or "")
    engine.dispose.assert_called_once()


@pytest.mark.unit
def test_bronze_data_freshness_fails_when_table_has_no_timestamps() -> None:
    """Freshness check should fail with a helpful message when no timestamps exist."""
    postgres, engine = _mock_postgres_resource()
    df = pl.DataFrame({"latest": [None]})

    with patch(
        "flight_performance_analytics_pipeline.assets.bronze.airline_delay_checks.pl.read_database",
        return_value=df,
    ):
        result = bronze_data_freshness(postgres=postgres)

    assert result.passed is False
    assert result.severity == AssetCheckSeverity.WARN
    assert "no ingestion timestamps found" in (result.description or "").lower()
    engine.dispose.assert_called_once()
