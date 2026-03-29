"""Unit tests for Dagster automation job definitions."""

import pytest
from flight_performance_analytics_pipeline.assets.gold.fact_flight_delays import monthly_partition
from flight_performance_analytics_pipeline.jobs.daily_pipeline import (
    _CSV_PATH,
    _DAILY_RUN_CONFIG,
    backfill_pipeline_job,
    daily_pipeline_job,
)


@pytest.mark.unit
def test_daily_pipeline_job_metadata() -> None:
    """The daily job should expose the expected name, config, and partitioning."""
    assert daily_pipeline_job.name == "daily_pipeline"
    assert daily_pipeline_job.config == _DAILY_RUN_CONFIG
    assert daily_pipeline_job.partitions_def == monthly_partition


@pytest.mark.unit
def test_daily_pipeline_job_targets_bronze_silver_and_gold_groups() -> None:
    """The daily job must target the bronze, silver, and gold asset groups."""
    selection_repr = repr(daily_pipeline_job.selection)

    assert "GroupsAssetSelection" in selection_repr
    assert "bronze" in selection_repr
    assert "silver" in selection_repr
    assert "gold" in selection_repr


@pytest.mark.unit
def test_daily_pipeline_job_uses_expected_bronze_csv_path() -> None:
    """The daily job run config should point at the canonical bronze CSV path."""
    csv_path = _DAILY_RUN_CONFIG["ops"]["read_raw_airline_delay_csv"]["config"]["csv_path"]

    assert csv_path == _CSV_PATH
    assert csv_path.endswith("Airline_Delay_Cause_Full.csv")


@pytest.mark.unit
def test_backfill_pipeline_job_metadata() -> None:
    """The backfill job should be partitioned and named consistently."""
    assert backfill_pipeline_job.name == "backfill_pipeline"
    assert backfill_pipeline_job.partitions_def == monthly_partition


@pytest.mark.unit
def test_backfill_pipeline_job_targets_only_gold_fact_asset() -> None:
    """The backfill job should target only the gold fact asset."""
    selection_repr = repr(backfill_pipeline_job.selection)

    assert "KeysAssetSelection" in selection_repr
    assert "gold_fact_flight_delays" in selection_repr
