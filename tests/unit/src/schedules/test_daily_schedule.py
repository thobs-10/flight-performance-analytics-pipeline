"""Unit tests for Dagster daily schedule configuration."""

from datetime import datetime, timezone

import pytest
from flight_performance_analytics_pipeline.schedules.daily_schedule import (
    _current_month_partition_key,
    daily_pipeline_schedule,
)


@pytest.mark.unit
def test_current_month_partition_key_normalizes_to_first_day_of_month() -> None:
    """The schedule helper should always emit the first day of the target month."""
    now_utc = datetime(2025, 11, 15, 3, 0, tzinfo=timezone.utc)

    assert _current_month_partition_key(now_utc) == "2025-11-01"


@pytest.mark.unit
def test_current_month_partition_key_preserves_year_boundary() -> None:
    """The schedule helper should handle January dates correctly."""
    now_utc = datetime(2026, 1, 2, 3, 0, tzinfo=timezone.utc)

    assert _current_month_partition_key(now_utc) == "2026-01-01"


@pytest.mark.unit
def test_daily_pipeline_schedule_metadata() -> None:
    """The schedule should target the daily pipeline job at the expected UTC cron."""
    assert daily_pipeline_schedule.name == "daily_pipeline_schedule"
    assert daily_pipeline_schedule.cron_schedule == "0 3 * * *"
    assert daily_pipeline_schedule.execution_timezone == "UTC"
    assert daily_pipeline_schedule.job_name == "daily_pipeline"
