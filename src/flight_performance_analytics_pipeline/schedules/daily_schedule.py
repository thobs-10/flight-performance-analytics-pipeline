"""Dagster schedules for automated pipeline execution."""

from datetime import datetime, timezone

from dagster import RunRequest, ScheduleEvaluationContext, schedule

from flight_performance_analytics_pipeline.jobs.daily_pipeline import daily_pipeline_job


def _current_month_partition_key(now_utc: datetime) -> str:
    """Return the month partition key in YYYY-MM-DD format (first day of month)."""
    return now_utc.strftime("%Y-%m-01")


@schedule(
    job=daily_pipeline_job,
    # runs at 03:00 UTC daily, which is 05:00 in the source data's timezone South Africa.
    cron_schedule="0 3 * * *",
    execution_timezone="UTC",
)
def daily_pipeline_schedule(context: ScheduleEvaluationContext) -> RunRequest:
    """Schedule the daily pipeline at 03:00 UTC for the active month partition."""
    now_utc = context.scheduled_execution_time or datetime.now(timezone.utc)
    partition_key = _current_month_partition_key(now_utc)
    return RunRequest(
        run_key=f"daily-pipeline-{now_utc.strftime('%Y-%m-%d')}",
        partition_key=partition_key,
    )
