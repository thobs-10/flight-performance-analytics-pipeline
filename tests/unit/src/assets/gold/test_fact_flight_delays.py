"""Unit tests for the gold fact_flight_delays asset."""

from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from flight_performance_analytics_pipeline.assets.gold.fact_flight_delays import (
    gold_fact_flight_delays,
)

_GOLD_FACT_COMPUTE_FN = gold_fact_flight_delays.node_def.compute_fn.decorated_fn


@pytest.fixture
def yearly_fact_df() -> pl.DataFrame:
    """Minimal yearly fact dataset used to validate the gold asset."""
    return pl.DataFrame(
        {
            "airline_delay_id": ["id-1", "id-2"],
            "date_key": [202501, 202511],
            "carrier_key": ["carrier-1", "carrier-2"],
            "airport_key": ["airport-1", "airport-2"],
            "year": [2025, 2025],
            "month": [1, 11],
            "carrier": ["AA", "DL"],
            "carrier_name": ["American Airlines", "Delta Air Lines"],
            "airport": ["JFK", "ATL"],
            "airport_name": ["John F. Kennedy International", "Hartsfield-Jackson Atlanta"],
            "arr_flights": [100, 120],
            "arr_del15": [12, 20],
            "arr_cancelled": [1, 2],
            "arr_diverted": [0, 1],
            "carrier_ct": [1.0, 2.0],
            "weather_ct": [0.5, 0.0],
            "nas_ct": [1.5, 2.5],
            "security_ct": [0.0, 0.0],
            "late_aircraft_ct": [2.5, 3.5],
            "arr_delay": [150.0, 240.0],
            "carrier_delay": [50.0, 90.0],
            "weather_delay": [20.0, 0.0],
            "nas_delay": [30.0, 70.0],
            "security_delay": [0.0, 0.0],
            "late_aircraft_delay": [50.0, 80.0],
            "_ingested_at": ["2025-01-01 00:00:00", "2025-11-01 00:00:00"],
        }
    )


@pytest.mark.unit
def test_gold_fact_refreshes_entire_year_partition(yearly_fact_df: pl.DataFrame) -> None:
    """The asset should reload the full year and drop the matching ClickHouse partition."""
    context = MagicMock()
    context.partition_key = "2025-11-01"
    postgres = MagicMock()
    clickhouse = MagicMock()
    engine = MagicMock()
    postgres.get_engine.return_value = engine

    with patch(
        "flight_performance_analytics_pipeline.assets.gold.fact_flight_delays.pl.read_database",
        return_value=yearly_fact_df,
    ) as mock_read_database:
        _GOLD_FACT_COMPUTE_FN(context=context, postgres=postgres, clickhouse=clickhouse)

    query = " ".join(mock_read_database.call_args.kwargs["query"].split())
    assert "WHERE year = 2025" in query
    assert "month =" not in query
    engine.dispose.assert_called_once()
    clickhouse.execute.assert_called_once_with(
        "ALTER TABLE gold.fact_flight_delays DROP PARTITION 2025"
    )


@pytest.mark.unit
def test_gold_fact_inserts_yearly_dataframe_in_chunks(yearly_fact_df: pl.DataFrame) -> None:
    """The asset should insert the refreshed yearly DataFrame with the configured chunk size."""
    context = MagicMock()
    context.partition_key = "2025-11-01"
    postgres = MagicMock()
    clickhouse = MagicMock()
    postgres.get_engine.return_value = MagicMock()

    with patch(
        "flight_performance_analytics_pipeline.assets.gold.fact_flight_delays.pl.read_database",
        return_value=yearly_fact_df,
    ):
        _GOLD_FACT_COMPUTE_FN(context=context, postgres=postgres, clickhouse=clickhouse)

    args, kwargs = clickhouse.insert_dataframe.call_args
    assert args[0] == "gold.fact_flight_delays"
    assert len(args[1]) == len(yearly_fact_df)
    assert kwargs == {"truncate": False, "chunk_size": 10_000}


@pytest.mark.unit
def test_gold_fact_logs_year_based_refresh_messages(yearly_fact_df: pl.DataFrame) -> None:
    """The asset logs should reflect that the year, not the individual month, is refreshed."""
    context = MagicMock()
    context.partition_key = "2025-11-01"
    postgres = MagicMock()
    clickhouse = MagicMock()
    postgres.get_engine.return_value = MagicMock()

    with patch(
        "flight_performance_analytics_pipeline.assets.gold.fact_flight_delays.pl.read_database",
        return_value=yearly_fact_df,
    ):
        _GOLD_FACT_COMPUTE_FN(context=context, postgres=postgres, clickhouse=clickhouse)

    logged_messages = [call.args[0] for call in context.log.info.call_args_list]
    assert any("Read 2 rows for year 2025" in message for message in logged_messages)
    assert any("Inserted 2 rows for year 2025" in message for message in logged_messages)
