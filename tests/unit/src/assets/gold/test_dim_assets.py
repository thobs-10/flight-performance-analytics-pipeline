"""Unit tests for gold dimension assets loaded from Postgres into ClickHouse."""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from flight_performance_analytics_pipeline.assets.gold.dim_airport import gold_dim_airport
from flight_performance_analytics_pipeline.assets.gold.dim_carrier import gold_dim_carrier
from flight_performance_analytics_pipeline.assets.gold.dim_date import gold_dim_date

_AIRPORT_COMPUTE_FN = gold_dim_airport.node_def.compute_fn.decorated_fn
_CARRIER_COMPUTE_FN = gold_dim_carrier.node_def.compute_fn.decorated_fn
_DATE_COMPUTE_FN = gold_dim_date.node_def.compute_fn.decorated_fn


def _airport_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "airport_key": ["JFK"],
            "airport": ["JFK"],
            "airport_name": ["John F. Kennedy International"],
        }
    )


def _carrier_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "carrier_key": ["AA"],
            "carrier": ["AA"],
            "carrier_name": ["American Airlines"],
        }
    )


def _date_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "date_key": [202501],
            "year": [2025],
            "month": [1],
            "quarter": [1],
            "month_name": ["January"],
            "year_month": ["2025-01"],
        }
    )


@pytest.mark.parametrize(
    "compute_fn, patch_target, expected_query, expected_gold_table, df_factory",
    [
        (
            _AIRPORT_COMPUTE_FN,
            "flight_performance_analytics_pipeline.assets.gold.dim_airport.pl.read_database",
            "SELECT airport_key, airport, airport_name FROM marts.dim_airport",
            "gold.dim_airport",
            _airport_df,
        ),
        (
            _CARRIER_COMPUTE_FN,
            "flight_performance_analytics_pipeline.assets.gold.dim_carrier.pl.read_database",
            "SELECT carrier_key, carrier, carrier_name FROM marts.dim_carrier",
            "gold.dim_carrier",
            _carrier_df,
        ),
        (
            _DATE_COMPUTE_FN,
            "flight_performance_analytics_pipeline.assets.gold.dim_date.pl.read_database",
            "SELECT date_key, year, month, quarter, month_name, year_month FROM marts.dim_date",
            "gold.dim_date",
            _date_df,
        ),
    ],
)
@pytest.mark.unit
def test_gold_dim_assets_query_and_insert(
    compute_fn: Callable[..., None],
    patch_target: str,
    expected_query: str,
    expected_gold_table: str,
    df_factory: Callable[[], pl.DataFrame],
) -> None:
    """Each gold dimension asset should read from marts and insert into ClickHouse."""
    context = MagicMock()
    postgres = MagicMock()
    clickhouse = MagicMock()
    engine = MagicMock()
    postgres.get_engine.return_value = engine
    source_df = df_factory()

    with patch(patch_target, return_value=source_df) as mock_read_database:
        compute_fn(context=context, postgres=postgres, clickhouse=clickhouse)

    assert mock_read_database.call_args.kwargs["query"] == expected_query
    assert mock_read_database.call_args.kwargs["connection"] is engine
    engine.dispose.assert_called_once()

    args, kwargs = clickhouse.insert_dataframe.call_args
    assert args[0] == expected_gold_table
    assert len(args[1]) == len(source_df)
    assert kwargs == {"truncate": True, "chunk_size": 10_000}


@pytest.mark.parametrize(
    "compute_fn, patch_target",
    [
        (
            _AIRPORT_COMPUTE_FN,
            "flight_performance_analytics_pipeline.assets.gold.dim_airport.pl.read_database",
        ),
        (
            _CARRIER_COMPUTE_FN,
            "flight_performance_analytics_pipeline.assets.gold.dim_carrier.pl.read_database",
        ),
        (
            _DATE_COMPUTE_FN,
            "flight_performance_analytics_pipeline.assets.gold.dim_date.pl.read_database",
        ),
    ],
)
@pytest.mark.unit
def test_gold_dim_assets_dispose_engine_on_read_failure(
    compute_fn: Callable[..., None],
    patch_target: str,
) -> None:
    """The SQLAlchemy engine must always be disposed, even when reads fail."""
    context = MagicMock()
    postgres = MagicMock()
    clickhouse = MagicMock()
    engine = MagicMock()
    postgres.get_engine.return_value = engine

    with patch(patch_target, side_effect=RuntimeError("read failed")):
        with pytest.raises(RuntimeError, match="read failed"):
            compute_fn(context=context, postgres=postgres, clickhouse=clickhouse)

    engine.dispose.assert_called_once()
    clickhouse.insert_dataframe.assert_not_called()
