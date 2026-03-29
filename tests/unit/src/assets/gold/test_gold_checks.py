"""Unit tests for gold ClickHouse asset checks."""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock

import pytest
from dagster import AssetCheckResult, AssetCheckSeverity
from flight_performance_analytics_pipeline.assets.gold.gold_checks import (
    gold_dim_airport_no_nulls,
    gold_dim_carrier_no_nulls,
    gold_fact_no_orphaned_airport_keys,
    gold_fact_no_orphaned_carrier_keys,
    gold_fact_no_orphaned_date_keys,
    gold_fact_row_count,
)


def _mock_clickhouse_with_count(count: int) -> tuple[MagicMock, MagicMock]:
    """Return mocked ClickHouse resource/client with a count query result."""
    clickhouse = MagicMock()
    client = MagicMock()
    result = MagicMock()
    result.first_row = [count]
    client.query.return_value = result
    clickhouse.get_client.return_value = client
    return clickhouse, client


def _normalize_sql(sql: str) -> str:
    """Collapse whitespace for stable SQL assertions."""
    return " ".join(sql.split())


@pytest.mark.unit
@pytest.mark.parametrize(
    "row_count, expected_passed, expected_description",
    [
        (10, True, "fact_flight_delays has 10 rows."),
        (0, False, "fact_flight_delays is empty."),
    ],
)
def test_gold_fact_row_count_reports_expected_outcome(
    row_count: int,
    expected_passed: bool,
    expected_description: str,
) -> None:
    """Fact row-count check should pass with rows and fail when empty."""
    clickhouse, client = _mock_clickhouse_with_count(row_count)

    result = gold_fact_row_count(clickhouse=clickhouse)

    assert result.passed is expected_passed
    assert result.severity == AssetCheckSeverity.ERROR
    assert result.description == expected_description
    assert _normalize_sql(client.query.call_args.args[0]) == (
        "SELECT count() FROM gold.fact_flight_delays"
    )
    client.close.assert_called_once()


@pytest.mark.unit
@pytest.mark.parametrize(
    "check_fn, expected_query, bad_count, expected_fail_description",
    [
        (
            gold_dim_carrier_no_nulls,
            "SELECT count() FROM gold.dim_carrier "
            "WHERE carrier_key = '' OR carrier = '' OR carrier_name = ''",
            3,
            "3 rows have empty carrier_key, carrier, or carrier_name.",
        ),
        (
            gold_dim_airport_no_nulls,
            "SELECT count() FROM gold.dim_airport "
            "WHERE airport_key = '' OR airport = '' OR airport_name = ''",
            2,
            "2 rows have empty airport_key, airport, or airport_name.",
        ),
        (
            gold_fact_no_orphaned_carrier_keys,
            "SELECT count() FROM gold.fact_flight_delays f "
            "LEFT JOIN gold.dim_carrier d ON d.carrier_key = f.carrier_key "
            "WHERE d.carrier_key IS NULL",
            4,
            "4 fact rows have orphaned carrier_key values.",
        ),
        (
            gold_fact_no_orphaned_airport_keys,
            "SELECT count() FROM gold.fact_flight_delays f "
            "LEFT JOIN gold.dim_airport d ON d.airport_key = f.airport_key "
            "WHERE d.airport_key IS NULL",
            5,
            "5 fact rows have orphaned airport_key values.",
        ),
        (
            gold_fact_no_orphaned_date_keys,
            "SELECT count() FROM gold.fact_flight_delays f "
            "LEFT JOIN gold.dim_date d ON d.date_key = f.date_key "
            "WHERE d.date_key IS NULL",
            6,
            "6 fact rows have orphaned date_key values.",
        ),
    ],
)
def test_gold_checks_pass_and_fail_paths(
    check_fn: Callable[[MagicMock], AssetCheckResult],
    expected_query: str,
    bad_count: int,
    expected_fail_description: str,
) -> None:
    """Gold checks should pass on zero issues and fail with informative counts."""
    clickhouse_pass, client_pass = _mock_clickhouse_with_count(0)
    pass_result = check_fn(clickhouse=clickhouse_pass)  # type: ignore

    assert pass_result.passed is True
    assert pass_result.severity == AssetCheckSeverity.ERROR
    assert _normalize_sql(client_pass.query.call_args.args[0]) == _normalize_sql(expected_query)
    client_pass.close.assert_called_once()

    clickhouse_fail, client_fail = _mock_clickhouse_with_count(bad_count)
    fail_result = check_fn(clickhouse=clickhouse_fail)  # type: ignore

    assert fail_result.passed is False
    assert fail_result.severity == AssetCheckSeverity.ERROR
    assert fail_result.description == expected_fail_description
    assert _normalize_sql(client_fail.query.call_args.args[0]) == _normalize_sql(expected_query)
    client_fail.close.assert_called_once()


@pytest.mark.unit
@pytest.mark.parametrize(
    "check_fn",
    [
        gold_fact_row_count,
        gold_dim_carrier_no_nulls,
        gold_dim_airport_no_nulls,
        gold_fact_no_orphaned_carrier_keys,
        gold_fact_no_orphaned_airport_keys,
        gold_fact_no_orphaned_date_keys,
    ],
)
def test_gold_checks_close_client_when_query_fails(
    check_fn: Callable[[MagicMock], AssetCheckResult],
) -> None:
    """Every check should close the client in a failure scenario."""
    clickhouse = MagicMock()
    client = MagicMock()
    client.query.side_effect = RuntimeError("query failed")
    clickhouse.get_client.return_value = client

    with pytest.raises(RuntimeError, match="query failed"):
        check_fn(clickhouse=clickhouse)  # type: ignore

    client.close.assert_called_once()
