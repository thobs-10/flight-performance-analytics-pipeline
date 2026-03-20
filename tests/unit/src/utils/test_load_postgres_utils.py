"""Unit tests for load_postgres_utils functions — edge cases and isolation tests."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import polars as pl
import pytest
from sqlalchemy.exc import OperationalError, SQLAlchemyError

from flight_performance_analytics_pipeline.utils.load_postgres_utils import (
    add_ingested_column,
    get_csv_file_path,
    load_query,
    write_to_database,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """Minimal two-row DataFrame for utility function tests."""
    return pl.DataFrame(
        {"year": [2025, 2025], "carrier": ["9E", "AA"], "arr_delay": [100.0, 200.0]}
    )


@pytest.fixture
def empty_df() -> pl.DataFrame:
    """Empty DataFrame with the same schema."""
    return pl.DataFrame({"year": [], "carrier": [], "arr_delay": []})


@pytest.fixture
def single_row_df() -> pl.DataFrame:
    """Single-row DataFrame."""
    return pl.DataFrame({"year": [2025], "carrier": ["9E"], "arr_delay": [100.0]})


# ---------------------------------------------------------------------------
# get_csv_file_path — edge cases
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_get_csv_file_path_returns_string() -> None:
    """Return type should always be str."""
    result = get_csv_file_path("data/some_file.csv")
    assert isinstance(result, str)


@pytest.mark.unit
def test_get_csv_file_path_does_not_mutate_absolute() -> None:
    """An already-absolute path must not be altered in any way."""
    absolute = "/data/warehouse/flights.csv"
    assert get_csv_file_path(absolute) == absolute


@pytest.mark.unit
def test_get_csv_file_path_relative_does_not_contain_double_slash() -> None:
    """The resolved path should not contain double slashes."""
    result = get_csv_file_path("data/file.csv")
    assert "//" not in result


# ---------------------------------------------------------------------------
# add_ingested_column — edge cases
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_add_ingested_column_on_empty_dataframe(empty_df: pl.DataFrame) -> None:
    """add_ingested_column should handle an empty DataFrame without error."""
    result = add_ingested_column(empty_df)
    assert "_ingested_at" in result.columns
    assert len(result) == 0


@pytest.mark.unit
def test_add_ingested_column_on_single_row(single_row_df: pl.DataFrame) -> None:
    """add_ingested_column should work correctly on a single-row DataFrame."""
    result = add_ingested_column(single_row_df)
    assert len(result) == 1
    assert "_ingested_at" in result.columns


@pytest.mark.unit
def test_add_ingested_column_timestamp_is_recent(sample_df: pl.DataFrame) -> None:
    """The _ingested_at timestamp should be within 5 seconds of now."""
    before = datetime.now(timezone.utc)
    result = add_ingested_column(sample_df)
    after = datetime.now(timezone.utc)

    timestamps = result["_ingested_at"].to_list()
    for ts in timestamps:
        # Polars returns datetime objects; ensure they're in range
        ts_aware = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
        assert before <= ts_aware <= after


@pytest.mark.unit
def test_add_ingested_column_all_rows_have_same_timestamp(sample_df: pl.DataFrame) -> None:
    """All rows should receive the same ingestion timestamp within a single call."""
    result = add_ingested_column(sample_df)
    timestamps = result["_ingested_at"].to_list()
    assert len(set(str(t) for t in timestamps)) == 1


@pytest.mark.unit
def test_add_ingested_column_does_not_modify_input(sample_df: pl.DataFrame) -> None:
    """The original DataFrame should not be modified (Polars is immutable)."""
    original_columns = sample_df.columns.copy()
    add_ingested_column(sample_df)
    assert sample_df.columns == original_columns


# ---------------------------------------------------------------------------
# load_query — edge cases
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_query_passes_ddl_to_execute() -> None:
    """The exact DDL returned by load_sql should be passed to conn.execute."""
    fake_ddl = "CREATE SCHEMA IF NOT EXISTS bronze;"
    mock_conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_context = MagicMock()

    with patch(
        "flight_performance_analytics_pipeline.utils.load_postgres_utils.load_sql",
        return_value=fake_ddl,
    ):
        load_query(mock_engine, mock_context)

    # Verify execute was called with a SQLAlchemy text() wrapping the DDL
    args = mock_conn.execute.call_args[0][0]
    assert fake_ddl in str(args)


@pytest.mark.unit
def test_load_query_logs_twice() -> None:
    """load_query should log exactly twice: once at start and once on completion."""
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_context = MagicMock()

    with patch(
        "flight_performance_analytics_pipeline.utils.load_postgres_utils.load_sql",
        return_value="SELECT 1;",
    ):
        load_query(mock_engine, mock_context)

    assert mock_context.log.info.call_count == 2


@pytest.mark.unit
def test_load_query_propagates_db_error() -> None:
    """load_query should not swallow database errors."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    # SQLAlchemy 2.0 OperationalError requires (statement, params, orig)
    mock_conn.execute.side_effect = OperationalError("stmt", {}, Exception("connection refused"))
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_context = MagicMock()

    with patch(
        "flight_performance_analytics_pipeline.utils.load_postgres_utils.load_sql",
        return_value="CREATE SCHEMA IF NOT EXISTS bronze;",
    ):
        with pytest.raises(SQLAlchemyError):
            load_query(mock_engine, mock_context)


# ---------------------------------------------------------------------------
# write_to_database — edge cases
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_to_database_uses_append_mode(sample_df: pl.DataFrame) -> None:
    """write_to_database should always use if_table_exists='append'."""
    mock_engine = MagicMock()
    mock_context = MagicMock()

    with patch.object(pl.DataFrame, "write_database", return_value=-1) as mock_write:
        write_to_database(sample_df, mock_engine, "bronze.airline_delay_data", mock_context)

    # call_args[0] = positional (self), call_args[1] = kwargs
    kwargs = mock_write.call_args[1]
    assert kwargs.get("if_table_exists") == "append"


@pytest.mark.unit
def test_write_to_database_passes_correct_table_name(sample_df: pl.DataFrame) -> None:
    """write_to_database should pass the given table_name to write_database."""
    mock_engine = MagicMock()
    mock_context = MagicMock()
    table_name = "bronze.airline_delay_data"

    with patch.object(pl.DataFrame, "write_database", return_value=-1) as mock_write:
        write_to_database(sample_df, mock_engine, table_name, mock_context)

    kwargs = mock_write.call_args[1]
    assert kwargs.get("table_name") == table_name


@pytest.mark.unit
def test_write_to_database_returns_zero_for_empty_df(empty_df: pl.DataFrame) -> None:
    """write_to_database should return 0 for an empty DataFrame."""
    mock_engine = MagicMock()
    mock_context = MagicMock()

    with patch.object(pl.DataFrame, "write_database", return_value=-1):
        result = write_to_database(empty_df, mock_engine, "bronze.airline_delay_data", mock_context)

    assert result == 0


@pytest.mark.unit
def test_write_to_database_log_includes_table_name(sample_df: pl.DataFrame) -> None:
    """The log message should include the target table name."""
    mock_engine = MagicMock()
    mock_context = MagicMock()
    table_name = "bronze.airline_delay_data"

    with patch.object(pl.DataFrame, "write_database", return_value=-1):
        write_to_database(sample_df, mock_engine, table_name, mock_context)

    log_message = mock_context.log.info.call_args[0][0]
    assert table_name in log_message
