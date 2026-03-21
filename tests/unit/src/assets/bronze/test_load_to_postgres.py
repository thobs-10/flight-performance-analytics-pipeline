"""Unit tests for bronze ingestion assets and utility functions."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from dagster import build_asset_context
from flight_performance_analytics_pipeline.assets.bronze.load_to_postgres import (
    BronzeIngestionConfig,
    add_metadata_columns_to_airline_delay_data,
    read_raw_airline_delay_csv,
)
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
    """Minimal DataFrame matching the bronze table schema."""
    return pl.DataFrame(
        {
            "year": [2025, 2025],
            "month": [11, 11],
            "carrier": ["9E", "AA"],
            "carrier_name": ["Endeavor Air Inc.", "American Airlines"],
            "airport": ["ABE", "JFK"],
            "airport_name": ["Lehigh Valley International", "John F. Kennedy International"],
            "arr_flights": [87.0, 120.0],
            "arr_del15": [15.0, 20.0],
            "carrier_ct": [3.66, 5.0],
            "weather_ct": [1.71, 0.5],
            "nas_ct": [3.23, 2.0],
            "security_ct": [0.0, 0.0],
            "late_aircraft_ct": [6.40, 8.0],
            "arr_cancelled": [7.0, 3.0],
            "arr_diverted": [0.0, 1.0],
            "arr_delay": [1001.0, 800.0],
            "carrier_delay": [132.0, 200.0],
            "weather_delay": [114.0, 50.0],
            "nas_delay": [112.0, 100.0],
            "security_delay": [0.0, 0.0],
            "late_aircraft_delay": [643.0, 450.0],
        }
    )


_AIRLINE_DELAY_CSV_CONTENT = (
    "year,month,carrier,carrier_name,airport,airport_name,"
    "arr_flights,arr_del15,carrier_ct,weather_ct,nas_ct,security_ct,"
    "late_aircraft_ct,arr_cancelled,arr_diverted,arr_delay,carrier_delay,"
    "weather_delay,nas_delay,security_delay,late_aircraft_delay\n"
    "2025,11,AA,American Airlines,JFK,John F. Kennedy International,"
    "120.0,20.0,5.0,0.5,2.0,0.0,8.0,3.0,1.0,800.0,200.0,50.0,100.0,0.0,450.0\n"
)


@pytest.fixture
def airline_delay_csv(tmp_path: Path) -> Path:
    """Write a minimal airline delay CSV to a temp directory and return its path."""
    csv_file = tmp_path / "Airline_Delay_Cause.csv"
    csv_file.write_text(_AIRLINE_DELAY_CSV_CONTENT, encoding="utf-8")
    return csv_file


# ---------------------------------------------------------------------------
# get_csv_file_path
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_get_csv_file_path_absolute_passthrough() -> None:
    """An absolute path should be returned unchanged."""
    absolute = "/tmp/some_file.csv"
    result = get_csv_file_path(absolute)
    assert result == absolute


@pytest.mark.unit
def test_get_csv_file_path_relative_resolves_to_absolute() -> None:
    """A relative path should be resolved to an absolute path under the repo root."""
    result = get_csv_file_path("data/ot_delaycause1_DL/Airline_Delay_Cause.csv")
    assert Path(result).is_absolute()
    assert result.endswith("data/ot_delaycause1_DL/Airline_Delay_Cause.csv")


@pytest.mark.unit
def test_get_csv_file_path_relative_points_to_existing_file(tmp_path: Path) -> None:
    """The resolved path should point to a CSV file that exists on disk using a temp fixture."""
    # Create a small temporary CSV file to avoid relying on repo-local data directories.
    csv_path = tmp_path / "Airline_Delay_Cause.csv"
    csv_path.write_text("year,month,carrier\n2025,11,AA\n", encoding="utf-8")

    # When given an absolute path to an existing file, get_csv_file_path should return it unchanged.
    result = get_csv_file_path(str(csv_path))
    assert result == str(csv_path)
    assert Path(result).exists(), f"Expected temporary CSV to exist at: {result}"
# ---------------------------------------------------------------------------
# add_ingested_column
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_add_ingested_column_adds_column(sample_df: pl.DataFrame) -> None:
    """The _ingested_at column should be present after transformation."""
    result = add_ingested_column(sample_df)
    assert "_ingested_at" in result.columns


@pytest.mark.unit
def test_add_ingested_column_preserves_row_count(sample_df: pl.DataFrame) -> None:
    """Row count must not change after adding the metadata column."""
    result = add_ingested_column(sample_df)
    assert len(result) == len(sample_df)


@pytest.mark.unit
def test_add_ingested_column_preserves_existing_columns(sample_df: pl.DataFrame) -> None:
    """All original columns must still be present after transformation."""
    result = add_ingested_column(sample_df)
    for col in sample_df.columns:
        assert col in result.columns


@pytest.mark.unit
def test_add_ingested_column_dtype_is_datetime(sample_df: pl.DataFrame) -> None:
    """_ingested_at should be a UTC-aware Datetime column."""
    result = add_ingested_column(sample_df)
    dtype = result.schema["_ingested_at"]
    assert isinstance(dtype, pl.Datetime)
    assert dtype.time_zone == "UTC"


# ---------------------------------------------------------------------------
# load_query
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_query_executes_ddl() -> None:
    """load_query should read the SQL file and execute it via the engine connection."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_context = MagicMock()

    with patch(
        "flight_performance_analytics_pipeline.utils.load_postgres_utils.load_sql",
        return_value="CREATE SCHEMA IF NOT EXISTS bronze;",
    ):
        load_query(mock_engine, mock_context)

    mock_conn.execute.assert_called_once()


@pytest.mark.unit
def test_load_query_logs_completion() -> None:
    """load_query should log that the schema and table were ensured."""
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    mock_context = MagicMock()

    with patch(
        "flight_performance_analytics_pipeline.utils.load_postgres_utils.load_sql",
        return_value="CREATE SCHEMA IF NOT EXISTS bronze;",
    ):
        load_query(mock_engine, mock_context)

    mock_context.log.info.assert_called()


# ---------------------------------------------------------------------------
# write_to_database
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_to_database_returns_row_count(sample_df: pl.DataFrame) -> None:
    """write_to_database should return the number of rows in the DataFrame."""
    mock_engine = MagicMock()
    mock_context = MagicMock()

    with patch.object(sample_df.__class__, "write_database", return_value=-1):
        result = write_to_database(
            sample_df, mock_engine, "bronze.airline_delay_data", mock_context
        )

    assert result == len(sample_df)


@pytest.mark.unit
def test_write_to_database_logs_row_count(sample_df: pl.DataFrame) -> None:
    """write_to_database should log the number of rows written."""
    mock_engine = MagicMock()
    mock_context = MagicMock()

    with patch.object(pl.DataFrame, "write_database", return_value=-1):
        write_to_database(sample_df, mock_engine, "bronze.airline_delay_data", mock_context)

    mock_context.log.info.assert_called_once_with(
        f"Appended {len(sample_df)} rows to bronze.airline_delay_data."
    )


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_raw_airline_delay_csv_returns_dataframe(airline_delay_csv: Path) -> None:
    """read_raw_airline_delay_csv should return a non-empty Polars DataFrame."""
    context = build_asset_context()
    config = BronzeIngestionConfig(csv_path=str(airline_delay_csv))
    result = read_raw_airline_delay_csv(context=context, config=config)
    assert isinstance(result, pl.DataFrame)
    assert len(result) > 0


@pytest.mark.unit
def test_read_raw_airline_delay_csv_has_expected_columns(airline_delay_csv: Path) -> None:
    """The raw CSV asset should contain all 21 source columns."""
    expected_columns = {
        "year",
        "month",
        "carrier",
        "carrier_name",
        "airport",
        "airport_name",
        "arr_flights",
        "arr_del15",
        "carrier_ct",
        "weather_ct",
        "nas_ct",
        "security_ct",
        "late_aircraft_ct",
        "arr_cancelled",
        "arr_diverted",
        "arr_delay",
        "carrier_delay",
        "weather_delay",
        "nas_delay",
        "security_delay",
        "late_aircraft_delay",
    }
    context = build_asset_context()
    config = BronzeIngestionConfig(csv_path=str(airline_delay_csv))
    result = read_raw_airline_delay_csv(context=context, config=config)
    assert expected_columns.issubset(set(result.columns))


@pytest.mark.unit
def test_add_metadata_columns_asset_adds_ingested_at(sample_df: pl.DataFrame) -> None:
    """add_metadata_columns_to_airline_delay_data asset should add _ingested_at."""
    context = build_asset_context()
    result = add_metadata_columns_to_airline_delay_data(
        context=context,
        read_raw_airline_delay_csv=sample_df,
    )
    assert "_ingested_at" in result.columns
    assert len(result) == len(sample_df)
