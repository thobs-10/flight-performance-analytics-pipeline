"""Unit tests for the sql_loader utility."""

from pathlib import Path
from unittest.mock import patch

import pytest
from flight_performance_analytics_pipeline.utils.sql_loader import load_sql

# ---------------------------------------------------------------------------
# load_sql — happy path
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_sql_returns_string_content() -> None:
    """load_sql should return the file contents as a string."""
    result = load_sql("postgres/01_create_raw_tables.sql")
    assert isinstance(result, str)
    assert len(result) > 0


@pytest.mark.unit
def test_load_sql_content_contains_expected_ddl() -> None:
    """The bronze DDL script should contain the table and schema definitions."""
    result = load_sql("postgres/01_create_raw_tables.sql")
    assert "CREATE SCHEMA" in result.upper()
    assert "CREATE TABLE" in result.upper()
    assert "bronze" in result.lower()
    assert "airline_delay" in result.lower()


@pytest.mark.unit
def test_load_sql_returns_full_file_content(tmp_path: Path) -> None:
    """load_sql should return the exact content of the SQL file."""
    sql_content = "SELECT 1;\nSELECT 2;\n"
    sql_file = tmp_path / "test.sql"
    sql_file.write_text(sql_content, encoding="utf-8")

    with patch(
        "flight_performance_analytics_pipeline.utils.sql_loader._SQL_DIR",
        tmp_path,
    ):
        result = load_sql("test.sql")

    assert result == sql_content


# ---------------------------------------------------------------------------
# load_sql — error path
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_sql_raises_file_not_found_for_missing_file() -> None:
    """load_sql should raise FileNotFoundError for a non-existent SQL file."""
    with pytest.raises(FileNotFoundError):
        load_sql("postgres/does_not_exist.sql")


@pytest.mark.unit
def test_load_sql_error_message_contains_path() -> None:
    """The FileNotFoundError message should include the attempted file path."""
    with pytest.raises(FileNotFoundError, match="does_not_exist.sql"):
        load_sql("postgres/does_not_exist.sql")


@pytest.mark.unit
def test_load_sql_raises_for_wrong_dialect_path() -> None:
    """load_sql should raise FileNotFoundError for a valid filename in the wrong dialect folder."""
    with pytest.raises(FileNotFoundError):
        load_sql("clickhouse/01_create_raw_tables.sql")


# ---------------------------------------------------------------------------
# load_sql — path resolution
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_load_sql_resolves_nested_path(tmp_path: Path) -> None:
    """load_sql should correctly resolve paths with subdirectories."""
    subdir = tmp_path / "postgres"
    subdir.mkdir()
    sql_content = "CREATE TABLE test (id INT);"
    (subdir / "nested.sql").write_text(sql_content, encoding="utf-8")

    with patch(
        "flight_performance_analytics_pipeline.utils.sql_loader._SQL_DIR",
        tmp_path,
    ):
        result = load_sql("postgres/nested.sql")

    assert result == sql_content
