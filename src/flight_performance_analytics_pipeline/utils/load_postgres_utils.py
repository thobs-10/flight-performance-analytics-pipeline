from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from dagster import AssetExecutionContext
from sqlalchemy import text
from sqlalchemy.engine import Engine

from flight_performance_analytics_pipeline.utils.sql_loader import load_sql


def get_csv_file_path(csv_path: str) -> str:
    """
    Resolve a CSV path string to an absolute path within the project root.

    The project root is assumed to be three levels above this file
    (i.e. ``Path(__file__).parents[3]``). Relative paths are resolved
    against this base directory. Absolute and relative paths that
    resolve outside the base directory are rejected.

    :param csv_path: CSV file path, absolute or relative to the project root.
    :return: Absolute path to the CSV file as a string.
    :raises ValueError: If the resolved path is outside the allowed base directory.
    """
    base_dir = Path(__file__).parents[3].resolve()
    path = Path(csv_path)

    if path.is_absolute():
        resolved_path = path.resolve()
    else:
        # utils/ is 3 levels deep inside src/<package>/utils/
        resolved_path = (base_dir / path).resolve()

    # Ensure the resolved path is within the allowed base directory to
    # prevent path traversal and unintended file access.
    if not resolved_path.is_relative_to(base_dir):
        raise ValueError(
            f"CSV path '{csv_path}' resolves outside the allowed base directory "
            f"'{base_dir}'."
        )

    return str(resolved_path)
def add_ingested_column(df: pl.DataFrame) -> pl.DataFrame:
    """Add a UTC ingestion timestamp column to the DataFrame."""
    return df.with_columns(
        pl.lit(datetime.now(timezone.utc)).cast(pl.Datetime("us", "UTC")).alias("_ingested_at")
    )


def load_query(
    engine: Engine,
    context: AssetExecutionContext,
) -> None:
    """Execute the DDL script to create the bronze schema and table if they do not exist."""
    context.log.info("Loading SQL script for creating raw tables.")
    ddl = load_sql("postgres/01_create_raw_tables.sql")
    with engine.begin() as conn:
        conn.execute(text(ddl))
    context.log.info("Ensured bronze schema and table exist.")


def write_to_database(
    df: pl.DataFrame,
    engine: Engine,
    table_name: str,
    context: AssetExecutionContext,
) -> int:
    """Write the DataFrame to the specified table using append mode."""
    df.write_database(
        table_name=table_name,
        connection=engine,
        if_table_exists="append",
    )
    rows_written = len(df)
    context.log.info(f"Appended {rows_written} rows to {table_name}.")
    return rows_written
