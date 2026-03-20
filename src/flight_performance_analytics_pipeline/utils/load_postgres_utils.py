from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from dagster import AssetExecutionContext
from sqlalchemy import text
from sqlalchemy.engine import Engine

from flight_performance_analytics_pipeline.utils.sql_loader import load_sql


def get_csv_file_path(csv_path: str) -> str:
    """Resolve a CSV path string to an absolute path."""
    path = Path(csv_path)
    if not path.is_absolute():
        # utils/ is 3 levels deep inside src/<package>/utils/
        path = Path(__file__).parents[3] / path
    return str(path)


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
