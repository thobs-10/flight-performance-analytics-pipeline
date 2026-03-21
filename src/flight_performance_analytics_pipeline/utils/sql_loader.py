from pathlib import Path

from flight_performance_analytics_pipeline.logging.logging import Logger

# Resolve the repo root relative to this file:
# src/flight_performance_analytics_pipeline/utils/ -> up 4 levels -> repo root
_REPO_ROOT = Path(__file__).parents[3]
_SQL_DIR = _REPO_ROOT / "sql_scripts"
_logger = Logger()


def load_sql(relative_path: str) -> str:
    """Read a SQL file from the sql_scripts directory and return its contents.

    Args:
        relative_path: Path relative to the sql_scripts/ directory,
        e.g. "postgres/01_create_raw_tables.sql".

    Returns:
        The SQL file contents as a string.

    Raises:
        FileNotFoundError: If the SQL file does not exist or the path is invalid.
    """

    base_sql_dir = _SQL_DIR.resolve()
    sql_file = (base_sql_dir / relative_path).resolve()

    # Prevent directory traversal by ensuring the resolved path is within sql_scripts.
    if not sql_file.is_relative_to(base_sql_dir):
        _logger.error(f"Attempted to load SQL script outside of sql_scripts directory: {sql_file}")
        raise FileNotFoundError("SQL script not found or invalid path specified.")
    if not sql_file.exists():
        _logger.error(f"SQL script not found: {sql_file}")
        raise FileNotFoundError(f"SQL script not found: {sql_file}")

    _logger.info(f"Loading SQL script: {sql_file}")
    return sql_file.read_text(encoding="utf-8")
