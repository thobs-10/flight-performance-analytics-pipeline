"""ClickHouse resource for the gold analytical layer.

Provides a thin wrapper around clickhouse-connect with helpers for bulk DataFrame
insertion and raw command execution.  Connection credentials are supplied via
Dagster EnvVar definitions in definitions.py.
"""

import pandas as pd
from dagster import ConfigurableResource

try:
    import clickhouse_connect
    from clickhouse_connect.driver.client import Client
except ImportError as exc:
    raise ImportError(
        "clickhouse-connect is required for the ClickHouse resource. "
        "Install it with: pip install clickhouse-connect"
    ) from exc


class ClickhouseResource(ConfigurableResource):
    """Dagster resource that provides a ClickHouse client.

    Config fields are populated by Dagster at runtime, typically via EnvVar(...)
    in definitions.py.
    """

    host: str
    port: int
    user: str
    password: str
    database: str

    def get_client(self) -> Client:
        """Return a connected ClickHouse client."""
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.database,
        )

    def insert_dataframe(
        self,
        table: str,
        df: pd.DataFrame,
        *,
        truncate: bool = False,
        chunk_size: int = 10_000,
    ) -> None:
        """Bulk-insert a Pandas DataFrame into the given ClickHouse table.

        Args:
            table: Fully-qualified table name (e.g. ``gold.dim_carrier``).
            df: DataFrame whose columns match the target table schema exactly.
            truncate: If True, truncates the target table before inserting rows.
            chunk_size: Number of rows per insert batch to limit memory usage.
        """
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")

        client = self.get_client()
        try:
            if truncate:
                client.command(f"TRUNCATE TABLE {table}")  # nosec B608

            if df.empty:
                return

            for start in range(0, len(df), chunk_size):
                batch = df.iloc[start : start + chunk_size]
                client.insert_df(table, batch)
        finally:
            client.close()

    def execute(self, query: str) -> None:
        """Execute a DDL or DML command against ClickHouse.

        Args:
            query: SQL statement to execute (no result set expected).
        """
        client = self.get_client()
        try:
            client.command(query)
        finally:
            client.close()
