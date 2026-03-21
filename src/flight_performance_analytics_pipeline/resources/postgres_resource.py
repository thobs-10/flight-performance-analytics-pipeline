from dagster import ConfigurableResource
from sqlalchemy import URL, create_engine
from sqlalchemy.engine import Engine


class PostgresResource(ConfigurableResource):
    """Dagster resource that provides a SQLAlchemy engine for PostgreSQL.

    Config fields are populated by Dagster at runtime, typically via EnvVar(...)
    in definitions.py.
    """

    host: str
    port: int
    user: str
    password: str
    database: str
    pool_size: int = 5
    max_overflow: int = 2

    def get_engine(self) -> Engine:
        """Return a SQLAlchemy engine connected to the configured PostgreSQL instance."""
        url = URL.create(
            drivername="postgresql+psycopg",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )
        return create_engine(
            url,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_pre_ping=True,
        )
