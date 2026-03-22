from pathlib import Path

from dagster import Definitions, EnvVar
from dagster_dbt import DbtCliResource

from flight_performance_analytics_pipeline.assets.bronze import (
    add_metadata_columns_to_airline_delay_data,
    bronze_data_freshness,
    bronze_minimum_row_count,
    bronze_no_null_key_columns,
    read_raw_airline_delay_csv,
    write_to_bronze_airline_delay_data,
)
from flight_performance_analytics_pipeline.assets.silver.dbt_staging_assets import (
    dbt_staging_airline_delay_assets,
)
from flight_performance_analytics_pipeline.assets.silver.staging_checks import (
    staging_month_range,
    staging_no_null_key_columns,
    staging_non_negative_delays,
    staging_unique_surrogate_key,
)
from flight_performance_analytics_pipeline.resources.postgres_resource import PostgresResource

_DBT_PROJECT_DIR = Path(__file__).parent / "dbt_transformations"
# Resolve the dbt executable relative to this file so it works in any environment
# where the project is installed via uv (venv sits at the repo root).
_DBT_EXECUTABLE = Path(__file__).parents[2] / ".venv" / "bin" / "dbt"
_DBT_EXECUTABLE_ABS = _DBT_EXECUTABLE.resolve()

defs = Definitions(
    assets=[
        read_raw_airline_delay_csv,
        add_metadata_columns_to_airline_delay_data,
        write_to_bronze_airline_delay_data,
        dbt_staging_airline_delay_assets,
    ],
    asset_checks=[
        bronze_no_null_key_columns,
        bronze_minimum_row_count,
        bronze_data_freshness,
        staging_unique_surrogate_key,
        staging_no_null_key_columns,
        staging_month_range,
        staging_non_negative_delays,
    ],
    resources={
        "postgres": PostgresResource(
            host=EnvVar("POSTGRES_HOST"),
            port=EnvVar.int("POSTGRES_PORT"),
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            database=EnvVar("POSTGRES_DB"),
        ),
        "dbt": DbtCliResource(
            project_dir=str(_DBT_PROJECT_DIR),
            profiles_dir=str(_DBT_PROJECT_DIR),
            dbt_executable=str(_DBT_EXECUTABLE_ABS),
        ),
    },
)
