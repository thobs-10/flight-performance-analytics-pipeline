from dagster import Definitions, EnvVar

from flight_performance_analytics_pipeline.assets.bronze.load_to_postgres import (
    add_metadata_columns_to_airline_delay_data,
    read_raw_airline_delay_csv,
    write_to_bronze_airline_delay_data,
)
from flight_performance_analytics_pipeline.resources.postgres_resource import PostgresResource

defs = Definitions(
    assets=[
        read_raw_airline_delay_csv,
        add_metadata_columns_to_airline_delay_data,
        write_to_bronze_airline_delay_data,
    ],
    resources={
        "postgres": PostgresResource(
            host=EnvVar("POSTGRES_HOST"),
            port=EnvVar.int("POSTGRES_PORT"),
            user=EnvVar("POSTGRES_USER"),
            password=EnvVar("POSTGRES_PASSWORD"),
            database=EnvVar("POSTGRES_DB"),
        )
    },
)
