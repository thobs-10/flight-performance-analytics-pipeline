"""Dagster dbt assets for the silver/staging layer.

The @dbt_assets decorator wraps the dbt staging model so Dagster can orchestrate
and observe it as a first-class asset in the asset graph, downstream of the bronze
ingestion assets.
"""

from collections.abc import Generator
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parents[2] / "dbt_transformations"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)

# In local development, compile the dbt project to regenerate the manifest when
# source files change. In CI/production the manifest is pre-built.
dbt_project.prepare_if_dev()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="staging",
)
def dbt_staging_airline_delay_assets(
    context: AssetExecutionContext, dbt: DbtCliResource
) -> Generator[Any, None, None]:
    """Run dbt staging models that clean and type-cast the bronze airline delay data."""
    yield from dbt.cli(["run", "--select", "staging"], context=context).stream()
