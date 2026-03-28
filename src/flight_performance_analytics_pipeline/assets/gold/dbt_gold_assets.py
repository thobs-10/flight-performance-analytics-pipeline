"""Dagster dbt assets for the gold/marts layer.

Mirrors the silver staging asset pattern.  The @dbt_assets decorator wraps all
dbt mart models so Dagster can orchestrate and observe them as first-class assets
downstream of the silver staging models.
"""

from collections.abc import Generator
from pathlib import Path
from typing import Any, Dict

from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(__file__).parents[2] / "dbt_transformations"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
)

dbt_project.prepare_if_dev()


class _GoldDbtTranslator(DagsterDbtTranslator):
    """Assigns all mart dbt models to the 'gold' asset group."""

    def get_group_name(self, dbt_resource_props: Dict[str, Any]) -> str:
        """Return the Dagster group name for a dbt resource."""
        return "gold"


@dbt_assets(
    manifest=dbt_project.manifest_path,
    select="marts",
    dagster_dbt_translator=_GoldDbtTranslator(),
)
def dbt_gold_airline_delay_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
) -> Generator[Any, None, None]:
    """Build dbt mart models and execute their schema tests."""
    yield from dbt.cli(["build", "--select", "marts"], context=context).stream()
