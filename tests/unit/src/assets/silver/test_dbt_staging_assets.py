"""Unit tests for the silver dbt asset wrapper."""

from __future__ import annotations

import importlib
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def dbt_staging_module() -> Any:
    """Reload the silver dbt module with prepare_if_dev patched for test isolation."""
    with patch("dagster_dbt.DbtProject.prepare_if_dev", return_value=None):
        import flight_performance_analytics_pipeline.assets.silver.dbt_staging_assets as module

        importlib.reload(module)
    return module


@pytest.mark.unit
def test_dbt_staging_asset_uses_build_selector(dbt_staging_module: Any) -> None:
    """The silver dbt asset must call dbt build for the staging selector."""
    context = MagicMock()
    dbt = MagicMock()
    expected_events = [{"event": "model built"}, {"event": "tests executed"}]
    dbt.cli.return_value.stream.return_value = iter(expected_events)
    compute_fn = (
        dbt_staging_module.dbt_staging_airline_delay_assets.node_def.compute_fn.decorated_fn
    )

    events = list(compute_fn(context=context, dbt=dbt))

    dbt.cli.assert_called_once_with(["build", "--select", "staging"], context=context)
    assert events == expected_events


@pytest.mark.unit
def test_dbt_staging_asset_yields_stream_generator(dbt_staging_module: Any) -> None:
    """The silver dbt asset should yield directly from the dbt stream generator."""
    context = MagicMock()
    dbt = MagicMock()

    def _stream() -> Generator[str, None, None]:
        yield "event-1"
        yield "event-2"

    dbt.cli.return_value.stream.return_value = _stream()
    compute_fn = (
        dbt_staging_module.dbt_staging_airline_delay_assets.node_def.compute_fn.decorated_fn
    )

    result = list(compute_fn(context=context, dbt=dbt))

    assert result == ["event-1", "event-2"]
