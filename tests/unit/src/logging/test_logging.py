"""Unit tests for the Logger singleton and logging module configuration."""

from __future__ import annotations

import importlib
from collections.abc import Generator
from pathlib import Path
from typing import Any

import flight_performance_analytics_pipeline.logging.logging as log_module
import pytest
from flight_performance_analytics_pipeline.logging.logging import LOG_DIR, LOG_FILE, Logger
from loguru import logger as loguru_logger

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_singleton() -> Generator[None, None, None]:
    """Reset the Logger singleton before and after each test for isolation."""
    Logger._instance = None
    yield
    Logger._instance = None


@pytest.fixture()
def capture_sink() -> Generator[list[str], None, None]:
    """Attach a temporary in-memory loguru sink and yield the captured messages."""
    messages: list[str] = []

    def _sink(message: Any) -> None:
        messages.append(str(message))

    handler_id = loguru_logger.add(_sink, level="DEBUG")
    yield messages
    loguru_logger.remove(handler_id)


# ---------------------------------------------------------------------------
# Singleton pattern
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_logger_singleton() -> None:
    """Two Logger() calls must return the exact same object."""
    logger1 = Logger()
    logger2 = Logger()
    assert logger1 is logger2, "Logger instances are not the same (singleton pattern broken)"


@pytest.mark.unit
def test_logger_singleton_preserves_existing_instance() -> None:
    """Once the instance exists, subsequent calls never replace it."""
    first = Logger()
    Logger._instance = first
    assert Logger() is first


# ---------------------------------------------------------------------------
# Log methods write to the in-memory sink
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_logger_info_writes_message(capture_sink: list[str]) -> None:
    """Logger.info() must forward the message to loguru."""
    Logger().info("info test message")
    assert any("info test message" in m for m in capture_sink)


@pytest.mark.unit
def test_logger_error_writes_message(capture_sink: list[str]) -> None:
    """Logger.error() must forward the message to loguru."""
    Logger().error("error test message")
    assert any("error test message" in m for m in capture_sink)


@pytest.mark.unit
def test_logger_warning_writes_message(capture_sink: list[str]) -> None:
    """Logger.warning() must forward the message to loguru."""
    Logger().warning("warning test message")
    assert any("warning test message" in m for m in capture_sink)


@pytest.mark.unit
def test_logger_debug_writes_message(capture_sink: list[str]) -> None:
    """Logger.debug() must forward the message to loguru at DEBUG level."""
    Logger().debug("debug test message")
    assert any("debug test message" in m for m in capture_sink)


# ---------------------------------------------------------------------------
# Module-level configuration defaults
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_log_dir_exists() -> None:
    """LOG_DIR must be created when the module is imported."""
    assert LOG_DIR.exists()
    assert LOG_DIR.is_dir()


@pytest.mark.unit
def test_log_file_is_inside_log_dir() -> None:
    """LOG_FILE must sit directly inside LOG_DIR and follow the naming convention."""
    assert LOG_FILE.parent == LOG_DIR
    assert LOG_FILE.name.startswith("system_")


@pytest.mark.unit
def test_log_level_default() -> None:
    """LOG_LEVEL must default to INFO when the env var is not set."""
    assert log_module.LOG_LEVEL == "INFO"


@pytest.mark.unit
def test_log_rotation_default() -> None:
    """LOG_ROTATION must default to '10 MB'."""
    assert log_module.LOG_ROTATION == "10 MB"


@pytest.mark.unit
def test_log_retention_default() -> None:
    """LOG_RETENTION must default to '30 days'."""
    assert log_module.LOG_RETENTION == "30 days"


# ---------------------------------------------------------------------------
# Environment variable overrides
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_log_level_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """LOG_LEVEL must reflect the LOG_LEVEL environment variable after a reload."""
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    importlib.reload(log_module)
    assert log_module.LOG_LEVEL == "DEBUG"


@pytest.mark.unit
def test_log_dir_from_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """LOG_DIR must use the LOG_DIR environment variable when set."""
    custom_dir = tmp_path / "custom_logs"
    monkeypatch.setenv("LOG_DIR", str(custom_dir))
    importlib.reload(log_module)
    assert log_module.LOG_DIR == custom_dir
    assert custom_dir.exists()


@pytest.mark.unit
def test_log_rotation_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """LOG_ROTATION must reflect the LOG_ROTATION environment variable after a reload."""
    monkeypatch.setenv("LOG_ROTATION", "5 MB")
    importlib.reload(log_module)
    assert log_module.LOG_ROTATION == "5 MB"


@pytest.mark.unit
def test_log_retention_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """LOG_RETENTION must reflect the LOG_RETENTION environment variable after a reload."""
    monkeypatch.setenv("LOG_RETENTION", "7 days")
    importlib.reload(log_module)
    assert log_module.LOG_RETENTION == "7 days"


# ---------------------------------------------------------------------------
# Log file is actually written to
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_log_file_created_after_write() -> None:
    """The log file must exist after the first message is written."""
    Logger().info("trigger file creation")
    assert LOG_FILE.exists()


@pytest.mark.unit
def test_log_file_contains_written_message() -> None:
    """A message written via Logger.info() must appear in the log file."""
    sentinel = "sentinel_log_file_content_check"
    Logger().info(sentinel)
    assert sentinel in LOG_FILE.read_text(encoding="utf-8")
