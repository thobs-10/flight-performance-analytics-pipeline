"""This module sets up logging for the flight performance analytics pipeline.

It enables logs to be saved to a file and printed to the console, with a consistent
format and log level. It uses a singleton pattern to ensure that the logger is
configured only once and can be imported and used across the entire project without
reconfiguration.

Configuration is driven by environment variables:
  LOG_DIR      - directory to write log files (default: logs)
  LOG_LEVEL    - minimum log level (default: INFO)
  LOG_ROTATION - max file size before rotation (default: 10 MB)
  LOG_RETENTION - how long to keep rotated logs (default: 30 days)
"""

import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"system_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_ROTATION = os.getenv("LOG_ROTATION", "10 MB")
LOG_RETENTION = os.getenv("LOG_RETENTION", "30 days")

logger.add(
    LOG_FILE,
    rotation=LOG_ROTATION,
    retention=LOG_RETENTION,
    level=LOG_LEVEL,
    format="{time:YYYY-MM-DD_HH:mm:ss} | {level} | {message}",
)


class Logger:
    """Singleton Logger class to provide a consistent logging interface across the project."""

    _instance: "Logger | None" = None

    def __new__(cls) -> "Logger":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def info(self, message: str) -> None:
        """Log an info message."""
        logger.info(message)

    def error(self, message: str) -> None:
        """Log an error message."""
        logger.error(message)

    def warning(self, message: str) -> None:
        """Log a warning message."""
        logger.warning(message)

    def debug(self, message: str) -> None:
        """Log a debug message."""
        logger.debug(message)
