from __future__ import annotations

import logging
from pathlib import Path


LOGGER_NAME = "backtester"
LOG_FORMAT = "%(levelname)s | %(name)s | %(message)s"
#LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging(log_level: str, log_file_path: Path) -> logging.Logger:
    """Configure console and file logging for the application."""

    log_file_path.parent.mkdir(parents=True, exist_ok=True)

    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format=LOG_FORMAT,
        handlers=[console_handler, file_handler],
        force=True,
    )

    logger = logging.getLogger(LOGGER_NAME)
    logger.debug("Logging initialized at level %s.", log_level.upper())
    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    """Return an app logger or one of its children."""

    if not name:
        return logging.getLogger(LOGGER_NAME)
    return logging.getLogger(f"{LOGGER_NAME}.{name}")
