"""Structured logging implementation for the Lakehouse Data Platform."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JSONFormatter(logging.Formatter):
    """Custom logging formatter that outputs log records as structured JSON."""

    def format(self, record: logging.LogRecord) -> str:
        log_payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include custom context attributes if present in the record
        context_keys = ["stage", "batch_id", "entity", "duration_seconds", "record_count"]
        for key in context_keys:
            if hasattr(record, key):
                log_payload[key] = getattr(record, key)

        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_payload, default=str)


def get_structured_logger(
    name: str = "data_platform",
    level: int = logging.INFO,
    as_json: bool = False,
) -> logging.Logger:
    """Factory that configures and returns a structured logger.

    Args:
        name: Logger name.
        level: Logging severity level.
        as_json: If True, uses JSONFormatter; otherwise uses standard text formatter.

    Returns:
        Configured logging.Logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers if already configured
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        if as_json:
            handler.setFormatter(JSONFormatter())
        else:
            fmt = logging.Formatter(
                "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(fmt)
        logger.addHandler(handler)

    return logger
