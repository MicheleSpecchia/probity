from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

REQUIRED_LOG_FIELDS = ("run_id", "job_name", "code_version", "config_hash")
_LOGGING_CONFIGURED = False


class JsonFormatter(logging.Formatter):
    """Emit stable JSON logs for audit and replay."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "msg": record.getMessage(),
        }

        for field in REQUIRED_LOG_FIELDS:
            payload[field] = getattr(record, field, "unknown")

        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            payload.update(extra_fields)

        return json.dumps(payload, separators=(",", ":"), ensure_ascii=True, sort_keys=True)


def configure_json_logging(level: str = "INFO") -> None:
    global _LOGGING_CONFIGURED

    if _LOGGING_CONFIGURED:
        return

    root_logger = logging.getLogger()
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())

    root_logger.handlers = [handler]
    root_logger.setLevel(level.upper())
    _LOGGING_CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    configure_json_logging()
    return logging.getLogger(name)
