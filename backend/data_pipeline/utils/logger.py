"""Structured logging for the pipeline.

Every log line carries the ``batch_id`` of the run that produced it, so logs
from concurrent or historical runs can be separated after the fact. Output is
JSON by default (machine readable, suited to CloudWatch) and switches to a
human readable format when ``LOG_FORMAT=text``.

No module in the pipeline should ever call ``print``.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager
from typing import Any, Iterator

from .. import config

_CONFIGURED = False

#: Attributes present on every LogRecord, used to detect caller supplied extras.
_RESERVED = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()
) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    """Render log records as single line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "batch_id": config.BATCH_ID,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class TextFormatter(logging.Formatter):
    """Render log records for human consumption during local development."""

    def format(self, record: logging.LogRecord) -> str:
        extras = {
            k: v for k, v in record.__dict__.items() if k not in _RESERVED
        }
        suffix = ""
        if extras:
            suffix = "  " + " ".join(f"{k}={v}" for k, v in extras.items())
        base = (
            f"{self.formatTime(record, '%H:%M:%S')} "
            f"{record.levelname:<7} {record.name:<28} {record.getMessage()}{suffix}"
        )
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


def configure_logging(level: str | None = None, fmt: str | None = None) -> None:
    """Install the root handler. Safe to call more than once."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    resolved_level = (level or config.LOG_LEVEL).upper()
    resolved_fmt = (fmt or config.LOG_FORMAT).lower()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if resolved_fmt == "json" else TextFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(resolved_level)

    # Spark and botocore are extremely chatty at INFO.
    for noisy in ("py4j", "pyspark", "botocore", "boto3", "s3transfer", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger for a module."""
    configure_logging()
    return logging.getLogger(name)


@contextmanager
def log_duration(
    logger: logging.Logger, operation: str, **fields: Any
) -> Iterator[dict[str, Any]]:
    """Log the start, end and duration of an operation.

    Yields a mutable dict; anything placed in it is included in the completion
    log line, which lets callers report row counts they only learn part way
    through the operation.
    """
    started = time.perf_counter()
    context: dict[str, Any] = {}
    logger.info("%s started", operation, extra={"operation": operation, **fields})
    try:
        yield context
    except Exception:
        logger.exception(
            "%s failed",
            operation,
            extra={
                "operation": operation,
                "status": "FAILED",
                "duration_s": round(time.perf_counter() - started, 3),
                **fields,
                **context,
            },
        )
        raise
    logger.info(
        "%s completed",
        operation,
        extra={
            "operation": operation,
            "status": "SUCCESS",
            "duration_s": round(time.perf_counter() - started, 3),
            **fields,
            **context,
        },
    )