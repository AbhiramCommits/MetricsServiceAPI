"""Structured JSON logging with request/run correlation IDs."""

from __future__ import annotations

import contextvars
import json
import logging
import logging.config
import uuid

request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-"
)
run_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("run_id", default="-")


def new_request_id() -> str:
    return uuid.uuid4().hex[:16]


class CorrelationFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        record.run_id = run_id_var.get()
        return True


class JsonFormatter(logging.Formatter):
    _RESERVED = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
        "request_id",
        "run_id",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", "-"),
            "run_id": getattr(record, "run_id", "-"),
        }
        for key, value in record.__dict__.items():
            if key not in self._RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure_logging(level: str = "INFO") -> None:
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "filters": {"correlation": {"()": CorrelationFilter}},
            "formatters": {"json": {"()": JsonFormatter}},
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "json",
                    "filters": ["correlation"],
                }
            },
            "root": {"handlers": ["console"], "level": level},
            "loggers": {
                "uvicorn.access": {"handlers": ["console"], "level": "WARNING"},
                "uvicorn.error": {"handlers": ["console"], "level": level},
            },
        }
    )
