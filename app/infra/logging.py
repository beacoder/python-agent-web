"""Structured logging with a per-request correlation id.

One app logger tree (``paw.*``) writes to stderr; every record carries a
``request_id`` bound by the HTTP middleware (see ``main``), so a line
from deep in a controller can be traced back to the request that caused
it.  ``PAW_LOG_JSON=true`` switches to one JSON object per line for log
shippers; the default is a readable text line for dev.
"""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar, Token

# Bound per request by the middleware; "-" outside a request (startup,
# background threads that didn't inherit the context).
_request_id: ContextVar[str] = ContextVar("request_id", default="-")


def set_request_id(rid: str) -> Token[str]:
    return _request_id.set(rid)


def reset_request_id(token: Token[str]) -> None:
    _request_id.reset(token)


def current_request_id() -> str:
    return _request_id.get()


class _RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = _request_id.get()
        return True


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "request_id": getattr(record, "request_id", "-"),
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str = "INFO", json_logs: bool = False) -> None:
    """(Re)configure the ``paw`` logger tree. Idempotent per call."""
    logger = logging.getLogger("paw")
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stderr)
    handler.addFilter(_RequestIdFilter())
    if json_logs:
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-5s %(name)s [%(request_id)s] %(message)s",
                datefmt="%H:%M:%S",
            )
        )
    logger.addHandler(handler)
    logger.setLevel(level.upper())
    logger.propagate = False  # don't double-log through the root logger


def get_logger(name: str) -> logging.Logger:
    """A child logger under the configured ``paw`` tree."""
    return logging.getLogger(f"paw.{name}")
