"""JSON-lines logging on stderr, with the cycle and run ids taken from context variables."""

import json
import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

cycle_id_var: ContextVar[int | None] = ContextVar("cycle_id", default=None)
run_id_var: ContextVar[str | None] = ContextVar("run_id", default=None)

_STANDARD = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """One JSON object per line: UTC timestamp, level, logger, message, ids and extras."""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds")
        payload: dict[str, Any] = {
            "ts": ts.replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if (cycle_id := cycle_id_var.get()) is not None:
            payload["cycle_id"] = cycle_id
        if (run_id := run_id_var.get()) is not None:
            payload["run_id"] = run_id
        payload.update({k: v for k, v in record.__dict__.items() if k not in _STANDARD})
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    """Send every log record to stderr as JSON lines."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
