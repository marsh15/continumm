"""Structured standard-library logging with defense-in-depth redaction."""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime
from typing import Any

_REDACTION_PATTERNS = (
    re.compile(
        r"(?i)(password|passwd|secret|token|api[_-]?key|authorization)"
        r"(\s*[=:]\s*)(?:bearer\s+)?(?:\"[^\"]*\"|'[^']*'|[^\s,;&]+)"
    ),
    re.compile(r"(?i)((?:postgres(?:ql)?|mysql|redis)://[^:/\s]+:)[^@\s]+(@)"),
    re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+"),
    re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{16,}\b"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"(?i)([?&](?:key|token|password|secret|access_token|api_key)=)[^&#\s]+"),
)


def redact_secrets(value: str) -> str:
    """Mask common credentials before text reaches a log sink."""
    redacted = value
    for pattern in _REDACTION_PATTERNS:
        if pattern.groups == 2:
            redacted = pattern.sub(r"\1[REDACTED]\2", redacted)
        elif pattern.groups == 1:
            redacted = pattern.sub(r"\1[REDACTED]", redacted)
        else:
            redacted = pattern.sub("[REDACTED]", redacted)
    return redacted


class JsonFormatter(logging.Formatter):
    """Render one redacted JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_secrets(record.getMessage()),
        }
        request_id = getattr(record, "request_id", None)
        if request_id is not None:
            payload["request_id"] = str(request_id)
        method = getattr(record, "method", None)
        if method is not None:
            payload["method"] = redact_secrets(str(method))
        status = getattr(record, "status", None)
        if isinstance(status, int):
            payload["status"] = status
        if record.exc_info:
            payload["exception"] = redact_secrets(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def configure_logging(level: str) -> None:
    """Install the JSON formatter on the process root logger."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
