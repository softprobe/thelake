from __future__ import annotations

import re
from typing import Any

_SECRET_FIELD_NAMES = (
    r"api[_-]?key|authorization|credential|password|private[_-]?key|secret|"
    r"access[_-]?token|refresh[_-]?token|token"
)
_SECRET_KEY = re.compile(rf"(?i)(?:^|[_-])(?:{_SECRET_FIELD_NAMES})(?:$|[_-])")
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)(?P<key_quote>[\"']?)(?P<key>[a-z][a-z0-9_-]*)(?P=key_quote)"
    r"(?P<separator>\s*[:=]\s*)(?![\"']?[a-z][a-z0-9_-]*[\"']?\s*[:=])"
    r"(?P<value>\"[^\"\n]*\"|'[^'\n]*'|Bearer\s+[^\s,;}\]]+|[^\s,;}\]\"']+)"
)
_SECRET_PATTERNS = (
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/-]+=*"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{16,}\b"),
    re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{16,}\b"),
    re.compile(r"\bAIza[0-9A-Za-z_-]{20,}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
)


def _is_secret_key(key: str) -> bool:
    separated = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key)
    return bool(_SECRET_KEY.search(separated))


def redact_evidence(value: Any) -> Any:
    """Mask credentials in telemetry before evidence reaches the model."""
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if _is_secret_key(str(key)) else redact_evidence(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_evidence(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


def redact_text(value: str) -> str:
    def redact_assignment(match: re.Match[str]) -> str:
        key_quote = match.group("key_quote")
        prefix = f"{key_quote}{match.group('key')}{key_quote}{match.group('separator')}"
        if not _is_secret_key(match.group("key")):
            return prefix + redact_text(match.group("value"))
        secret_value = match.group("value")
        if secret_value.startswith(('"', "'")) and secret_value.endswith(secret_value[0]):
            secret_value = f"{secret_value[0]}[REDACTED]{secret_value[0]}"
        else:
            secret_value = "[REDACTED]"
        return prefix + secret_value

    redacted = _SECRET_ASSIGNMENT.sub(redact_assignment, value)
    for pattern in _SECRET_PATTERNS:
        redacted = pattern.sub("[REDACTED]", redacted)
    return redacted


