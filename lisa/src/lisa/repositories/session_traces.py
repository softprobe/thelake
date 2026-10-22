"""Durable, immutable OTLP session evidence in Lisa's agent workspace."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..storage import file_lock


@dataclass(frozen=True)
class StoredSessionTrace:
    session_id: str
    agent_name: str
    path: Path
    payload_hash: str
    created: bool


class SessionTraceConflict(ValueError):
    """A session ID was already bound to different immutable OTLP evidence."""


class SessionTraceStore:
    def __init__(self, workspace_root: Path):
        configured_root = Path(workspace_root).expanduser()
        if configured_root.is_symlink():
            raise ValueError("Agent workspace root cannot be a symlink")
        self.workspace_root = configured_root.resolve()

    def persist(self, bundle: dict[str, Any]) -> StoredSessionTrace:
        spans = _normalize_otlp_bundle(bundle)
        session_ids = {_session_id(span) for span in spans}
        agent_names = {_agent_name(span) for span in spans}
        if len(session_ids) != 1:
            raise ValueError("All spans in a pushed bundle must resolve to the same session ID")
        if len(agent_names) != 1:
            raise ValueError("All spans in a pushed bundle must resolve to the same agent")
        session_id = next(iter(session_ids))
        agent_name = next(iter(agent_names))
        if not session_id or not agent_name:
            raise ValueError("Session ID and agent name are required")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}", session_id):
            raise ValueError("Session ID contains characters unsafe for a filesystem path")

        canonical = json.dumps(bundle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        payload_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        root = self._trace_root()
        target = root / session_id
        lock_key = hashlib.sha256(session_id.encode("utf-8")).hexdigest()
        with file_lock(root / ".locks" / f"{lock_key}.lock"):
            self._check_path(root, target)
            if target.exists():
                manifest = self._read_manifest(target)
                if manifest.get("session_id") != session_id:
                    raise ValueError("Session trace path is bound to a different session ID")
                if manifest.get("payload_hash") != payload_hash:
                    raise SessionTraceConflict(
                        "Session ID already has different immutable OTLP content"
                    )
                return StoredSessionTrace(session_id, agent_name, target, payload_hash, False)

            root.mkdir(parents=True, exist_ok=True)
            temporary = Path(tempfile.mkdtemp(prefix=f".{lock_key}.", dir=root))
            try:
                (temporary / "otlp.json").write_text(canonical, encoding="utf-8")
                manifest = {
                    "session_id": session_id,
                    "agent_name": agent_name,
                    "payload_hash": payload_hash,
                    "received_at": datetime.now(UTC).isoformat(),
                    "format": "otlp-json",
                    "summary": _summary(spans),
                }
                (temporary / "manifest.json").write_text(
                    json.dumps(manifest, ensure_ascii=False, sort_keys=True), encoding="utf-8"
                )
                _fsync_file(temporary / "otlp.json")
                _fsync_file(temporary / "manifest.json")
                os.replace(temporary, target)
                _fsync_directory(root)
            finally:
                if temporary.exists():
                    for child in temporary.iterdir():
                        child.unlink(missing_ok=True)
                    temporary.rmdir()
        return StoredSessionTrace(session_id, agent_name, target, payload_hash, True)

    def load(self, session_id: str) -> dict[str, Any]:
        root = self._trace_root()
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}", session_id):
            raise FileNotFoundError(session_id)
        target = root / session_id
        self._check_path(root, target)
        manifest = self._read_manifest(target)
        if manifest.get("session_id") != session_id:
            raise FileNotFoundError(session_id)
        bundle = json.loads((target / "otlp.json").read_text(encoding="utf-8"))
        canonical = json.dumps(bundle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if hashlib.sha256(canonical.encode("utf-8")).hexdigest() != manifest.get("payload_hash"):
            raise ValueError("Stored session trace failed its content integrity check")
        spans = _normalize_otlp_bundle(bundle)
        return {
            "session_id": session_id,
            "agent_names": [manifest["agent_name"]],
            "payload_hash": manifest["payload_hash"],
            "trace_ids": list(dict.fromkeys(span["trace_id"] for span in spans)),
            "span_count": len(spans),
            "spans": spans,
            "evidence_limited": False,
            "received_at": manifest["received_at"],
        }

    def list_manifests(self) -> list[dict[str, Any]]:
        root = self._trace_root()
        if not root.exists():
            return []
        results = []
        for directory in root.iterdir():
            if directory.name == ".locks":
                continue
            if directory.is_symlink() or not directory.is_dir():
                continue
            results.append(self._read_manifest(directory))
        return sorted(results, key=lambda item: item.get("received_at", ""), reverse=True)

    def _trace_root(self) -> Path:
        root = self.workspace_root / "session_traces"
        if root.is_symlink() or not root.resolve().is_relative_to(self.workspace_root):
            raise ValueError("Session trace directory escapes the agent workspace")
        return root

    @staticmethod
    def _check_path(root: Path, target: Path) -> None:
        if root.is_symlink() or target.is_symlink():
            raise ValueError("Session trace paths cannot be symlinks")
        if not target.resolve().is_relative_to(root.resolve()):
            raise ValueError("Session trace path escapes its root")

    @staticmethod
    def _read_manifest(directory: Path) -> dict[str, Any]:
        manifest_path = directory / "manifest.json"
        if manifest_path.is_symlink() or not manifest_path.is_file():
            raise ValueError("Session trace manifest is missing or unsafe")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict):
            raise ValueError("Session trace manifest has an invalid shape")
        return manifest


def _normalize_otlp_bundle(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(bundle, dict):
        raise ValueError("OTLP request body must be an object")
    resource_spans = bundle.get("resourceSpans")
    if not isinstance(resource_spans, list):
        raise ValueError("OTLP bundle must contain resourceSpans")
    spans: list[dict[str, Any]] = []
    for resource_span in resource_spans:
        if not isinstance(resource_span, dict):
            raise ValueError("Every resourceSpans entry must be an object")
        raw_resource = resource_span.get("resource")
        if not isinstance(raw_resource, dict):
            raise ValueError("Every resourceSpans entry must contain a resource object")
        resource = _attributes(raw_resource.get("attributes", []))
        scope_spans = resource_span.get("scopeSpans")
        if not isinstance(scope_spans, list):
            raise ValueError("Every resourceSpans entry must contain scopeSpans")
        for scope_span in scope_spans:
            if not isinstance(scope_span, dict):
                raise ValueError("Every scopeSpans entry must be an object")
            raw_spans = scope_span.get("spans")
            if not isinstance(raw_spans, list):
                raise ValueError("Every scopeSpans entry must contain spans")
            for raw in raw_spans:
                if not isinstance(raw, dict):
                    raise ValueError("Every span must be an object")
                attrs = _attributes(raw.get("attributes", []))
                span = {
                    "trace_id": str(raw.get("traceId", "")),
                    "span_id": str(raw.get("spanId", "")),
                    "parent_span_id": str(raw.get("parentSpanId", "")) or None,
                    "name": str(raw.get("name", "")),
                    "start_time": _timestamp(raw.get("startTimeUnixNano")),
                    "end_time": _timestamp(raw.get("endTimeUnixNano")),
                    "attributes": attrs,
                    "resource_attributes": resource,
                    "status_code": _status(raw.get("status")),
                }
                if not re.fullmatch(r"[0-9a-fA-F]{32}", span["trace_id"]):
                    raise ValueError("Every OTLP span must include a 16-byte traceId")
                if not re.fullmatch(r"[0-9a-fA-F]{16}", span["span_id"]):
                    raise ValueError("Every OTLP span must include an 8-byte spanId")
                spans.append(span)
    if not spans:
        raise ValueError("OTLP bundle must contain at least one span")
    return spans


def _attributes(items: Any) -> dict[str, Any]:
    if not isinstance(items, list):
        raise ValueError("OTLP attributes must be a list")
    if any(not isinstance(item, dict) or not isinstance(item.get("key"), str) for item in items):
        raise ValueError("Every OTLP attribute must have a string key")
    keys = [item["key"] for item in items]
    if len(keys) != len(set(keys)):
        raise ValueError("OTLP attributes must not contain duplicate keys")
    return {
        str(item["key"]): _any_value(item.get("value", {}))
        for item in items
        if isinstance(item, dict) and isinstance(item.get("key"), str)
    }


def _any_value(value: Any) -> Any:
    if not isinstance(value, dict):
        return value
    for key in ("stringValue", "intValue", "doubleValue", "boolValue", "bytesValue"):
        if key in value:
            return value[key]
    if "arrayValue" in value:
        array = value["arrayValue"]
        if not isinstance(array, dict) or not isinstance(array.get("values", []), list):
            raise ValueError("OTLP arrayValue must contain a values list")
        return [_any_value(item) for item in array.get("values", [])]
    if "kvlistValue" in value:
        kvlist = value["kvlistValue"]
        if not isinstance(kvlist, dict):
            raise ValueError("OTLP kvlistValue must be an object")
        return _attributes(kvlist.get("values", []))
    return value


def _summary(spans: list[dict[str, Any]]) -> dict[str, Any]:
    starts = [span["start_time"] for span in spans if span.get("start_time")]
    ends = [span["end_time"] for span in spans if span.get("end_time")]
    attributes = [span["attributes"] for span in spans]
    start = min(starts, default=None)
    end = max(ends, default=None)
    duration = None
    if start and end:
        duration = max(0, int((datetime.fromisoformat(end.replace("Z", "+00:00"))
                              - datetime.fromisoformat(start.replace("Z", "+00:00")))
                             .total_seconds() * 1000))
    return {
        "start_time": start,
        "end_time": end,
        "duration": duration,
        "trace_count": len({span["trace_id"] for span in spans}),
        "span_count": len(spans),
        "error_count": sum(span.get("status_code") == "ERROR" for span in spans),
        "root_count": sum(not span.get("parent_span_id") for span in spans),
        "user_ids": sorted({str(attrs["user.id"]) for attrs in attributes if attrs.get("user.id")}),
        "models": sorted({str(attrs["gen_ai.request.model"]) for attrs in attributes if attrs.get("gen_ai.request.model")}),
        "input_tokens": _sum_attributes(attributes, "gen_ai.usage.input_tokens"),
        "output_tokens": _sum_attributes(attributes, "gen_ai.usage.output_tokens"),
        "total_tokens": _sum_attributes(attributes, "gen_ai.usage.total_tokens"),
    }


def _sum_attributes(attributes: list[dict[str, Any]], key: str) -> int | None:
    values = [value for attrs in attributes if (value := attrs.get(key)) is not None]
    try:
        return sum(int(value) for value in values) if values else None
    except (TypeError, ValueError):
        return None


def _session_id(span: dict[str, Any]) -> str:
    attrs = span["attributes"]
    return str(
        attrs.get("sp.session.id")
        or attrs.get("gen_ai.conversation.id")
        or span["trace_id"]
    ).strip()


def _agent_name(span: dict[str, Any]) -> str:
    attrs = span["attributes"]
    resource = span["resource_attributes"]
    return str(
        attrs.get("sp.agent.name")
        or resource.get("sp.agent.name")
        or resource.get("service.name")
        or ""
    ).strip()


def _timestamp(value: Any) -> str | None:
    if value in (None, "", "0", 0):
        return None
    try:
        timestamp = datetime.fromtimestamp(int(value) / 1_000_000_000, UTC)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("OTLP span timestamps must be Unix nanoseconds") from None
    return timestamp.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _status(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    code = value.get("code")
    if code in (None, 0, "STATUS_CODE_UNSET"):
        return None
    return {1: "OK", 2: "ERROR", "STATUS_CODE_OK": "OK", "STATUS_CODE_ERROR": "ERROR"}.get(
        code, str(code)
    )


def _fsync_file(path: Path) -> None:
    with path.open("rb") as file:
        os.fsync(file.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
