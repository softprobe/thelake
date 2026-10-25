"""Optional pull-based sampling of complete sessions from TheLake."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import tempfile
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx
import psycopg

logger = logging.getLogger(__name__)
_PAGE_SIZE = 200
_MAX_TRACE_SPANS = 5_000
_MAX_SEARCHED_SESSIONS = 5_000
_QUIET_WINDOW = timedelta(minutes=10)


@dataclass(frozen=True)
class TheLakeConfiguration:
    base_url: str
    bearer_token: str

    @classmethod
    def from_values(cls, base_url: str, bearer_token: str) -> TheLakeConfiguration | None:
        url = base_url.strip()
        token = bearer_token.strip()
        if not url and not token:
            return None
        if not url or not token:
            raise ValueError("TheLake URL and bearer token must be configured together")
        if not url.startswith(("http://", "https://")):
            raise ValueError("TheLake URL must use HTTP or HTTPS")
        return cls(url.rstrip("/"), token)

    @classmethod
    def from_environment(cls) -> TheLakeConfiguration | None:
        import os

        return cls.from_values(
            os.getenv("LISA_THELAKE_URL", ""),
            os.getenv("LISA_THELAKE_BEARER_TOKEN", ""),
        )


class TheLakeApiError(RuntimeError):
    """A bounded TheLake read failed; its response body is intentionally hidden."""


class TheLakeClient:
    def __init__(self, configuration: TheLakeConfiguration, client: httpx.Client | None = None):
        self._client = client or httpx.Client(
            base_url=configuration.base_url,
            headers={"Authorization": f"Bearer {configuration.bearer_token}"},
            timeout=httpx.Timeout(20.0, connect=5.0),
        )

    def close(self) -> None:
        self._client.close()

    def search_sessions(
        self, *, from_time: str, to_time: str, cursor: str | None
    ) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/sessions/search",
            json={
                "from": from_time,
                "to": to_time,
                "roots_only": True,
                "order_by": "start_time",
                "order": "desc",
                "limit": _PAGE_SIZE,
                "cursor": cursor,
            },
        )

    def get_complete_session(self, summary: dict[str, Any]) -> dict[str, Any]:
        session_id = str(summary["session_id"])
        from_time = _parse_time(summary["start_time"])
        to_time = _parse_time(summary["end_time"]) + timedelta(microseconds=1)
        if to_time <= from_time:
            to_time = from_time + timedelta(microseconds=1)
        lower = from_time.isoformat().replace("+00:00", "Z")
        upper = to_time.isoformat().replace("+00:00", "Z")

        span_items: list[dict[str, Any]] = []
        cursor = None
        seen_cursors = set()
        while True:
            page = self._request(
                "POST",
                "/v1/spans/search",
                json={
                    "from": lower,
                    "to": upper,
                    "session_id": session_id,
                    "limit": _PAGE_SIZE,
                    "cursor": cursor,
                },
            )
            span_items.extend(page.get("items", []))
            if len(span_items) > _MAX_TRACE_SPANS:
                raise ValueError("TheLake session exceeds Lisa's bounded sampling limit")
            cursor = page.get("next_cursor")
            if not cursor:
                break
            if cursor in seen_cursors:
                raise TheLakeApiError("TheLake span search returned a repeated cursor")
            seen_cursors.add(cursor)

        trace_ids = sorted({str(item.get("trace_id", "")) for item in span_items} - {""})
        trace_details = []
        session_detail_span_count = 0
        for trace_id in trace_ids:
            trace_spans: list[dict[str, Any]] = []
            cursor = None
            seen_cursors = set()
            while True:
                page = self._request(
                    "GET",
                    f"/v1/traces/{trace_id}",
                    params={
                        "from": lower,
                        "to": upper,
                        "session_id": session_id,
                        "limit": _PAGE_SIZE,
                        "cursor": cursor,
                    },
                )
                trace_spans.extend(page.get("spans", []))
                session_detail_span_count += len(page.get("spans", []))
                if session_detail_span_count > _MAX_TRACE_SPANS:
                    raise ValueError("TheLake session exceeds Lisa's bounded sampling limit")
                if len(trace_spans) > _MAX_TRACE_SPANS:
                    raise ValueError("TheLake session exceeds Lisa's bounded sampling limit")
                cursor = page.get("next_span_cursor")
                if not cursor:
                    trace_details.append({"trace": page.get("trace", {}), "spans": trace_spans})
                    break
                if cursor in seen_cursors:
                    raise TheLakeApiError("TheLake trace returned a repeated cursor")
                seen_cursors.add(cursor)

        actual_spans = sum(len(trace.get("spans", [])) for trace in trace_details)
        expected_spans = int(summary.get("span_count") or 0)
        if actual_spans != expected_spans or len(trace_details) != int(summary.get("trace_count") or 0):
            raise ValueError("TheLake returned incomplete session evidence")
        if actual_spans != len(span_items):
            raise ValueError("TheLake span search and trace details disagree")
        return {"traces": trace_details, "span_count": actual_spans}

    def _request(self, method: str, path: str, **kwargs) -> dict[str, Any]:
        try:
            response = self._client.request(method, path, **kwargs)
            response.raise_for_status()
            value = response.json()
        except (httpx.HTTPError, ValueError) as error:
            raise TheLakeApiError(f"TheLake {method} {path} request failed") from error
        if not isinstance(value, dict):
            raise TheLakeApiError(f"TheLake {method} {path} returned an invalid response")
        return value


class TheLakeSessionSampler:
    def __init__(
        self,
        lake: Any,
        intake: Any,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        self.lake = lake
        self.intake = intake
        self.now = now

    def sample_recent_sessions(self, *, lookback_hours: int = 24, max_sessions: int = 5) -> dict[str, int]:
        if not 1 <= lookback_hours <= 168:
            raise ValueError("lookback_hours must be between 1 and 168")
        if not 1 <= max_sessions <= 5:
            raise ValueError("max_sessions must be between 1 and 5")
        now = self.now().astimezone(UTC)
        from_time = now - timedelta(hours=lookback_hours)
        start, end = _time_text(from_time), _time_text(now)
        summaries = self._search_all(start, end)
        result = {
            "scanned_count": len(summaries),
            "eligible_count": 0,
            "already_stored_count": 0,
            "selected_count": 0,
            "queued_count": 0,
            "already_queued_count": 0,
            "incomplete_count": 0,
            "failed_count": 0,
        }
        candidates = []
        for summary in summaries:
            if not _eligible(summary, from_time, now):
                continue
            result["eligible_count"] += 1
            candidates.append(summary)

        sample_key = str(int(now.timestamp() // 3600))
        selected = _stratified_sample(candidates, max_sessions, sample_key=sample_key)
        result["selected_count"] = len(selected)
        for summary in selected:
            try:
                if self.intake.has_session(str(summary["session_id"])):
                    result["already_stored_count"] += 1
                    continue
            except Exception as error:  # noqa: BLE001 - one bad local record must not stop sampling.
                result["failed_count"] += 1
                logger.warning(
                    "Could not check one sampled session receipt (%s)", type(error).__name__
                )
                continue
            try:
                complete = self.lake.get_complete_session(summary)
                expected = int(summary.get("span_count") or 0)
                if complete.get("span_count") != expected:
                    raise ValueError("TheLake returned incomplete session evidence")
                bundle = session_to_otlp(
                    str(summary["session_id"]), str(summary["agent_name"]), complete
                )
                receipt = self.intake.accept(bundle)
                if receipt["status"] == "queued":
                    result["queued_count"] += 1
                else:
                    result["already_queued_count"] += 1
            except ValueError as error:
                if "incomplete" in str(error).lower() or "bounded" in str(error).lower():
                    result["incomplete_count"] += 1
                else:
                    result["failed_count"] += 1
                logger.info("Skipping TheLake session evidence (%s)", type(error).__name__)
            except Exception as error:  # noqa: BLE001 - one bad session must not stop the sample.
                result["failed_count"] += 1
                logger.warning("Could not queue one sampled TheLake session (%s)", type(error).__name__)
        return result

    def _search_all(self, from_time: str, to_time: str) -> list[dict[str, Any]]:
        summaries = []
        cursor = None
        seen_cursors = set()
        while True:
            page = self.lake.search_sessions(
                from_time=from_time, to_time=to_time, cursor=cursor
            )
            summaries.extend(page.get("items", []))
            if len(summaries) > _MAX_SEARCHED_SESSIONS:
                raise TheLakeApiError("TheLake session search exceeded Lisa's sampling limit")
            cursor = page.get("next_cursor")
            if not cursor:
                return summaries
            if cursor in seen_cursors:
                raise TheLakeApiError("TheLake session search returned a repeated cursor")
            seen_cursors.add(cursor)


def session_to_otlp(session_id: str, agent_name: str, complete: dict[str, Any]) -> dict[str, Any]:
    traces = complete.get("traces")
    if traces is None:
        traces = [complete]
    by_scope: dict[str, list[dict[str, Any]]] = defaultdict(list)
    span_count = 0
    for trace_detail in traces:
        for item in trace_detail.get("spans", []):
            summary = item.get("summary", item)
            span_session = summary.get("session_id") or summary.get("sessionId") or session_id
            if str(span_session) != session_id:
                raise ValueError("TheLake trace contains a span from a different session")
            trace_id = str(summary.get("trace_id") or summary.get("traceId") or "")
            span_id = str(summary.get("span_id") or summary.get("spanId") or "")
            start = _to_unix_nanos(summary.get("start_time") or summary.get("startTime"))
            end = _to_unix_nanos(summary.get("end_time") or summary.get("endTime"))
            attrs = dict(item.get("attributes") or {})
            attrs.setdefault("sp.session.id", session_id)
            attrs.setdefault("sp.agent.name", agent_name)
            span = {
                "traceId": trace_id,
                "spanId": span_id,
                "name": str(summary.get("name") or "span"),
                "kind": _span_kind(summary.get("span_kind") or summary.get("span_type")),
                "startTimeUnixNano": str(start),
                "attributes": [_otlp_attribute(key, value) for key, value in sorted(attrs.items())],
                "events": [_otlp_event(event) for event in item.get("events", [])],
                "status": _otlp_status(summary.get("status_code")),
            }
            parent_id = summary.get("parent_span_id") or summary.get("parentSpanId")
            if parent_id:
                span["parentSpanId"] = str(parent_id)
            if end is not None:
                span["endTimeUnixNano"] = str(end)
            by_scope[str(summary.get("span_type") or "span")].append(span)
            span_count += 1
    if not by_scope:
        raise ValueError("TheLake returned incomplete session evidence")
    if complete.get("span_count") is not None and span_count != int(complete["span_count"]):
        raise ValueError("TheLake returned incomplete session evidence")
    resource_attributes = [
        _otlp_attribute("sp.agent.name", agent_name),
        _otlp_attribute("service.name", agent_name),
    ]
    return {
        "resourceSpans": [
            {
                "resource": {"attributes": resource_attributes},
                "scopeSpans": [
                    {"scope": {"name": "thelake"}, "spans": spans}
                    for _, spans in sorted(by_scope.items())
                ],
            }
        ]
    }


def _eligible(summary: dict[str, Any], from_time: datetime, now: datetime) -> bool:
    try:
        session_id = str(summary["session_id"])
        agent_name = str(summary["agent_name"] or "").strip()
        start = _parse_time(summary["start_time"])
        end = _parse_time(summary["end_time"])
        span_count = int(summary["span_count"])
    except (KeyError, TypeError, ValueError):
        return False
    return bool(
        session_id
        and agent_name
        and span_count > 0
        and from_time <= start <= now
        and end <= now - _QUIET_WINDOW
    )


def _stratified_sample(
    summaries: list[dict[str, Any]], limit: int, *, sample_key: str = ""
) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, bool], list[dict[str, Any]]] = defaultdict(list)
    for summary in summaries:
        key = (str(summary["agent_name"]), int(summary.get("error_count") or 0) > 0)
        buckets[key].append(summary)
    for values in buckets.values():
        values.sort(
            key=lambda item: hashlib.sha256(
                f"{sample_key}:{item['session_id']}".encode()
            ).digest()
        )
    keys = sorted(
        buckets,
        key=lambda key: hashlib.sha256(f"{sample_key}:{key[0]}:{key[1]}".encode()).digest(),
    )
    selected = []
    while len(selected) < limit:
        advanced = False
        for key in keys:
            if buckets[key]:
                selected.append(buckets[key].pop(0))
                advanced = True
                if len(selected) >= limit:
                    break
        if not advanced:
            break
    return selected


def _parse_time(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        raise ValueError("TheLake session timestamps must include a timezone")
    return parsed.astimezone(UTC)


def _time_text(value: datetime) -> str:
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _to_unix_nanos(value: Any) -> int | None:
    if value is None:
        return None
    parsed = _parse_time(value)
    return int(parsed.timestamp() * 1_000_000_000)


def _otlp_attribute(key: str, value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        encoded = {"boolValue": value}
    elif isinstance(value, int):
        encoded = {"intValue": str(value)}
    elif isinstance(value, float):
        encoded = {"doubleValue": value}
    elif isinstance(value, list):
        encoded = {"arrayValue": {"values": [_any_value(item) for item in value]}}
    elif isinstance(value, dict):
        encoded = {"kvlistValue": {"values": [_otlp_attribute(k, v) for k, v in sorted(value.items())]}}
    else:
        encoded = {"stringValue": str(value)}
    return {"key": key, "value": encoded}


def _any_value(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, dict):
        return {"kvlistValue": {"values": [_otlp_attribute(k, v) for k, v in sorted(value.items())]}}
    if isinstance(value, list):
        return {"arrayValue": {"values": [_any_value(item) for item in value]}}
    return {"stringValue": str(value)}


def _otlp_event(event: dict[str, Any]) -> dict[str, Any]:
    converted = {key: value for key, value in event.items() if key in {"name", "timeUnixNano"}}
    timestamp = event.get("time_unix_nano") or event.get("timestamp")
    if "timeUnixNano" not in converted and timestamp is not None:
        if isinstance(timestamp, int):
            converted["timeUnixNano"] = str(timestamp)
        else:
            converted["timeUnixNano"] = str(_to_unix_nanos(timestamp) or 0)
    attributes = event.get("attributes")
    if isinstance(attributes, dict):
        converted["attributes"] = [_otlp_attribute(k, v) for k, v in sorted(attributes.items())]
    elif isinstance(attributes, list):
        converted["attributes"] = attributes
    else:
        converted["attributes"] = []
    return converted


def _otlp_status(code: Any) -> dict[str, Any]:
    text = str(code or "").upper()
    value = 2 if text == "ERROR" else (1 if text == "OK" else 0)
    return {"code": value}


def _span_kind(value: Any) -> int:
    kinds = {"INTERNAL": 1, "SERVER": 2, "CLIENT": 3, "PRODUCER": 4, "CONSUMER": 5}
    return kinds.get(str(value or "").upper(), 0)


class TheLakeSamplingWorker:
    """Wake Lisa's packaged QA skill on one replica at a time."""

    def __init__(self, context, interval_seconds: int, run_turn: Callable):
        self.context = context
        self.interval_seconds = interval_seconds
        self.run_turn = run_turn
        lock_bytes = hashlib.sha256(
            f"lisa:thelake-sampling:{context.workspace_id()}".encode()
        ).digest()[:8]
        self.lock_id = int.from_bytes(lock_bytes, "big", signed=True)
        self.conversation_id = str(
            uuid5(NAMESPACE_URL, f"lisa:thelake-sampling:{context.workspace_id()}")
        )

    async def run_forever(self) -> None:
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - keep the service alive after one failed wakeup.
                logger.warning("Lisa TheLake sampling wakeup failed (%s)", type(error).__name__)
            await asyncio.sleep(self.interval_seconds)

    async def run_once(self) -> bool:
        if self.interval_seconds == 0:
            return False
        connection = await asyncio.to_thread(self._acquire_lock)
        if connection is None:
            return False
        try:
            with tempfile.TemporaryDirectory(prefix="lisa-sampler-") as directory:
                turn = await asyncio.to_thread(
                    self.run_turn,
                    conversation_id=self.conversation_id,
                    directory=str(Path(directory)),
                    workspace_id=self.context.workspace_id(),
                    text=(
                        "This is Lisa's scheduled QA sampling wakeup. Invoke the "
                        "sample-recent-sessions skill, load the TheLake tools, and call "
                        "sample_recent_sessions with its defaults. Do not create an "
                        "activity or inspect individual session evidence in this turn. "
                        "Return only a concise count summary."
                    ),
                    turn_source="scheduler",
                    chat_turn_id=str(uuid4()),
                )
                if getattr(turn, "failed", False):
                    raise RuntimeError("Lisa scheduled sampling turn did not complete")
            return True
        finally:
            await asyncio.to_thread(self._release_lock, connection)

    def _acquire_lock(self):
        connection = psycopg.connect(self.context.database_url, autocommit=True)
        try:
            acquired = connection.execute(
                "SELECT pg_try_advisory_lock(%s)", (self.lock_id,)
            ).fetchone()[0]
        except Exception:
            connection.close()
            raise
        if not acquired:
            connection.close()
            return None
        return connection

    def _release_lock(self, connection) -> None:
        try:
            connection.execute("SELECT pg_advisory_unlock(%s)", (self.lock_id,))
        finally:
            connection.close()
