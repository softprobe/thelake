"""Session search and trajectory use cases shared by tools and evaluation.

Canonical Lisa terms are in ``docs/architecture/lisa-terms.md``: the session ID
identifies the evaluation input; its trajectory is a projection, not a second
identified object.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from ..evaluation.trajectory import SESSION_SPAN_LIMIT, build_trajectory_events
from ..security import redact_evidence
from ..repositories.session_traces import SessionTraceStore


class SessionService:
    def __init__(self, traces: SessionTraceStore, build_events=build_trajectory_events):
        self.traces = traces
        self.build_events = build_events

    def search(
        self,
        *,
        from_time: str | None = None,
        to_time: str | None = None,
        has_errors: bool | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        agent_name: str | None = None,
        roots_only: bool = True,
        order_by: str = "start_time",
        order: str = "desc",
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        if cursor:
            raise ValueError("Session cursors are not supported by Lisa's local trace store")
        start = _parse_time(from_time) if from_time else datetime.now(UTC) - timedelta(days=7)
        end = _parse_time(to_time) if to_time else datetime.now(UTC)
        summaries = []
        for manifest in self.traces.list_manifests():
            summary = manifest["summary"]
            start_time = summary["start_time"] or manifest["received_at"]
            start_at = _parse_time(start_time)
            agent = str(manifest["agent_name"])
            errors = summary["error_count"]
            users = summary["user_ids"]
            models = summary["models"]
            if not start <= start_at <= end:
                continue
            if agent_name and agent_name != agent:
                continue
            if user_id and user_id not in users:
                continue
            if model_name and model_name not in models:
                continue
            if has_errors is not None and bool(errors) != has_errors:
                continue
            if roots_only and not summary["root_count"]:
                continue
            summaries.append(
                {
                    "session_id": manifest["session_id"],
                    "start_time": start_time,
                    "end_time": summary["end_time"],
                    "trace_count": summary["trace_count"],
                    "span_count": summary["span_count"],
                    "error_count": errors,
                    "input_tokens": summary["input_tokens"],
                    "output_tokens": summary["output_tokens"],
                    "total_tokens": summary["total_tokens"],
                    "duration": summary["duration"],
                    "total_cost": None,
                    "agent_name": agent,
                    "user_ids": users,
                    "models": models,
                }
            )
        if order_by not in {"start_time", "error_count", "duration", "total_tokens"}:
            raise ValueError("Unsupported session order_by value")
        if order not in {"asc", "desc"} or not 1 <= limit <= 500:
            raise ValueError("Invalid session search limit or order")
        summaries.sort(
            key=lambda item: item.get(order_by) or ("" if order_by == "start_time" else 0),
            reverse=order == "desc",
        )
        return {"items": summaries[:limit], "next_cursor": None, "cursor_supported": False}

    def detail(self, session_id: str) -> dict[str, Any]:
        detail = self.traces.load(session_id)
        if len(detail["spans"]) > SESSION_SPAN_LIMIT:
            detail["spans"] = detail["spans"][:SESSION_SPAN_LIMIT]
            detail["evidence_limited"] = True
        return redact_evidence(detail)

    def trajectory(self, session_detail: dict[str, Any]) -> dict[str, Any]:
        """Reduce raw session detail to the ordered evidence used for evaluation."""
        spans = session_detail.get("spans", [])
        events = self.build_events(spans)
        limitations = list(events.limitations)
        try:
            span_count = int(session_detail.get("span_count", len(spans)))
        except (TypeError, ValueError):
            span_count = len(spans)
        if span_count > len(spans):
            limitations.append("The session detail span limit was reached.")
        if session_detail.get("evidence_limited", False) or any(
            "[truncated]" in str(event.get("content", ""))
            or "[truncated]" in str(event.get("payload", ""))
            for event in events.events
        ):
            limitations.append("The session detail was bounded or truncated.")
        return {
            "session_id": session_detail.get("session_id", ""),
            "agent_names": session_detail.get("agent_names", []),
            "trace_ids": list(
                dict.fromkeys(str(span["trace_id"]) for span in spans if span.get("trace_id"))
            ),
            "events": events.events,
            "complete": not limitations,
            "tool_events_captured": events.tool_events_captured,
            "tool_order_certain": events.tool_order_certain,
            "message_order_certain": events.message_order_certain,
            "limitations": list(dict.fromkeys(limitations)),
        }

    def get_trajectory(self, session_id: str) -> dict[str, Any]:
        return self.trajectory(self.detail(session_id))


def _parse_time(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except (TypeError, ValueError) as error:
        raise ValueError("Session search timestamps must be ISO-8601") from error
