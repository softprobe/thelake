"""Session search and trajectory use cases shared by tools and evaluation.

Canonical Lisa terms are in ``docs/architecture/lisa-terms.md``: the TheLake
session ID identifies the evaluation input; its trajectory is a projection,
not a second identified object.
"""

from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

from ..evaluation.trajectory import SESSION_SPAN_LIMIT, build_trajectory_events
from ..security import redact_evidence
from ..thelake_client import TheLakeClient


class SessionService:
    def __init__(self, client: TheLakeClient, build_events=build_trajectory_events):
        self.client = client
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
        now = datetime.now(UTC)
        return self.client.post(
            "/v1/sessions/search",
            {
                "from": from_time or (now - timedelta(days=7)).isoformat(),
                "to": to_time or now.isoformat(),
                "has_errors": has_errors,
                "user_id": user_id,
                "model_name": model_name,
                "agent_name": agent_name,
                "roots_only": roots_only,
                "order_by": order_by,
                "order": order,
                "limit": limit,
                "cursor": cursor,
            },
        )

    def detail(self, session_id: str) -> dict[str, Any]:
        path = f"/v1/sessions/{quote(session_id, safe='')}?limit={SESSION_SPAN_LIMIT}"
        return redact_evidence(self.client.get(path))

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
