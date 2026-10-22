"""Persist and finalize activity trajectories for owner chat turns."""

from __future__ import annotations

import logging
from typing import Any

from ..runtime import (
    activity_events_for_target,
    chat_activity_tool_failed,
    chat_activity_waiting_state,
    chat_activity_requested_status,
)

logger = logging.getLogger(__name__)


def record_chat_activity_trajectories(
    activity_store,
    *,
    activity_ids: list[str],
    messages: list[dict[str, Any]],
    activities: list[dict[str, Any]],
    final_summary: str,
    turn_failed: bool = False,
) -> None:
    """Store each request's scoped evidence and close or wait it from tool results."""
    for activity_id in activity_ids:
        if not activity_store.activity_exists(activity_id):
            raise RuntimeError(
                f"Resolved Lisa chat activity {activity_id} does not exist while recording its trajectory"
            )
        runtime_events = activity_events_for_target(activities, activity_ids, activity_id)
        detail = activity_store.get_activity_thread(activity_id) or {}
        prior_activity = detail.get("activity") or {}
        prior_events = []
        for action in detail.get("actions") or []:
            if action.get("event_type") != "runtime.result":
                continue
            title = str(action.get("title") or "")
            suffix = " returned evidence"
            if not title.endswith(suffix):
                continue
            prior_events.append(
                {
                    "id": f"stored:{action.get('seq')}",
                    "kind": "evidence",
                    "tool_name": title[: -len(suffix)],
                    "detail": action.get("summary") or "",
                    "sequence": action.get("seq", 0),
                }
            )
        next_sequence = max(
            (int(item.get("sequence", 0)) for item in prior_events),
            default=0,
        )
        waiting, next_action = chat_activity_waiting_state(
            prior_events
            + [
                {**item, "sequence": next_sequence + index + 1}
                for index, item in enumerate(runtime_events)
            ]
        )
        requested_status = chat_activity_requested_status(runtime_events)
        if requested_status is not None:
            # An explicit completion request is the owner's chosen outcome for
            # this activity, even when its earlier trajectory recorded a
            # pending action that has since been handled elsewhere.
            waiting = False
            next_action = None
        prior_status = prior_activity.get("status")
        if (
            not waiting
            and prior_status in {"blocked", "waiting", "waiting_approval"}
            and (turn_failed or chat_activity_tool_failed(runtime_events))
        ):
            # A failed continuation cannot resolve the action that left this
            # activity waiting. Keep its persisted next action visible.
            waiting = True
            next_action = prior_activity.get("next_action") or "Continue the requested work"
        summary = (
            final_summary
            or "Lisa finished this request; see the timeline for recorded tool results."
        )
        status = (
            "waiting"
            if waiting
            else "failed"
            if turn_failed or chat_activity_tool_failed(runtime_events)
            else requested_status or "succeeded"
        )
        try:
            activity_store.record_chat_turn_trajectory(
                activity_id,
                messages,
                runtime_events,
                summary=summary,
                status=status,
                next_action=next_action or "Continue the requested work" if waiting else None,
            )
        except Exception:
            logger.exception("Could not persist and finalize Lisa chat activity %s", activity_id)
            raise
