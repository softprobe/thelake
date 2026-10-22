"""Lisa activity persistence using a deployment-bound PostgreSQL schema."""

from __future__ import annotations

import json
from hashlib import sha256
from importlib.resources import files
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row

from ..models.activities import (
    Activity,
    ActivityClaim,
    ActivityContext,
    ActivityEvent,
    EvaluationRun,
    ScoreOutboxItem,
)
from ..security import redact_text


class DeploymentBindingError(RuntimeError):
    """Raised when a Lisa database is bound to a different workspace."""


class StaleLeaseError(RuntimeError):
    """Raised when a worker attempts a write after its lease was superseded."""


def _workspace_fingerprint(workspace_id: str) -> str:
    normalized = str(UUID(workspace_id))
    return sha256(normalized.encode("utf-8")).hexdigest()


def _sql(name: str) -> str:
    return files("lisa").joinpath("sql", "activities", name).read_text(encoding="utf-8")


class PostgresActivityStore:
    """Typed lifecycle operations for activities; raw SQL stays in .sql files."""

    def __init__(self, dsn: str, workspace_id: str):
        self.dsn = dsn
        self.workspace_fingerprint = _workspace_fingerprint(workspace_id)
        with self._connect() as connection:
            binding = connection.execute(_sql("get_deployment.sql")).fetchone()
        if not binding or binding["workspace_fingerprint"] != self.workspace_fingerprint:
            raise DeploymentBindingError("Lisa database is not bound to the configured workspace")

    def _connect(self):
        return psycopg.connect(self.dsn, row_factory=dict_row)

    def create_activity(
        self, *, title: str, source: str, kind: str = "action", activity_id: str | None = None,
        context_path: str | None = None, activity_type: str | None = None,
        is_goal: bool = False, message_id: str | None = None, subtitle: str | None = None,
        task_label: str | None = None, agent_id: str | None = None,
        details: dict[str, Any] | None = None, parent_id: str | None = None,
        next_action: str | None = None, next_action_at: str | None = None,
        origin_conversation_id: str | None = None, slack_channel_id: str | None = None,
        slack_thread_ts: str | None = None, origin_slack_channel_id: str | None = None,
        origin_slack_thread_ts: str | None = None,
    ) -> Activity:
        if not kind.strip():
            raise ValueError("Activity kind must not be empty")
        activity_id = activity_id or str(uuid4())
        context_path = context_path or f"activities/{activity_id}"
        with self._connect() as connection, connection.transaction():
            return self._create_activity_on_connection(
                connection, title=title, source=source, kind=kind,
                activity_id=activity_id, context_path=context_path,
                activity_type=activity_type, is_goal=is_goal, message_id=message_id,
                subtitle=subtitle, task_label=task_label, agent_id=agent_id,
                details=details, parent_id=parent_id, next_action=next_action,
                next_action_at=next_action_at,
                origin_conversation_id=origin_conversation_id,
                slack_channel_id=slack_channel_id, slack_thread_ts=slack_thread_ts,
                origin_slack_channel_id=origin_slack_channel_id,
                origin_slack_thread_ts=origin_slack_thread_ts,
            )

    def _create_activity_on_connection(
        self, connection, *, title: str, source: str, kind: str = "action",
        activity_id: str, context_path: str | None = None,
        activity_type: str | None = None, is_goal: bool = False,
        message_id: str | None = None, subtitle: str | None = None,
        task_label: str | None = None, agent_id: str | None = None,
        details: dict[str, Any] | None = None, parent_id: str | None = None,
        next_action: str | None = None, next_action_at: str | None = None,
        origin_conversation_id: str | None = None, slack_channel_id: str | None = None,
        slack_thread_ts: str | None = None, origin_slack_channel_id: str | None = None,
        origin_slack_thread_ts: str | None = None,
    ) -> Activity:
        context_path = context_path or f"activities/{activity_id}"
        inserted = connection.execute(
            _sql("create_activity.sql"),
            {"id": activity_id, "kind": kind, "title": title,
             "source": source, "context_path": context_path,
             "parent_id": parent_id, "next_action": next_action,
             "next_action_at": next_action_at,
             "origin_conversation_id": origin_conversation_id,
             "origin_slack_channel_id": origin_slack_channel_id,
             "origin_slack_thread_ts": origin_slack_thread_ts,
             "slack_channel_id": slack_channel_id,
             "slack_thread_ts": slack_thread_ts},
        ).fetchone()
        if inserted is None:
            return Activity(id=activity_id, created=False, context_path=context_path)
        self._append_activity_event(connection, activity_id, "accepted", title, None, "lisa")
        self._create_activity_presentation(
            connection, activity_id, activity_type or kind, is_goal, message_id,
            subtitle, task_label, agent_id, details or {},
        )
        return Activity(id=activity_id, created=True, context_path=context_path)

    def resolve_chat_activity(
        self, *, turn_id: str, request_key: str, disposition: str,
        source: str, title: str = "", summary: str = "",
        activity_id: str | None = None, message_id: str | None = None,
        origin_conversation_id: str | None = None,
        origin_slack_channel_id: str | None = None,
        origin_slack_thread_ts: str | None = None,
    ) -> Activity:
        """Resolve one chat intent and make retries return its canonical activity."""
        normalized_turn = turn_id.strip()
        normalized_key = request_key.strip()
        if not normalized_turn or not normalized_key:
            raise ValueError("Chat turn ID and request key must not be empty")
        if disposition not in {"create", "continue", "fallback"}:
            raise ValueError("Chat activity disposition must be create, continue, or fallback")

        lock_key = f"{normalized_turn}:{normalized_key}"
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("lock_chat_activity_turn.sql"), {"lock_key": lock_key})
            existing = connection.execute(_sql("get_chat_activity_turn.sql"), {
                "turn_id": normalized_turn, "request_key": normalized_key,
            }).fetchone()
            if existing is not None:
                resolved_id = str(existing["activity_id"])
                connection.execute(_sql("activate_chat_activity_turn.sql"), {
                    "turn_id": normalized_turn, "request_key": normalized_key,
                })
                return Activity(id=resolved_id, created=False)

            if disposition in {"create", "fallback"}:
                if not title.strip():
                    raise ValueError("A new chat activity requires a title")
                safe_title = redact_text(title.strip())[:240]
                safe_summary = redact_text(summary.strip())[:12_000]
                resolved_id = str(uuid5(
                    NAMESPACE_URL, f"lisa:chat:{normalized_turn}:{normalized_key}"
                ))
                activity = self._create_activity_on_connection(
                    connection, title=safe_title, source=source, kind="chat_request",
                    activity_id=resolved_id, activity_type="chat_request", message_id=message_id,
                    subtitle=safe_summary[:500] or None,
                    details={"request_summary": safe_summary} if safe_summary else {},
                    origin_conversation_id=origin_conversation_id,
                    origin_slack_channel_id=origin_slack_channel_id,
                    origin_slack_thread_ts=origin_slack_thread_ts,
                )
            else:
                if not activity_id or not activity_id.strip():
                    raise ValueError("Continuing chat activity requires an activity ID")
                resolved_id = activity_id.strip()
                row = connection.execute(
                    _sql("get_activity_status.sql"), {"activity_id": resolved_id}
                ).fetchone()
                if row is None:
                    raise ValueError("Activity does not exist")
                if row["status"] not in {"queued", "running", "waiting", "waiting_approval"}:
                    raise ValueError("Closed activities cannot be continued; create a new activity")
                activity = Activity(id=resolved_id, created=False)

            self._record_chat_activity_turn(
                connection, turn_id=normalized_turn, request_key=normalized_key,
                activity_id=resolved_id, disposition=disposition,
            )
            connection.execute(_sql("activate_chat_activity_turn.sql"), {
                "turn_id": normalized_turn, "request_key": normalized_key,
            })
            return activity

    @staticmethod
    def _record_chat_activity_turn(
        connection, *, turn_id: str, request_key: str, activity_id: str,
        disposition: str,
    ) -> None:
        connection.execute(_sql("record_chat_activity_turn.sql"), {
            "turn_id": turn_id, "request_key": request_key,
            "activity_id": activity_id, "disposition": disposition,
        })

    def create_chat_activity(
        self, *, turn_id: str, request_key: str, title: str, summary: str,
        message_id: str | None = None, source: str,
        origin_conversation_id: str | None = None,
        origin_slack_channel_id: str | None = None,
        origin_slack_thread_ts: str | None = None,
    ) -> Activity:
        return self.resolve_chat_activity(
            turn_id=turn_id, request_key=request_key, disposition="fallback",
            title=title, summary=summary, message_id=message_id, source=source,
            origin_conversation_id=origin_conversation_id,
            origin_slack_channel_id=origin_slack_channel_id,
            origin_slack_thread_ts=origin_slack_thread_ts,
        )

    def chat_activities_for_turn(self, turn_id: str) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_chat_activities_for_turn.sql"), {"turn_id": turn_id}
            ).fetchall()
        return [str(row["activity_id"]) for row in rows]

    def active_chat_activity_for_turn(self, turn_id: str) -> str | None:
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_chat_activities_for_turn.sql"), {"turn_id": turn_id}
            ).fetchall()
        active = [row for row in rows if row["is_active"]]
        if len(active) > 1:
            raise RuntimeError("Chat turn has more than one active activity")
        return str(active[0]["activity_id"]) if active else None

    def fail_chat_turn(
        self, turn_id: str, user_text: str, error_text: str, *, source: str = "web",
        origin_conversation_id: str | None = None,
        origin_slack_channel_id: str | None = None,
        origin_slack_thread_ts: str | None = None,
    ) -> None:
        """Preserve a failed owner turn, including one that failed before resolution."""
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_chat_activities_for_turn.sql"), {"turn_id": turn_id}
            ).fetchall()
        if not rows:
            normalized_title = " ".join(user_text.split()).strip()[:120]
            self.create_chat_activity(
                turn_id=turn_id,
                request_key="failed-turn",
                title=normalized_title or "Lisa chat request",
                summary=user_text,
                source=source,
                origin_conversation_id=origin_conversation_id,
                origin_slack_channel_id=origin_slack_channel_id,
                origin_slack_thread_ts=origin_slack_thread_ts,
            )
            with self._connect() as connection:
                rows = connection.execute(
                    _sql("list_chat_activities_for_turn.sql"), {"turn_id": turn_id}
                ).fetchall()
            if not rows:
                raise RuntimeError(f"Could not create failed activity for chat turn {turn_id}")
        messages = [{
            "id": f"lisa-chat-user:{turn_id}", "role": "user",
            "text": user_text, "sequence": 1,
        }]
        error_event = {
            "id": f"lisa-chat-error:{turn_id}", "kind": "evidence",
            "tool_name": "chat_turn_error", "detail": redact_text(error_text),
            "sequence": 2,
        }
        for row in rows:
            activity_id = str(row["activity_id"])
            events = [error_event] if row["is_active"] else []
            if row["is_active"]:
                self.record_chat_turn_trajectory(
                    activity_id, messages, events,
                    summary="Chat turn failed after activity work started.", status="failed",
                )
            else:
                self.record_conversation_trajectory(activity_id, messages, events)

    def _create_activity_presentation(
        self, connection, activity_id, activity_type, is_goal, message_id,
        subtitle, task_label, agent_id, details,
    ) -> None:
        parameters = {
            "activity_id": activity_id, "activity_type": activity_type,
            "is_goal": is_goal, "message_id": message_id, "subtitle": subtitle,
            "task_label": task_label, "agent_id": agent_id,
            "status_title": None,
            "details": json.dumps(details),
        }
        connection.execute(_sql("create_activity_presentation.sql"), parameters)
        connection.execute(_sql("create_monitor_thread.sql"), parameters)
        connection.execute(_sql("create_agent_thread_link.sql"), parameters)
        connection.execute(_sql("create_message_thread_link.sql"), parameters)

    def list_activity_feed(
        self, *, limit: int = 50, is_goal: bool | None = None,
        activity_type: str | None = None, status: str | None = None,
        query: str | None = None, due_only: bool = False, open_only: bool = False,
    ) -> list[dict[str, Any]]:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        with self._connect() as connection:
            rows = connection.execute(_sql("list_activity_feed.sql"), {
                "limit": limit, "is_goal": is_goal, "activity_type": activity_type,
                "status": status, "query": query.strip() or None if query else None,
                "due_only": due_only, "open_only": open_only,
            }).fetchall()
        return [self._activity_feed_row(row) for row in rows]

    def get_activity_thread(self, activity_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            feed = connection.execute(_sql("get_activity_feed_entry.sql"), {
                "activity_id": activity_id,
            }).fetchone()
            if feed is None:
                return None
            row = connection.execute(_sql("get_activity_thread.sql"), {
                "activity_id": activity_id,
            }).fetchone()
            if row is None:
                return None
            actions = connection.execute(_sql("get_activity_thread_actions.sql"), {
                "activity_id": activity_id,
            }).fetchall()
            sections = connection.execute(_sql("get_activity_thread_sections.sql"), {
                "activity_id": activity_id,
            }).fetchall()
            references = connection.execute(_sql("get_activity_thread_links.sql"), {
                "activity_id": activity_id,
            }).fetchall()
        status = self._presentation_status(row["status"])
        thread = {"activity_thread_id": str(row["id"]), "name": row["title"],
                  "status": status, "icon": row["icon"],
                  "status_title": row["status_title"],
                  "details_json": row["thread_details"],
                  "finish_message": row["summary"] if row["finished_at"] else None}
        return {"activity": self._activity_feed_row(feed), "thread": thread,
                "actions": [{**dict(action), "status": self._presentation_status(action["status"])}
                            for action in actions],
                "sections": [{"activity_thread_id": str(section["activity_id"]),
                              "section_key": section["section_id"], "title": section["title"],
                              "status": section["status"], "section_order": section["section_order"],
                              "section_depth": section["section_depth"],
                              "parent_section_key": section["parent_section_key"],
                              "ordinal_label": section["ordinal_label"]}
                             for section in sections],
                "references": [dict(reference) for reference in references]}

    @staticmethod
    def _activity_feed_row(row) -> dict[str, Any]:
        result = dict(row)
        for key in ("id",):
            result[key] = str(result[key])
        result["activity_key"] = result["id"]
        if result.get("parent_id") is not None:
            result["parent_id"] = str(result["parent_id"])
        result["status"] = PostgresActivityStore._presentation_status(result["status"])
        result["details_json"] = result.pop("details_json") or {}
        return result

    @staticmethod
    def _presentation_status(status: str) -> str:
        return {"queued": "pending", "running": "pending", "waiting": "blocked",
                "waiting_approval": "blocked", "succeeded": "success",
                "failed": "error", "cancelled": "stopped"}.get(status, status)

    def update_activity_presentation(
        self, activity_id: str, *, title: str | None = None, subtitle: str | None = None,
        status_title: str | None = None, task_label: str | None = None,
        details: dict[str, Any] | None = None,
        event_type: str | None = None, event_title: str | None = None,
        event_summary: str | None = None,
    ) -> None:
        if (event_type is None) != (event_title is None):
            raise ValueError("event_type and event_title must be supplied together")
        with self._connect() as connection, connection.transaction():
            row = connection.execute(_sql("update_activity_presentation.sql"), {
                "activity_id": activity_id, "subtitle": subtitle,
                "status_title": status_title,
                "task_label": task_label,
                "details": json.dumps(details) if details is not None else None,
            }).rowcount
            if not row:
                raise ValueError("Activity does not exist")
            if title is not None:
                connection.execute(_sql("update_activity_title.sql"), {
                    "activity_id": activity_id, "title": title,
                })
            connection.execute(_sql("update_monitor_thread_details.sql"), {
                "activity_id": activity_id,
                "details": json.dumps(details) if details is not None else None,
            })
            if event_type is not None:
                self._append_activity_event(
                    connection, activity_id, event_type, event_title,
                    event_summary, "lisa",
                )

    def set_activity_section(
        self, activity_id: str, section_key: str, *, title: str = "",
        status: str = "pending", section_order: int = 0,
    ) -> None:
        if not section_key.strip():
            raise ValueError("section_key must not be empty")
        with self._connect() as connection, connection.transaction():
            row = connection.execute(_sql("set_activity_section.sql"), {
                "activity_id": activity_id, "section_id": section_key,
                "title": title, "status": status, "section_order": section_order,
            }).rowcount
            if not row:
                raise ValueError("Activity does not exist")

    def link_activity_reference(
        self, activity_id: str, reference_type: str, external_id: str,
    ) -> None:
        sql_by_type = {
            "agent": "link_agent_thread.sql",
            "message": "link_message_thread.sql",
            "runtime_work": "link_runtime_work_thread.sql",
            "runtime_event": "link_runtime_event_thread.sql",
            "carrier_message": "link_carrier_message_thread.sql",
        }
        statement = sql_by_type.get(reference_type)
        if statement is None:
            raise ValueError("Unsupported activity reference type")
        if not external_id.strip():
            raise ValueError("external_id must not be empty")
        with self._connect() as connection, connection.transaction():
            exists = connection.execute(_sql("activity_exists.sql"), {
                "activity_id": activity_id,
            }).fetchone()
            if not exists or not exists["found"]:
                raise ValueError("Activity does not exist")
            connection.execute(_sql(statement), {
                "activity_id": activity_id, "external_id": external_id,
            })

    def record_conversation_trajectory(
        self, activity_id: str, messages: list[dict[str, Any]],
        runtime_events: list[dict[str, Any]],
    ) -> int:
        """Append an idempotent, redacted chat and tool trace to an activity."""
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("lock_activity_for_update.sql"), {
                "activity_id": activity_id,
            })
            return self._record_conversation_trajectory(
                connection, activity_id, messages, runtime_events,
            )

    def record_chat_turn_trajectory(
        self, activity_id: str, messages: list[dict[str, Any]],
        runtime_events: list[dict[str, Any]], *, summary: str, status: str,
        next_action: str | None = None,
    ) -> int:
        """Persist a chat trajectory and its resulting lifecycle in one transaction."""
        if status not in {"waiting", "succeeded", "failed", "cancelled"}:
            raise ValueError("Unsupported chat turn lifecycle status")
        if status == "waiting" and not (next_action or "").strip():
            raise ValueError("A waiting chat activity requires its next action")
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("lock_activity_for_update.sql"), {
                "activity_id": activity_id,
            })
            recorded = self._record_conversation_trajectory(
                connection, activity_id, messages, runtime_events,
            )
            if status == "waiting":
                row = connection.execute(_sql("update_activity_progress.sql"), {
                    "activity_id": activity_id, "status": status,
                    "next_action": next_action, "next_action_at": None,
                }).fetchone()
                if row is None:
                    raise RuntimeError(f"Could not finalize chat activity {activity_id} as waiting")
                self._append_activity_event(
                    connection, activity_id, "progress", row["title"], summary, "lisa",
                )
            else:
                finished = connection.execute(_sql("complete_agent_activity.sql"), {
                    "activity_id": activity_id, "status": status,
                    "title": f"Activity {status}", "summary": summary,
                }).fetchone()
                if finished is None:
                    raise RuntimeError(f"Could not finalize chat activity {activity_id} as {status}")
            connection.execute(_sql("sync_activity_actions.sql"), {"activity_id": activity_id})
            return recorded

    def _record_conversation_trajectory(
        self, connection, activity_id: str, messages: list[dict[str, Any]],
        runtime_events: list[dict[str, Any]],
    ) -> int:
        recorded = 0
        timeline = [
            {**item, "timeline_kind": "message"} for item in messages
        ] + [
            {**item, "timeline_kind": "runtime"} for item in runtime_events
        ]
        timeline.sort(key=lambda item: item.get("sequence", 0))
        for item in timeline:
            item_id = str(item.get("id", "")).strip()
            if not item_id:
                continue
            reference_type = "message" if item["timeline_kind"] == "message" else "runtime_event"
            existing = connection.execute(_sql("find_activity_event_reference.sql"), {
                "activity_id": activity_id, "reference_type": reference_type,
                "reference_id": item_id,
            }).fetchone()
            if existing is not None:
                continue
            if item["timeline_kind"] == "message":
                role, text = item.get("role"), item.get("text")
                if role not in {"user", "assistant"} or not isinstance(text, str):
                    continue
                body = redact_text(text)[:12_000]
                if not body:
                    continue
                connection.execute(_sql("link_message_thread.sql"), {
                    "activity_id": activity_id, "external_id": item_id,
                })
                speaker = "You" if role == "user" else "Lisa"
                preview = " ".join(body.split())[:96]
                self._append_activity_event(
                    connection, activity_id,
                    "chat.user" if role == "user" else "chat.assistant",
                    f"{speaker}: {preview}" if role == "user" else "Lisa replied",
                    body, "user" if role == "user" else "lisa",
                    reference_type=reference_type, reference_id=item_id,
                )
                recorded += 1
                continue
            connection.execute(_sql("link_runtime_event_thread.sql"), {
                "activity_id": activity_id, "external_id": item_id,
            })
            name = str(item.get("tool_name", "tool"))[:120]
            if item.get("kind") == "tool":
                arguments = json.dumps(item.get("arguments", {}), ensure_ascii=False)
                summary = redact_text(arguments)[:12_000]
                event_type, title, actor = "runtime.tool", f"Lisa used {name}", "lisa"
            else:
                summary = redact_text(str(item.get("detail", "")))[:12_000]
                if not summary:
                    summary = "Tool returned no additional detail."
                event_type, title, actor = "runtime.result", f"{name} returned evidence", "lisa"
            self._append_activity_event(
                connection, activity_id, event_type, title, summary, actor,
                reference_type=reference_type, reference_id=item_id,
            )
            recorded += 1
        return recorded

    def activity_exists(self, activity_id: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                _sql("activity_exists.sql"), {"activity_id": activity_id}
            ).fetchone()
        return bool(row and row["found"])

    def update_activity_progress(
        self, activity_id: str, *, status: str, summary: str,
        next_action: str | None = None, next_action_at: str | None = None,
    ) -> None:
        if status not in {"queued", "running", "waiting"}:
            raise ValueError("Progress status must be queued, running, or waiting")
        with self._connect() as connection, connection.transaction():
            row = connection.execute(_sql("update_activity_progress.sql"), {
                "activity_id": activity_id, "status": status,
                "next_action": next_action, "next_action_at": next_action_at,
            }).fetchone()
            if row is None:
                raise ValueError("Activity does not exist or is already complete")
            self._append_activity_event(
                connection, activity_id, "progress", row["title"], summary, "lisa"
            )

    def create_activity_followup(
        self, *, activity_id: str, due_at: str | None, trigger_event_type: str | None,
        prompt: str, requested_by: str,
    ) -> str:
        followup_id = str(uuid4())
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("lock_activity_for_update.sql"), {
                "activity_id": activity_id,
            })
            row = connection.execute(_sql("create_activity_followup.sql"), {
                "id": followup_id, "activity_id": activity_id,
                "due_at": due_at, "trigger_event_type": trigger_event_type,
                "prompt": prompt, "requested_by": requested_by,
            }).fetchone()
            if row is None:
                raise ValueError("Activity does not exist or is already complete")
        return followup_id

    def approve_activity_followup(self, followup_id: str) -> bool:
        with self._connect() as connection, connection.transaction():
            row = connection.execute(_sql("approve_activity_followup.sql"), {
                "id": followup_id,
            }).fetchone()
            if row is not None:
                self._append_activity_event(
                    connection, str(row["activity_id"]), "followup_approved",
                    "Follow-up approved", f"Follow-up {followup_id} was approved by the owner.", "owner",
                )
        return row is not None

    def list_activity_followups(
        self, activity_id: str, limit: int = 20,
    ) -> list[dict[str, Any]]:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        with self._connect() as connection:
            rows = connection.execute(_sql("list_activity_followups.sql"), {
                "activity_id": activity_id, "limit": limit,
            }).fetchall()
        return [dict(row) for row in rows]

    def claim_due_activity_followups(self, limit: int = 10) -> list[dict[str, Any]]:
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("cancel_terminal_activity_followups.sql"))
            rows = connection.execute(_sql("claim_due_activity_followups.sql"), {
                "limit": limit,
            }).fetchall()
        return [dict(row) for row in rows]

    def finish_activity_followup(self, followup_id: str) -> None:
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("finish_activity_followup.sql"), {"id": followup_id})

    def release_activity_followup(self, followup_id: str) -> None:
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("release_activity_followup.sql"), {"id": followup_id})

    def create_evaluation(
        self,
        *,
        title: str,
        source: str,
        target_agent_name: str,
        session_id: str,
        policy_name: str,
        policy_id: str,
        policy_hash: str,
        judge_config: dict[str, Any],
        request_key: str,
        activity_id: str | None = None,
        context_path: str | None = None,
    ) -> EvaluationRun:
        """Create once per request key; a new key always starts an intentional rerun."""
        normalized_key = str(request_key)
        UUID(normalized_key)
        existing = self._get_evaluation_by_request(policy_hash, session_id, normalized_key)
        if existing:
            return EvaluationRun(**existing, created=False)

        standalone_activity = activity_id is None
        activity_id = activity_id or str(uuid4())
        evaluation_id = activity_id if standalone_activity else str(uuid4())
        try:
            with self._connect() as connection, connection.transaction():
                if standalone_activity:
                    connection.execute(
                        _sql("create_activity.sql"),
                        {"id": activity_id, "kind": "policy_evaluation", "title": title,
                         "source": source, "context_path": context_path,
                         "parent_id": None, "next_action": None,
                         "next_action_at": None, "origin_conversation_id": None,
                         "origin_slack_channel_id": None, "origin_slack_thread_ts": None,
                         "slack_channel_id": None, "slack_thread_ts": None},
                    )
                connection.execute(
                    _sql("create_evaluation.sql"),
                    {
                        "id": evaluation_id,
                        "activity_id": activity_id,
                        "target_agent_name": target_agent_name,
                        "session_id": session_id,
                        "policy_name": policy_name,
                        "policy_id": policy_id,
                        "policy_hash": policy_hash,
                        "judge_config": json.dumps(judge_config),
                        "request_key": normalized_key,
                    },
                )
                if standalone_activity:
                    self._append_activity_event(
                        connection, activity_id, "accepted", title, None, "lisa"
                    )
                    self._create_activity_presentation(
                        connection, activity_id, "policy_evaluation", False, None,
                        None, None, target_agent_name, {},
                    )
        except UniqueViolation:
            existing = self._get_evaluation_by_request(policy_hash, session_id, normalized_key)
            if existing:
                return EvaluationRun(**existing, created=False)
            raise
        return EvaluationRun(
            id=evaluation_id,
            activity_id=activity_id,
            session_id=session_id,
            created=True,
            context_path=context_path,
            policy_id=policy_id,
        )

    def _get_evaluation_by_request(
        self, policy_hash: str, session_id: str, request_key: str
    ) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                _sql("get_evaluation_by_request.sql"),
                {
                    "policy_hash": policy_hash,
                    "session_id": session_id,
                    "request_key": request_key,
                },
            ).fetchone()
        if row is None:
            return None
        return {
            "id": str(row["evaluation_id"]),
            "activity_id": str(row["activity_id"]),
            "session_id": row["session_id"],
            "policy_id": row["policy_id"],
            "context_path": row["context_path"],
            "status": row["status"],
            "verdict": row["verdict"],
            "score_id": row["score_id"],
        }

    def get_evaluation_by_request(
        self, policy_hash: str, session_id: str, request_key: str
    ) -> EvaluationRun | None:
        row = self._get_evaluation_by_request(policy_hash, session_id, request_key)
        return EvaluationRun(**row, created=False) if row else None

    def find_pending_evaluation(
        self, policy_hash: str, session_id: str
    ) -> tuple[EvaluationRun, ScoreOutboxItem] | None:
        with self._connect() as connection:
            row = connection.execute(
                _sql("find_pending_evaluation.sql"),
                {"policy_hash": policy_hash, "session_id": session_id},
            ).fetchone()
        return self._pending_evaluation_from_row(row)

    def find_pending_evaluations_for_session(
        self, target_agent_name: str, session_id: str
    ) -> list[tuple[EvaluationRun, ScoreOutboxItem]]:
        """Find every ambiguous score write before the new policy/evidence path runs."""
        with self._connect() as connection:
            rows = connection.execute(
                _sql("find_pending_evaluations_for_session.sql"),
                {"target_agent_name": target_agent_name, "session_id": session_id},
            ).fetchall()
        return [
            pending
            for row in rows
            if (pending := self._pending_evaluation_from_row(row)) is not None
        ]

    def find_pending_evaluation_for_policy(
        self,
        policy_id: str,
        target_agent_name: str,
        session_id: str,
    ) -> tuple[EvaluationRun, ScoreOutboxItem] | None:
        """Find the pending score outbox for one policy and session."""
        with self._connect() as connection:
            rows = connection.execute(
                _sql("find_pending_evaluation_for_policy.sql"),
                {
                    "policy_id": policy_id,
                    "target_agent_name": target_agent_name,
                    "session_id": session_id,
                },
            ).fetchall()
        if len(rows) > 1:
            ids = ", ".join(str(row["id"]) for row in rows)
            raise ValueError(
                "Multiple pending evaluations match this policy; resume a specific activity: "
                f"{ids}"
            )
        return self._pending_evaluation_from_row(rows[0] if rows else None)

    def policy_triage_decisions(
        self, session_id: str, policy_ids: list[str]
    ) -> dict[str, str]:
        """Return the triage decision already recorded for each session-policy pair."""
        if not policy_ids:
            return {}
        with self._connect() as connection:
            rows = connection.execute(
                _sql("get_policy_triage_decisions.sql"),
                {"session_id": session_id, "policy_ids": policy_ids},
            ).fetchall()
        return {row["policy_id"]: row["decision"] for row in rows}

    def record_policy_triage_decisions(
        self, session_id: str, decisions: list[dict[str, str]]
    ) -> None:
        """Persist completed Decisions answers once for each session-policy pair."""
        policy_ids = [item.get("id", "") for item in decisions]
        if any(not policy_id for policy_id in policy_ids) or len(set(policy_ids)) != len(policy_ids):
            raise ValueError("Policy triage decisions require unique policy IDs")
        valid_decisions = {
            "suspected_violation",
            "no_violation_found",
            "insufficient_evidence",
        }
        if any(item.get("decision") not in valid_decisions for item in decisions):
            raise ValueError("Policy triage decision is invalid")
        with self._connect() as connection, connection.transaction():
            for item in decisions:
                connection.execute(
                    _sql("record_policy_triage_decision.sql"),
                    {
                        "session_id": session_id,
                        "policy_id": item["id"],
                        "decision": item["decision"],
                    },
                )

    def _pending_evaluation_from_row(
        self, row: dict[str, Any] | None
    ) -> tuple[EvaluationRun, ScoreOutboxItem] | None:
        if row is None:
            return None
        activity = EvaluationRun(
            id=str(row["evaluation_id"]),
            activity_id=str(row["activity_id"]),
            session_id=row["session_id"],
            created=False,
            context_path=row["context_path"],
            status=row["status"],
            verdict=row["verdict"],
            score_id=row["evaluation_score_id"],
            policy_id=row["policy_id"],
        )
        outbox = ScoreOutboxItem(
            id=str(row["outbox_id"]),
            evaluation_id=str(row["evaluation_id"]),
            status=row["outbox_status"],
            score_payload=row["score_payload"],
            score_id=row["outbox_score_id"],
        )
        return activity, outbox

    def finish_activity_without_claim(
        self, activity_id: str, summary: str, *, status: str = "succeeded"
    ) -> bool:
        if status not in {"succeeded", "failed", "cancelled"}:
            raise ValueError("Activity must finish with a terminal status")
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("lock_activity_for_update.sql"), {
                "activity_id": activity_id,
            })
            finished = connection.execute(
                _sql("finish_unclaimed_activity.sql"),
                {
                    "activity_id": activity_id,
                    "status": status,
                    "title": f"Activity {status}",
                    "summary": summary,
                },
            )
            if finished.fetchone() is None:
                return False
            connection.execute(_sql("sync_activity_actions.sql"), {"activity_id": activity_id})
            return True

    def complete_agent_activity(
        self, activity_id: str, summary: str, *, status: str = "succeeded"
    ) -> bool:
        """Finish queued agent-owned work, leaving evaluation lifecycles to the evaluator."""
        if status not in {"succeeded", "failed", "cancelled"}:
            raise ValueError("Activity must finish with a terminal status")
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("lock_activity_for_update.sql"), {
                "activity_id": activity_id,
            })
            finished = connection.execute(
                _sql("complete_agent_activity.sql"),
                {
                    "activity_id": activity_id,
                    "status": status,
                    "title": f"Activity {status}",
                    "summary": summary,
                },
            )
            if finished.fetchone() is None:
                return False
            connection.execute(_sql("sync_activity_actions.sql"), {"activity_id": activity_id})
            return True

    def complete_chat_activity(
        self, activity_id: str, summary: str, *, status: str = "succeeded",
    ) -> bool:
        """Finish a replied-to chat request unless its trajectory leaves work pending."""
        if status not in {"succeeded", "failed", "cancelled"}:
            raise ValueError("Chat activity status must be succeeded, failed, or cancelled")
        return self.complete_agent_activity(activity_id, summary, status=status)

    def finish_activity_when_complete(
        self,
        activity_id: str,
        *,
        actor: str | None = None,
        fencing_token: int | None = None,
    ) -> bool:
        with self._connect() as connection, connection.transaction():
            finished = connection.execute(
                _sql("finish_activity_when_complete.sql"),
                {
                    "activity_id": activity_id,
                    "actor": actor,
                    "fencing_token": fencing_token,
                },
            ).fetchone()
            if finished is None:
                return False
            connection.execute(_sql("sync_activity_actions.sql"), {"activity_id": activity_id})
            return True

    def get_evaluation(self, evaluation_id: str) -> EvaluationRun | None:
        with self._connect() as connection:
            row = connection.execute(
                _sql("get_evaluation.sql"), {"evaluation_id": evaluation_id}
            ).fetchone()
        if row is None:
            return None
        return EvaluationRun(
            id=str(row["evaluation_id"]),
            activity_id=str(row["activity_id"]),
            session_id=row["session_id"],
            created=False,
            context_path=row["context_path"],
            status=row["status"],
            verdict=row["verdict"],
            score_id=row["score_id"],
            policy_id=row["policy_id"],
        )

    def claim_activity(
        self, activity_id: str, owner: str, *, lease_seconds: int = 300
    ) -> ActivityClaim | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")
        with self._connect() as connection, connection.transaction():
            row = connection.execute(
                _sql("claim_activity.sql"),
                {
                    "activity_id": activity_id,
                    "owner": owner,
                    "lease_seconds": lease_seconds,
                },
            ).fetchone()
            if row is None:
                return None
            token = row["fencing_token"]
            self._append_claimed_event(
                connection,
                activity_id,
                token,
                owner,
                "claimed",
                "Activity started",
                None,
            )
        return ActivityClaim(
            activity_id=str(row["id"]),
            owner=owner,
            fencing_token=token,
            lease_expires_at=row["lease_expires_at"],
        )

    def renew_lease(
        self,
        activity_id: str,
        owner: str,
        fencing_token: int,
        *,
        lease_seconds: int = 300,
    ) -> ActivityClaim | None:
        """Extend a live lease without changing its fencing generation."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")
        with self._connect() as connection:
            row = connection.execute(
                _sql("renew_lease.sql"),
                {
                    "activity_id": activity_id,
                    "owner": owner,
                    "fencing_token": fencing_token,
                    "lease_seconds": lease_seconds,
                },
            ).fetchone()
        if row is None:
            return None
        return ActivityClaim(
            activity_id=str(row["id"]),
            owner=owner,
            fencing_token=row["fencing_token"],
            lease_expires_at=row["lease_expires_at"],
        )

    def append_event(
        self,
        activity_id: str,
        event_type: str,
        *,
        title: str,
        actor: str,
        fencing_token: int,
        summary: str | None = None,
    ) -> int:
        with self._connect() as connection, connection.transaction():
            return self._append_claimed_event(
                connection,
                activity_id,
                fencing_token,
                actor,
                event_type,
                title,
                summary,
            )

    def record_activity_event(
        self,
        activity_id: str,
        event_type: str,
        *,
        title: str,
        summary: str | None = None,
        actor: str = "lisa",
    ) -> int:
        """Append a user-inspectable event to an open activity."""
        if not event_type.strip() or not title.strip() or not actor.strip():
            raise ValueError("Activity events require an event type, title, and actor")
        with self._connect() as connection, connection.transaction():
            return self._append_activity_event(
                connection, activity_id, event_type, title, summary, actor
            )

    def _append_activity_event(
        self, connection, activity_id, event_type, title, summary, actor,
        *, reference_type: str | None = None, reference_id: str | None = None,
    ) -> int:
        if (reference_type is None) != (reference_id is None):
            raise ValueError("Activity event reference type and ID must be supplied together")
        row = connection.execute(
            _sql("append_activity_event.sql"),
            {
                "activity_id": activity_id,
                "event_type": event_type,
                "title": title,
                "summary": summary,
                "actor": actor,
                "reference_type": reference_type,
                "reference_id": reference_id,
            },
        ).fetchone()
        return row["seq"]

    def _append_claimed_event(
        self,
        connection,
        activity_id,
        fencing_token,
        actor,
        event_type,
        title,
        summary,
    ) -> int:
        row = connection.execute(
            _sql("append_claimed_event.sql"),
            {
                "activity_id": activity_id,
                "fencing_token": fencing_token,
                "owner": actor,
                "event_type": event_type,
                "title": title,
                "summary": summary,
                "actor": actor,
            },
        ).fetchone()
        if row is None:
            raise StaleLeaseError("Activity lease expired or was superseded")
        return row["seq"]

    def list_events(self, activity_id: str) -> list[ActivityEvent]:
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_events.sql"), {"activity_id": activity_id}
            ).fetchall()
        return [
            ActivityEvent(
                activity_id=str(row["activity_id"]),
                sequence=row["seq"],
                event_type=row["event_type"],
                title=row["title"],
                summary=row["summary"],
                actor=row["actor"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def save_context_item(
        self,
        activity_id: str,
        kind: str,
        content: dict[str, Any],
        *,
        actor: str,
        fencing_token: int,
        source_ref: str | None = None,
    ) -> str:
        item_id = str(uuid4())
        encoded = json.dumps(content, ensure_ascii=False, sort_keys=True)
        content_hash = sha256(encoded.encode("utf-8")).hexdigest()
        with self._connect() as connection, connection.transaction():
            row = connection.execute(
                _sql("save_context_item.sql"),
                {
                    "id": item_id,
                    "activity_id": activity_id,
                    "kind": kind,
                    "content": encoded,
                    "source_ref": source_ref,
                    "content_hash": content_hash,
                    "owner": actor,
                    "fencing_token": fencing_token,
                },
            ).fetchone()
            if row is None:
                raise StaleLeaseError("Activity lease expired or was superseded")
            self._append_claimed_event(
                connection,
                activity_id,
                fencing_token,
                actor,
                "context_saved",
                f"Saved {kind.replace('_', ' ')}",
                source_ref,
            )
        return item_id

    def get_context_item(self, context_id: str) -> ActivityContext:
        with self._connect() as connection:
            row = connection.execute(_sql("get_context_item.sql"), {"id": context_id}).fetchone()
        if row is None:
            raise KeyError(context_id)
        return ActivityContext(
            id=str(row["id"]),
            activity_id=str(row["activity_id"]),
            sequence=row["seq"],
            kind=row["kind"],
            content=row["content"],
            content_hash=row["content_hash"],
        )

    def get_context_item_for_activity(
        self, activity_id: str, kind: str
    ) -> ActivityContext | None:
        with self._connect() as connection:
            row = connection.execute(
                _sql("get_context_item_for_activity.sql"),
                {"activity_id": activity_id, "kind": kind},
            ).fetchone()
        if row is None:
            return None
        return ActivityContext(
            id=str(row["id"]),
            activity_id=str(row["activity_id"]),
            sequence=row["seq"],
            kind=row["kind"],
            content=row["content"],
            content_hash=row["content_hash"],
        )

    def prepare_score_outbox(
        self,
        evaluation_id: str,
        payload: dict[str, Any],
        *,
        actor: str,
        fencing_token: int,
    ) -> ScoreOutboxItem:
        """Persist a score payload before submitting it to TheLake."""
        score_id = str(payload["score_id"])
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        payload_hash = sha256(encoded.encode("utf-8")).hexdigest()
        with self._connect() as connection, connection.transaction():
            parent = connection.execute(
                _sql("get_activity_for_evaluation.sql"), {"evaluation_id": evaluation_id}
            ).fetchone()
            if parent is None:
                raise KeyError(evaluation_id)
            activity_id = str(parent["activity_id"])
            if not self._lock_activity(connection, activity_id, actor, fencing_token):
                raise StaleLeaseError("Activity lease expired or was superseded")
            row = connection.execute(
                _sql("create_score_outbox.sql"),
                {
                    "id": str(uuid4()),
                    "evaluation_id": evaluation_id,
                    "idempotency_key": score_id,
                    "score_payload": encoded,
                    "payload_hash": payload_hash,
                },
            ).fetchone()
            created = row is not None
            if row is None:
                row = connection.execute(
                    _sql("get_score_outbox_by_key.sql"),
                    {"idempotency_key": score_id},
                ).fetchone()
                if (
                    row is None
                    or str(row["evaluation_id"]) != evaluation_id
                    or row["payload_hash"] != payload_hash
                ):
                    raise ValueError("Score idempotency key already has a different payload")
            if created:
                self._append_claimed_event(
                    connection,
                    activity_id,
                    fencing_token,
                    actor,
                    "score_prepared",
                    "Evaluation score prepared",
                    score_id,
                )
        return ScoreOutboxItem(
            id=str(row["id"]),
            evaluation_id=str(row["evaluation_id"]),
            status=row["status"],
            score_payload=row["score_payload"],
            score_id=row["score_id"],
        )

    def get_score_outbox(self, evaluation_id: str) -> ScoreOutboxItem | None:
        with self._connect() as connection:
            row = connection.execute(
                _sql("get_score_outbox.sql"), {"evaluation_id": evaluation_id}
            ).fetchone()
        if row is None:
            return None
        return ScoreOutboxItem(
            id=str(row["id"]),
            evaluation_id=str(row["evaluation_id"]),
            status=row["status"],
            score_payload=row["score_payload"],
            score_id=row["score_id"],
        )

    def acknowledge_score(
        self,
        activity_id: str,
        evaluation_id: str,
        outbox_id: str,
        *,
        score_id: str,
        verdict: str,
        actor: str,
        fencing_token: int,
        summary: str | None = None,
        auto_finish: bool = True,
    ) -> None:
        if verdict not in {"pass", "fail", "uncertain", "insufficient_evidence"}:
            raise ValueError("Unsupported evaluation verdict")
        with self._connect() as connection, connection.transaction():
            if not self._lock_activity(connection, activity_id, actor, fencing_token):
                raise StaleLeaseError("Activity lease expired or was superseded")
            acknowledged = connection.execute(
                _sql("acknowledge_score_outbox.sql"),
                {
                    "outbox_id": outbox_id,
                    "evaluation_id": evaluation_id,
                    "score_id": score_id,
                    "verdict": verdict,
                },
            ).fetchone()
            if acknowledged is None:
                raise ValueError("Score outbox entry is missing or already acknowledged")
            result = connection.execute(
                _sql("complete_evaluation_result.sql"),
                {
                    "evaluation_id": evaluation_id,
                    "verdict": verdict,
                    "score_id": score_id,
                },
            )
            if result.rowcount == 0:
                raise KeyError(evaluation_id)
            if auto_finish:
                remaining = connection.execute(
                    _sql("activity_has_incomplete_evaluations.sql"), {"activity_id": activity_id}
                ).fetchone()
                if remaining and not remaining["has_incomplete"]:
                    self._finish_activity(
                        connection,
                        activity_id,
                        status="succeeded",
                        actor=actor,
                        fencing_token=fencing_token,
                        summary=summary,
                    )

    def release_activity(
        self,
        activity_id: str,
        *,
        actor: str,
        fencing_token: int,
        reason: str,
    ) -> None:
        """Return a retryable run to the queue while preserving its inputs."""
        with self._connect() as connection, connection.transaction():
            row = connection.execute(
                _sql("release_activity.sql"),
                {
                    "activity_id": activity_id,
                    "owner": actor,
                    "fencing_token": fencing_token,
                    "summary": reason,
                    "event_type": "retryable_error",
                    "title": "Activity will retry",
                    "actor": actor,
                },
            ).fetchone()
            if row is not None:
                connection.execute(_sql("sync_activity_actions.sql"), {"activity_id": activity_id})
        if row is None:
            raise StaleLeaseError("Activity lease expired or was superseded")

    def _lock_activity(self, connection, activity_id, actor, fencing_token) -> bool:
        row = connection.execute(
            _sql("lock_activity_claim.sql"),
            {
                "activity_id": activity_id,
                "owner": actor,
                "fencing_token": fencing_token,
            },
        ).fetchone()
        return row is not None

    def finish_activity(
        self,
        activity_id: str,
        *,
        status: str,
        actor: str,
        fencing_token: int,
        summary: str | None = None,
    ) -> None:
        if status not in {"succeeded", "failed", "cancelled"}:
            raise ValueError("A activity must finish with a terminal status")
        with self._connect() as connection, connection.transaction():
            self._finish_activity(
                connection,
                activity_id,
                status=status,
                actor=actor,
                fencing_token=fencing_token,
                summary=summary,
            )

    def _finish_activity(
        self, connection, activity_id, *, status, actor, fencing_token, summary
    ) -> None:
        row = connection.execute(
            _sql("finish_activity.sql"),
            {
                "activity_id": activity_id,
                "status": status,
                "owner": actor,
                "fencing_token": fencing_token,
                "event_type": status,
                "title": f"Activity {status}",
                "summary": summary,
                "actor": actor,
            },
        ).fetchone()
        if row is None:
            if status == "succeeded":
                acknowledged = connection.execute(
                    _sql("has_acknowledged_score.sql"),
                    {"activity_id": activity_id},
                ).fetchone()
                if acknowledged and not acknowledged["acknowledged"]:
                    raise ValueError(
                        "A successful evaluation requires an acknowledged score outbox"
                    )
            raise StaleLeaseError("Activity lease expired or was superseded")
        connection.execute(_sql("sync_activity_actions.sql"), {"activity_id": activity_id})
