from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from urllib.parse import quote

from ..context import WorkspaceContext
from ..security import redact_evidence, redact_text
from ..storage import atomic_write_text, file_lock
from ..thelake_client import TheLakeClient
from .evidence import SESSION_SPAN_LIMIT
from .scores import matches_policy_score


class FailureStore:
    def __init__(self, context: WorkspaceContext, client: TheLakeClient):
        self.context = context
        self.client = client

    def _pending_failures_path(self) -> Path:
        home = self.context.home()
        path = home / "pending-evaluation-failures.json"
        if path.is_symlink() or not path.parent.resolve().is_relative_to(home.resolve()):
            raise ValueError(
                "Pending failure record escapes Lisa's workspace home or uses a symlink"
            )
        return path

    def _read_pending_failures(self, path: Path | None = None) -> list[dict]:
        path = path or self._pending_failures_path()
        try:
            items = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return []
        except json.JSONDecodeError as error:
            raise ValueError("Lisa's pending evaluation failure record is invalid") from error
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
            raise ValueError("Lisa's pending evaluation failure record has an invalid shape")
        return items

    def change(self, change) -> list[dict]:
        path = self._pending_failures_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = path.with_suffix(".lock")
        if lock_path.is_symlink():
            raise ValueError("Pending failure lock cannot be a symlink")
        with file_lock(lock_path):
            pending = self._read_pending_failures(path)
            pending = change(pending)
            atomic_write_text(path, json.dumps(pending, ensure_ascii=False))
            return pending

    def record(
        self,
        evaluator: dict,
        session_id: str,
        trace_id: str,
        rationale: str,
        timestamp: str | None,
        *,
        score_payload: dict | None = None,
    ) -> None:
        item = {
            **evaluator,
            "session_id": session_id,
            "trace_id": trace_id,
            "rationale": redact_text(rationale),
            "timestamp": timestamp,
            "state": "pending_score" if score_payload else "ready",
            "score_payload": score_payload,
        }
        key = self.key(item)

        def upsert(items: list[dict]) -> list[dict]:
            items = [existing for existing in items if self.key(existing) != key]
            if len(items) >= 10_000:
                raise RuntimeError(
                    "Lisa's pending failure queue is full; follow-ups need attention"
                )
            items.append(item)
            return items

        self.change(upsert)

    def key(self, item: dict) -> str:
        if item.get("policy_hash"):
            return sha256(
                json.dumps(
                    [item["policy_hash"], item.get("session_id"), item.get("trace_id")]
                ).encode()
            ).hexdigest()
        identity = json.dumps(
            [
                item.get("evaluator_id"),
                item.get("version"),
                item.get("session_id"),
                item.get("trace_id"),
            ],
            separators=(",", ":"),
        )
        return sha256(identity.encode("utf-8")).hexdigest()

    def reconcile(self) -> list[dict]:
        """Finish score writes interrupted after Lisa saved the failure locally."""
        pending = self._read_pending_failures()
        for queued in pending:
            if queued.get("state") != "pending_score":
                continue
            payload = queued.get("score_payload")
            if not isinstance(payload, dict):
                raise ValueError("A pending failure is missing its recoverable score")
            evidence = redact_evidence(
                self.client.get(
                    f"/v1/sessions/{quote(queued['session_id'], safe='')}?limit={SESSION_SPAN_LIMIT}"
                )
            )
            already_stored = any(
                score.get("score_id") == payload.get("score_id")
                and matches_policy_score(score, queued, str(queued.get("trace_id", "")))
                for score in evidence.get("scores", [])
            )
            if not already_stored:
                self.client.post("/v1/scores", payload)

            pending = self.mark_ready(queued, queued["session_id"], queued["trace_id"])
        return pending

    def list_pending_evaluation_failures(self) -> list[dict] | str:
        """List stored evaluation failures that still need Lisa's follow-up."""
        items = self.reconcile()
        if not items:
            return "No evaluation failures are awaiting follow-up."
        visible = []
        for item in items:
            if not isinstance(item, dict):
                continue
            visible.append(
                {
                    "failure_key": self.key(item),
                    **{
                        key: item.get(key)
                        for key in (
                            "name",
                            "target_agent_name",
                            "session_id",
                            "rationale",
                            "timestamp",
                        )
                    },
                }
            )
        return visible or "No evaluation failures are awaiting follow-up."

    def complete_evaluation_failure_followup(
        self,
        failure_key: str,
    ) -> dict:
        """Clear a pending failure after Lisa investigated and reported it."""
        if self.context.turn_source not in {"web", "slack", "scheduled"}:
            raise ValueError("This conversation cannot complete an evaluation follow-up")
        counts = {"before": 0, "after": 0}
        completed_session: list[str | None] = [None]

        def remove(items: list[dict]) -> list[dict]:
            counts["before"] = len(items)
            for index, item in enumerate(items):
                if self.key(item) == failure_key:
                    completed_session[0] = item.get("session_id")
                    del items[index]
                    break
            counts["after"] = len(items)
            return items

        self.change(remove)
        return {
            "status": "completed" if counts["after"] < counts["before"] else "not_pending",
            "session_id": completed_session[0],
        }

    def mark_ready(self, evaluator: dict, session_id: str, trace_id: str) -> list[dict]:
        key = self.key({**evaluator, "session_id": session_id, "trace_id": trace_id})

        def update(items: list[dict]) -> list[dict]:
            for item in items:
                if self.key(item) == key:
                    item["state"] = "ready"
                    item.pop("score_payload", None)
                    break
            return items

        return self.change(update)
