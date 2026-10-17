"""Lisa work-item persistence using a deployment-bound PostgreSQL schema."""

from __future__ import annotations

import json
from hashlib import sha256
from importlib.resources import files
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row

from ..models.work_items import (
    EvaluationWorkItem,
    ScoreOutboxItem,
    WorkEvent,
    WorkItemClaim,
    WorkItemContext,
)


class DeploymentBindingError(RuntimeError):
    """Raised when a Lisa database is bound to a different workspace."""


class StaleLeaseError(RuntimeError):
    """Raised when a worker attempts a write after its lease was superseded."""


def _workspace_fingerprint(workspace_id: str) -> str:
    normalized = str(UUID(workspace_id))
    return sha256(normalized.encode("utf-8")).hexdigest()


def _sql(name: str) -> str:
    return files("lisa").joinpath("sql", "work_items", name).read_text(encoding="utf-8")


class PostgresWorkItemStore:
    """Typed lifecycle operations for work items; raw SQL stays in .sql files."""

    def __init__(self, dsn: str, workspace_id: str):
        self.dsn = dsn
        self.workspace_fingerprint = _workspace_fingerprint(workspace_id)
        with self._connect() as connection:
            binding = connection.execute(_sql("get_deployment.sql")).fetchone()
        if not binding or binding["workspace_fingerprint"] != self.workspace_fingerprint:
            raise DeploymentBindingError("Lisa database is not bound to the configured workspace")

    def _connect(self):
        return psycopg.connect(self.dsn, row_factory=dict_row)

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
        work_item_id: str | None = None,
        context_path: str | None = None,
    ) -> EvaluationWorkItem:
        """Create once per request key; a new key always starts an intentional rerun."""
        normalized_key = str(request_key)
        UUID(normalized_key)
        existing = self._get_evaluation_by_request(policy_hash, session_id, normalized_key)
        if existing:
            return EvaluationWorkItem(**existing, created=False)

        work_item_id = work_item_id or str(uuid4())
        try:
            with self._connect() as connection, connection.transaction():
                connection.execute(
                    _sql("create_work_item.sql"),
                    {
                        "id": work_item_id,
                        "kind": "evaluation",
                        "title": title,
                        "source": source,
                        "context_path": context_path,
                    },
                )
                connection.execute(
                    _sql("create_evaluation.sql"),
                    {
                        "work_item_id": work_item_id,
                        "target_agent_name": target_agent_name,
                        "session_id": session_id,
                        "policy_name": policy_name,
                        "policy_id": policy_id,
                        "policy_hash": policy_hash,
                        "judge_config": json.dumps(judge_config),
                        "request_key": normalized_key,
                    },
                )
                self._append_unclaimed_event(
                    connection,
                    work_item_id,
                    "accepted",
                    "Evaluation accepted",
                    None,
                    "lisa",
                )
        except UniqueViolation:
            existing = self._get_evaluation_by_request(policy_hash, session_id, normalized_key)
            if existing:
                return EvaluationWorkItem(**existing, created=False)
            raise
        return EvaluationWorkItem(
            id=work_item_id,
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
            "id": str(row["id"]),
            "session_id": row["session_id"],
            "policy_id": row["policy_id"],
            "context_path": row["context_path"],
            "status": row["status"],
            "verdict": row["verdict"],
            "score_id": row["score_id"],
        }

    def get_evaluation_by_request(
        self, policy_hash: str, session_id: str, request_key: str
    ) -> EvaluationWorkItem | None:
        row = self._get_evaluation_by_request(policy_hash, session_id, request_key)
        return EvaluationWorkItem(**row, created=False) if row else None

    def find_pending_evaluation(
        self, policy_hash: str, session_id: str
    ) -> tuple[EvaluationWorkItem, ScoreOutboxItem] | None:
        with self._connect() as connection:
            row = connection.execute(
                _sql("find_pending_evaluation.sql"),
                {"policy_hash": policy_hash, "session_id": session_id},
            ).fetchone()
        return self._pending_evaluation_from_row(row)

    def find_pending_evaluations_for_session(
        self, target_agent_name: str, session_id: str
    ) -> list[tuple[EvaluationWorkItem, ScoreOutboxItem]]:
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
    ) -> tuple[EvaluationWorkItem, ScoreOutboxItem] | None:
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
                "Multiple pending evaluations match this policy; resume a specific work item: "
                f"{ids}"
            )
        return self._pending_evaluation_from_row(rows[0] if rows else None)

    def _pending_evaluation_from_row(
        self, row: dict[str, Any] | None
    ) -> tuple[EvaluationWorkItem, ScoreOutboxItem] | None:
        if row is None:
            return None
        work_item = EvaluationWorkItem(
            id=str(row["id"]),
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
            work_item_id=str(row["id"]),
            status=row["outbox_status"],
            score_payload=row["score_payload"],
            score_id=row["outbox_score_id"],
        )
        return work_item, outbox

    def get_evaluation(self, work_item_id: str) -> EvaluationWorkItem | None:
        with self._connect() as connection:
            row = connection.execute(
                _sql("get_evaluation.sql"), {"work_item_id": work_item_id}
            ).fetchone()
        if row is None:
            return None
        return EvaluationWorkItem(
            id=str(row["id"]),
            session_id=row["session_id"],
            created=False,
            context_path=row["context_path"],
            status=row["status"],
            verdict=row["verdict"],
            score_id=row["score_id"],
            policy_id=row["policy_id"],
        )

    def claim_work_item(
        self, work_item_id: str, owner: str, *, lease_seconds: int = 300
    ) -> WorkItemClaim | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")
        with self._connect() as connection, connection.transaction():
            row = connection.execute(
                _sql("claim_work_item.sql"),
                {
                    "work_item_id": work_item_id,
                    "owner": owner,
                    "lease_seconds": lease_seconds,
                },
            ).fetchone()
            if row is None:
                return None
            token = row["fencing_token"]
            self._append_claimed_event(
                connection,
                work_item_id,
                token,
                owner,
                "claimed",
                "Evaluation started",
                None,
            )
        return WorkItemClaim(
            work_item_id=str(row["id"]),
            owner=owner,
            fencing_token=token,
            lease_expires_at=row["lease_expires_at"],
        )

    def renew_lease(
        self,
        work_item_id: str,
        owner: str,
        fencing_token: int,
        *,
        lease_seconds: int = 300,
    ) -> WorkItemClaim | None:
        """Extend a live lease without changing its fencing generation."""
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")
        with self._connect() as connection:
            row = connection.execute(
                _sql("renew_lease.sql"),
                {
                    "work_item_id": work_item_id,
                    "owner": owner,
                    "fencing_token": fencing_token,
                    "lease_seconds": lease_seconds,
                },
            ).fetchone()
        if row is None:
            return None
        return WorkItemClaim(
            work_item_id=str(row["id"]),
            owner=owner,
            fencing_token=row["fencing_token"],
            lease_expires_at=row["lease_expires_at"],
        )

    def append_event(
        self,
        work_item_id: str,
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
                work_item_id,
                fencing_token,
                actor,
                event_type,
                title,
                summary,
            )

    def _append_unclaimed_event(
        self, connection, work_item_id, event_type, title, summary, actor
    ) -> int:
        row = connection.execute(
            _sql("append_unclaimed_event.sql"),
            {
                "work_item_id": work_item_id,
                "event_type": event_type,
                "title": title,
                "summary": summary,
                "actor": actor,
            },
        ).fetchone()
        return row["seq"]

    def _append_claimed_event(
        self,
        connection,
        work_item_id,
        fencing_token,
        actor,
        event_type,
        title,
        summary,
    ) -> int:
        row = connection.execute(
            _sql("append_claimed_event.sql"),
            {
                "work_item_id": work_item_id,
                "fencing_token": fencing_token,
                "owner": actor,
                "event_type": event_type,
                "title": title,
                "summary": summary,
                "actor": actor,
            },
        ).fetchone()
        if row is None:
            raise StaleLeaseError("Work item lease expired or was superseded")
        return row["seq"]

    def list_events(self, work_item_id: str) -> list[WorkEvent]:
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_events.sql"), {"work_item_id": work_item_id}
            ).fetchall()
        return [
            WorkEvent(
                work_item_id=str(row["work_item_id"]),
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
        work_item_id: str,
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
                    "work_item_id": work_item_id,
                    "kind": kind,
                    "content": encoded,
                    "source_ref": source_ref,
                    "content_hash": content_hash,
                    "owner": actor,
                    "fencing_token": fencing_token,
                },
            ).fetchone()
            if row is None:
                raise StaleLeaseError("Work item lease expired or was superseded")
            self._append_claimed_event(
                connection,
                work_item_id,
                fencing_token,
                actor,
                "context_saved",
                f"Saved {kind.replace('_', ' ')}",
                source_ref,
            )
        return item_id

    def get_context_item(self, context_id: str) -> WorkItemContext:
        with self._connect() as connection:
            row = connection.execute(_sql("get_context_item.sql"), {"id": context_id}).fetchone()
        if row is None:
            raise KeyError(context_id)
        return WorkItemContext(
            id=str(row["id"]),
            work_item_id=str(row["work_item_id"]),
            sequence=row["seq"],
            kind=row["kind"],
            content=row["content"],
            content_hash=row["content_hash"],
        )

    def get_context_item_for_work_item(
        self, work_item_id: str, kind: str
    ) -> WorkItemContext | None:
        with self._connect() as connection:
            row = connection.execute(
                _sql("get_context_item_for_work_item.sql"),
                {"work_item_id": work_item_id, "kind": kind},
            ).fetchone()
        if row is None:
            return None
        return WorkItemContext(
            id=str(row["id"]),
            work_item_id=str(row["work_item_id"]),
            sequence=row["seq"],
            kind=row["kind"],
            content=row["content"],
            content_hash=row["content_hash"],
        )

    def prepare_score_outbox(
        self,
        work_item_id: str,
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
            if not self._lock_work_item(connection, work_item_id, actor, fencing_token):
                raise StaleLeaseError("Work item lease expired or was superseded")
            row = connection.execute(
                _sql("create_score_outbox.sql"),
                {
                    "id": str(uuid4()),
                    "work_item_id": work_item_id,
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
                    or str(row["work_item_id"]) != work_item_id
                    or row["payload_hash"] != payload_hash
                ):
                    raise ValueError("Score idempotency key already has a different payload")
            if created:
                self._append_claimed_event(
                    connection,
                    work_item_id,
                    fencing_token,
                    actor,
                    "score_prepared",
                    "Evaluation score prepared",
                    score_id,
                )
        return ScoreOutboxItem(
            id=str(row["id"]),
            work_item_id=str(row["work_item_id"]),
            status=row["status"],
            score_payload=row["score_payload"],
            score_id=row["score_id"],
        )

    def get_score_outbox(self, work_item_id: str) -> ScoreOutboxItem | None:
        with self._connect() as connection:
            row = connection.execute(
                _sql("get_score_outbox.sql"), {"work_item_id": work_item_id}
            ).fetchone()
        if row is None:
            return None
        return ScoreOutboxItem(
            id=str(row["id"]),
            work_item_id=str(row["work_item_id"]),
            status=row["status"],
            score_payload=row["score_payload"],
            score_id=row["score_id"],
        )

    def acknowledge_score(
        self,
        work_item_id: str,
        outbox_id: str,
        *,
        score_id: str,
        verdict: str,
        actor: str,
        fencing_token: int,
        summary: str | None = None,
    ) -> None:
        if verdict not in {"pass", "fail", "uncertain", "insufficient_evidence"}:
            raise ValueError("Unsupported evaluation verdict")
        with self._connect() as connection, connection.transaction():
            if not self._lock_work_item(connection, work_item_id, actor, fencing_token):
                raise StaleLeaseError("Work item lease expired or was superseded")
            acknowledged = connection.execute(
                _sql("acknowledge_score_outbox.sql"),
                {
                    "outbox_id": outbox_id,
                    "work_item_id": work_item_id,
                    "score_id": score_id,
                    "verdict": verdict,
                },
            ).fetchone()
            if acknowledged is None:
                raise ValueError("Score outbox entry is missing or already acknowledged")
            result = connection.execute(
                _sql("complete_evaluation_result.sql"),
                {
                    "work_item_id": work_item_id,
                    "verdict": verdict,
                    "score_id": score_id,
                },
            )
            if result.rowcount == 0:
                raise KeyError(work_item_id)
            self._finish_work_item(
                connection,
                work_item_id,
                status="succeeded",
                actor=actor,
                fencing_token=fencing_token,
                summary=summary,
            )

    def release_work_item(
        self,
        work_item_id: str,
        *,
        actor: str,
        fencing_token: int,
        reason: str,
    ) -> None:
        """Return a retryable run to the queue while preserving its inputs."""
        with self._connect() as connection, connection.transaction():
            row = connection.execute(
                _sql("release_work_item.sql"),
                {
                    "work_item_id": work_item_id,
                    "owner": actor,
                    "fencing_token": fencing_token,
                    "summary": reason,
                    "event_type": "retryable_error",
                    "title": "Evaluation will retry",
                    "actor": actor,
                },
            ).fetchone()
        if row is None:
            raise StaleLeaseError("Work item lease expired or was superseded")

    def _lock_work_item(self, connection, work_item_id, actor, fencing_token) -> bool:
        row = connection.execute(
            _sql("lock_work_item_claim.sql"),
            {
                "work_item_id": work_item_id,
                "owner": actor,
                "fencing_token": fencing_token,
            },
        ).fetchone()
        return row is not None

    def finish_work_item(
        self,
        work_item_id: str,
        *,
        status: str,
        actor: str,
        fencing_token: int,
        summary: str | None = None,
    ) -> None:
        if status not in {"succeeded", "failed", "cancelled"}:
            raise ValueError("A work item must finish with a terminal status")
        with self._connect() as connection, connection.transaction():
            self._finish_work_item(
                connection,
                work_item_id,
                status=status,
                actor=actor,
                fencing_token=fencing_token,
                summary=summary,
            )

    def _finish_work_item(
        self, connection, work_item_id, *, status, actor, fencing_token, summary
    ) -> None:
        row = connection.execute(
            _sql("finish_work_item.sql"),
            {
                "work_item_id": work_item_id,
                "status": status,
                "owner": actor,
                "fencing_token": fencing_token,
                "event_type": status,
                "title": f"Evaluation {status}",
                "summary": summary,
                "actor": actor,
            },
        ).fetchone()
        if row is None:
            if status == "succeeded":
                acknowledged = connection.execute(
                    _sql("has_acknowledged_score.sql"),
                    {"work_item_id": work_item_id},
                ).fetchone()
                if acknowledged and not acknowledged["acknowledged"]:
                    raise ValueError(
                        "A successful evaluation requires an acknowledged score outbox"
                    )
            raise StaleLeaseError("Work item lease expired or was superseded")
