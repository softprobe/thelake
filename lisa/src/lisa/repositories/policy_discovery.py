"""Postgres persistence for sampled policy-gap signals and suggestions."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from importlib.resources import files
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

from ..models.policies import PolicyRecord
from ..models.policy_discovery import (
    MIN_POLICY_SUGGESTION_ERROR_SAMPLES,
    has_minimum_error_samples,
)
from .proactive_updates import create_proactive_update


def _sql(name: str) -> str:
    return files("lisa").joinpath("sql", "policies", name).read_text(encoding="utf-8")


class PolicyDiscoveryRepository:
    """Own bounded discovery state; approved policy rows remain in lisa.policies."""

    def __init__(self, dsn: str):
        self.dsn = dsn

    def _connect(self):
        return psycopg.connect(self.dsn, row_factory=dict_row)

    def reserve_sample(
        self, *, session_id: str, agent_name: str, activity_id: str, hourly_limit: int,
        now: datetime | None = None,
    ) -> bool:
        now = now or datetime.now(UTC)
        hour = now.replace(minute=0, second=0, microsecond=0)
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("lock_policy_discovery_sample_hour.sql"),
                               {"key": f"lisa-policy-discovery:{hour.isoformat()}"})
            existing = connection.execute(_sql("get_policy_discovery_sample.sql"),
                                          {"session_id": session_id}).fetchone()
            if existing:
                return True
            row = connection.execute(_sql("reserve_policy_discovery_sample.sql"), {
                "session_id": session_id,
                "agent_name": agent_name,
                "activity_id": activity_id,
                "sampled_hour": hour,
                "hourly_limit": hourly_limit,
            }).fetchone()
        return row is not None

    def has_signal(self, session_id: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(_sql("get_policy_discovery_signal.sql"),
                                     {"session_id": session_id}).fetchone()
        return row is not None

    def save_signal(
        self, *, session_id: str, activity_id: str, agent_name: str,
        summary: str, span_ids: list[str], evidence: list[dict[str, Any]]
    ) -> str | None:
        with self._connect() as connection, connection.transaction():
            sample = connection.execute(_sql("get_policy_discovery_sample.sql"), {
                "session_id": session_id,
            }).fetchone()
            if sample is None:
                raise ValueError("Policy discovery signals require a reserved sample")
            if (
                str(sample["activity_id"]) != str(activity_id)
                or sample["agent_name"] != agent_name
            ):
                raise ValueError("Policy discovery signal must match its reserved sample")
            row = connection.execute(_sql("insert_policy_discovery_signal.sql"), {
                "id": str(uuid4()), "session_id": session_id,
                "activity_id": activity_id, "agent_name": agent_name,
                "summary": summary, "span_ids": json.dumps(span_ids),
                "evidence": json.dumps(evidence),
            }).fetchone()
            if row is None:
                row = connection.execute(_sql("get_policy_discovery_signal.sql"), {
                    "session_id": session_id,
                }).fetchone()
            if row is None:
                raise RuntimeError("Policy discovery signal was not persisted")
            signal_id = str(row["id"])
            source_activity_id = str(row.get("activity_id") or activity_id)
            create_proactive_update(
                connection,
                source_type="qa_observation",
                source_id=signal_id,
                messages=[summary],
                activity_id=source_activity_id,
            )
        return signal_id

    def get_observation(self, observation_id: str) -> dict[str, Any] | None:
        try:
            identifier = str(UUID(observation_id))
        except ValueError:
            raise ValueError("Invalid QA observation ID") from None
        with self._connect() as connection:
            row = connection.execute(_sql("get_policy_discovery_signal_by_id.sql"), {
                "id": identifier,
            }).fetchone()
        if row is None:
            return None
        return {
            "id": str(row["id"]),
            "session_id": row["session_id"],
            "activity_id": str(row["activity_id"]),
            "agent_name": row["agent_name"],
            "summary": row["summary"],
            "span_ids": row["span_ids"],
            "evidence": row["evidence"],
            "created_at": row["created_at"].isoformat(),
        }

    def complete_sample_evaluation(self, session_id: str, *, gap_reviewed: bool) -> None:
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("complete_policy_discovery_evaluation.sql"), {
                "session_id": session_id, "gap_reviewed": gap_reviewed,
            })

    def pending_samples(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_pending_policy_discovery_samples.sql"), {"limit": limit}
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_sample_reviewed(self, session_id: str) -> None:
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("mark_policy_discovery_sample_reviewed.sql"), {
                "session_id": session_id,
            })

    def pending_signals(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_policy_discovery_signals.sql"), {"limit": limit}
            ).fetchall()
        return [dict(row) for row in rows]

    def reviewed_decisions(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_policy_discovery_decisions.sql"), {"limit": limit}
            ).fetchall()
        return [dict(row) for row in rows]

    def save_suggestion(
        self, *, draft, signal_rows: list[dict[str, Any]], overlaps: list[dict[str, Any]]
    ) -> dict[str, Any]:
        signal_ids = [str(row["id"]) for row in signal_rows]
        if len(signal_ids) < MIN_POLICY_SUGGESTION_ERROR_SAMPLES or not has_minimum_error_samples(signal_rows):
            raise ValueError("Policy suggestions require three distinct error samples")
        if set(signal_ids) != set(draft.signal_ids):
            raise ValueError("Policy suggestion evidence does not match its cited signals")
        candidate_identity = json.dumps([
            draft.scope, draft.target_agent_name, draft.name.casefold().strip(),
            " ".join(draft.criteria.casefold().split()),
        ], ensure_ascii=False, separators=(",", ":"))
        candidate_hash = sha256(candidate_identity.encode("utf-8")).hexdigest()
        evidence = [{
            "signal_id": str(row["id"]), "session_id": row["session_id"],
            "activity_id": str(row["activity_id"]), "agent_name": row["agent_name"],
            "summary": row["summary"], "span_ids": row["span_ids"],
            "evidence": row["evidence"],
            "created_at": row["created_at"].isoformat(),
        } for row in signal_rows]
        suggestion_id = str(uuid4())
        with self._connect() as connection, connection.transaction():
            row = connection.execute(_sql("insert_policy_suggestion.sql"), {
                "id": suggestion_id, "candidate_hash": candidate_hash,
                "name": draft.name.strip(), "criteria": draft.criteria.strip(),
                "scope": draft.scope, "target_agent_name": draft.target_agent_name.strip(),
                "rationale": draft.rationale.strip(), "signal_ids": json.dumps(signal_ids),
                "evidence": json.dumps(evidence), "overlap_details": json.dumps(overlaps),
            }).fetchone()
            if row is None:
                existing = connection.execute(
                    _sql("find_policy_suggestion_by_hash.sql"),
                    {"candidate_hash": candidate_hash},
                ).fetchone()
                suggestion_id = str(existing["id"])
            connection.execute(_sql("link_policy_discovery_signals.sql"), {
                "suggestion_id": suggestion_id, "signal_ids": signal_ids,
            })
            saved = connection.execute(
                _sql("get_policy_suggestion.sql"),
                {"id": suggestion_id},
            ).fetchone()
            create_proactive_update(
                connection,
                source_type="policy_suggestion",
                source_id=suggestion_id,
                messages=[draft.rationale],
            )
        return _public_suggestion(saved)

    def create_suggestions(self, *, drafts, signal_rows, overlaps_by_draft):
        signal_by_id = {str(row["id"]): row for row in signal_rows}
        created = []
        for draft, overlaps in zip(drafts, overlaps_by_draft, strict=True):
            if len(set(draft.signal_ids)) < MIN_POLICY_SUGGESTION_ERROR_SAMPLES:
                raise ValueError("Policy suggestions require three distinct error samples")
            rows = [signal_by_id[item] for item in draft.signal_ids]
            if not has_minimum_error_samples(rows):
                raise ValueError("Policy suggestions require three distinct error samples")
            created.append(self.save_suggestion(draft=draft, signal_rows=rows, overlaps=overlaps))
        return created

    def list_suggestions(self, *, status: str | None, limit: int) -> list[dict[str, Any]]:
        if status not in {None, "pending", "approved", "dismissed"} or not 1 <= limit <= 100:
            raise ValueError("Invalid policy suggestion filter")
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_policy_suggestions.sql"), {"status": status, "limit": limit}
            ).fetchall()
        return [_public_suggestion(row) for row in rows]

    def get_suggestion(self, suggestion_id: str) -> dict[str, Any] | None:
        try:
            identifier = str(UUID(suggestion_id))
        except ValueError:
            raise ValueError("Invalid policy suggestion ID") from None
        with self._connect() as connection:
            row = connection.execute(
                _sql("read_policy_suggestion.sql"), {"id": identifier}
            ).fetchone()
        return _public_suggestion(row) if row else None

    def claim_daily_synthesis(self) -> bool:
        with self._connect() as connection, connection.transaction():
            row = connection.execute(_sql("claim_policy_discovery_synthesis.sql")).fetchone()
        return row is not None

    def finish_daily_synthesis(self, signal_ids: list[str]) -> None:
        with self._connect() as connection, connection.transaction():
            if signal_ids:
                connection.execute(_sql("finish_policy_discovery_signal_batch.sql"), {
                    "signal_ids": signal_ids,
                })
            connection.execute(_sql("finish_policy_discovery_synthesis.sql"))

    def release_daily_synthesis(self) -> None:
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("release_policy_discovery_synthesis.sql"))

    def approve_suggestion(
        self, *, policy_service, suggestion_id: str, actor_id: str,
        name: str | None = None, criteria: str | None = None
    ) -> tuple[dict[str, Any], PolicyRecord]:
        identifier = str(UUID(suggestion_id))
        with self._connect() as connection, connection.transaction():
            suggestion = connection.execute(
                _sql("get_policy_suggestion.sql"), {"id": identifier}
            ).fetchone()
            if suggestion is None:
                raise ValueError("Policy suggestion was not found")
            if suggestion["status"] == "approved":
                policy = connection.execute(
                    _sql("get_policy_by_id.sql"), {"id": suggestion["policy_id"]}
                ).fetchone()
                if policy is None:
                    raise ValueError("The approved policy no longer exists")
                return _public_suggestion(suggestion), _policy_record(policy)
            if suggestion["status"] != "pending":
                raise ValueError("Only a pending policy suggestion can be approved")
            final_name = (name or suggestion["name"]).strip()
            final_criteria = (criteria or suggestion["criteria"]).strip()
            policy = policy_service.approve_discovery_suggestion(
                connection=connection, actor_id=actor_id, scope=suggestion["scope"],
                agent_name=suggestion["target_agent_name"], name=final_name,
                criteria=final_criteria,
            )
            marked = connection.execute(_sql("mark_policy_suggestion_approved.sql"), {
                "id": identifier, "policy_id": policy.id, "reviewed_by": actor_id,
                "name": policy.name, "criteria": policy.criteria,
            }).fetchone()
            if marked is None:
                raise ValueError("Policy suggestion changed while being approved")
        return self.get_suggestion(identifier), policy

    def dismiss_suggestion(self, *, suggestion_id: str, actor_id: str) -> dict[str, Any]:
        identifier = str(UUID(suggestion_id))
        with self._connect() as connection, connection.transaction():
            suggestion = connection.execute(
                _sql("get_policy_suggestion.sql"), {"id": identifier}
            ).fetchone()
            if suggestion is None:
                raise ValueError("Policy suggestion was not found")
            if suggestion["status"] == "dismissed":
                return _public_suggestion(suggestion)
            if suggestion["status"] != "pending":
                raise ValueError("Only a pending policy suggestion can be dismissed")
            marked = connection.execute(_sql("mark_policy_suggestion_dismissed.sql"), {
                "id": identifier, "reviewed_by": actor_id,
            }).fetchone()
            signal_ids = [str(item) for item in suggestion["signal_ids"]]
            connection.execute(_sql("dismiss_policy_suggestion_signals.sql"), {
                "signal_ids": signal_ids,
            })
            if marked is None:
                raise ValueError("Policy suggestion changed while being dismissed")
        return self.get_suggestion(identifier)


def _policy_record(row: dict[str, Any]) -> PolicyRecord:
    return PolicyRecord(
        id=row["id"], scope=row["scope"], target_agent_name=row["target_agent_name"],
        name=row["name"], criteria=row["criteria"], approved=row["approved"],
        created_at=row.get("created_at"), updated_at=row.get("updated_at"),
    )


def _public_suggestion(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]), "name": row["name"], "criteria": row["criteria"],
        "scope": row["scope"], "target_agent_name": row["target_agent_name"],
        "rationale": row["rationale"], "signal_ids": row["signal_ids"],
        "signal_count": len(row["signal_ids"]), "evidence": row["evidence"],
        "overlaps": row["overlap_details"], "status": row["status"],
        "policy_id": row["policy_id"],
        "created_at": row["created_at"].isoformat(),
        "reviewed_at": row["reviewed_at"].isoformat() if row["reviewed_at"] else None,
    }
