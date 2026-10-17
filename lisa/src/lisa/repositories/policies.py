"""Typed PostgreSQL persistence for Lisa's policy rules."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from importlib.resources import files
from threading import RLock
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from ..models.policies import PolicyRecord, policy_id


def _sql(name: str) -> str:
    return files("lisa").joinpath("sql", "policies", name).read_text(encoding="utf-8")


class InMemoryPolicyRepository:
    """Process-local fallback used only when a deployment has no database configured."""

    def __init__(self):
        self._records: dict[tuple[str, str], dict[str, PolicyRecord]] = {}
        self._lock = RLock()
        self._next_policy_ordinal = 1

    @staticmethod
    def _key(scope: str, target_agent_name: str) -> tuple[str, str]:
        return scope, target_agent_name if scope == "agent" else ""

    def list_scope(self, scope: str, target_agent_name: str = "") -> list[PolicyRecord]:
        with self._lock:
            rows = self._records.get(self._key(scope, target_agent_name), {})
            return [row for row in rows.values() if row.archived_at is None]

    def list_scope_history(self, scope: str, target_agent_name: str = "") -> list[PolicyRecord]:
        with self._lock:
            rows = self._records.get(self._key(scope, target_agent_name), {})
            return list(rows.values())

    def list_rules(self, target_agent_name: str) -> list[PolicyRecord]:
        with self._lock:
            workspace = self._records.get(("workspace", ""), {})
            agent = self._records.get(("agent", target_agent_name), {})
            overlaid = {row.name: row for row in workspace.values() if row.archived_at is None}
            overlaid.update({row.name: row for row in agent.values() if row.archived_at is None})
            return [row for row in overlaid.values() if row.approved]

    def list_all(self) -> list[PolicyRecord]:
        with self._lock:
            return [
                row
                for rows in self._records.values()
                for row in rows.values()
                if row.archived_at is None
            ]

    def list_all_history(self) -> list[PolicyRecord]:
        with self._lock:
            return [row for rows in self._records.values() for row in rows.values()]

    def replace_scope(
        self,
        scope: str,
        target_agent_name: str,
        expected_rules: list[dict[str, Any]],
        incoming: list[dict[str, Any]],
    ) -> list[PolicyRecord]:
        key = self._key(scope, target_agent_name)
        with self._lock:
            current = list(self._records.get(key, {}).values())
            current = [row for row in current if row.archived_at is None]
            if _policy_values(current) != _policy_values(expected_rules):
                raise ValueError("Policy changed since it was read; read it again before updating")
            previous = {row.name: row for row in self._records.get(key, {}).values()}
            next_records = {
                row.name: row for row in previous.values() if row.archived_at is not None
            }
            archived_at = datetime.now(UTC)
            incoming_names = {item["name"] for item in incoming}
            for row in current:
                if row.name not in incoming_names:
                    next_records[row.name] = replace(row, archived_at=archived_at)
            for item in incoming:
                old = previous.get(item["name"])
                if old is None:
                    identifier = policy_id(self._next_policy_ordinal)
                    self._next_policy_ordinal += 1
                else:
                    identifier = old.id
                row = PolicyRecord(
                    id=identifier,
                    scope=scope,
                    target_agent_name=target_agent_name or None,
                    name=item["name"],
                    criteria=item["criteria"],
                    approved=item["approved"],
                )
                next_records[row.name] = row
            self._records[key] = next_records
            return list(next_records.values())


class PostgresPolicyRepository:
    """Typed policy operations over Lisa's deployment-bound Postgres schema."""

    def __init__(self, dsn: str, workspace_id: str):
        self.dsn = dsn
        self.workspace_id = str(UUID(workspace_id))
        self.workspace_fingerprint = sha256(self.workspace_id.encode("utf-8")).hexdigest()
        with self._connect() as connection:
            binding = connection.execute(_sql("get_deployment.sql")).fetchone()
        if not binding or binding["workspace_fingerprint"] != self.workspace_fingerprint:
            raise RuntimeError("Lisa policy database is not bound to the configured workspace")

    def _connect(self):
        return psycopg.connect(self.dsn, row_factory=dict_row)

    @staticmethod
    def _record(row: dict[str, Any]) -> PolicyRecord:
        return PolicyRecord(
            id=row["id"],
            scope=row["scope"],
            target_agent_name=row["target_agent_name"],
            name=row["name"],
            criteria=row["criteria"],
            approved=row["approved"],
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
            archived_at=row.get("archived_at"),
        )

    def list_scope(self, scope: str, target_agent_name: str = "") -> list[PolicyRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_policy_scope.sql"),
                {"scope": scope, "target_agent_name": target_agent_name or ""},
            ).fetchall()
        return [self._record(row) for row in rows]

    def list_scope_history(self, scope: str, target_agent_name: str = "") -> list[PolicyRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_policy_scope_history.sql"),
                {"scope": scope, "target_agent_name": target_agent_name or ""},
            ).fetchall()
        return [self._record(row) for row in rows]

    def list_rules(self, target_agent_name: str) -> list[PolicyRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_applicable_policies.sql"),
                {"target_agent_name": target_agent_name},
            ).fetchall()
        return [self._record(row) for row in rows]

    def list_all(self) -> list[PolicyRecord]:
        with self._connect() as connection:
            rows = connection.execute(_sql("list_all_policies.sql")).fetchall()
        return [self._record(row) for row in rows]

    def list_all_history(self) -> list[PolicyRecord]:
        with self._connect() as connection:
            rows = connection.execute(_sql("list_all_policy_history.sql")).fetchall()
        return [self._record(row) for row in rows]

    def replace_scope(
        self,
        scope: str,
        target_agent_name: str,
        expected_rules: list[dict[str, Any]],
        incoming: list[dict[str, Any]],
    ) -> list[PolicyRecord]:
        lock_key = f"{scope}:{target_agent_name if scope == 'agent' else ''}"
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("lock_policy_scope.sql"), {"lock_key": lock_key})
            rows = connection.execute(
                _sql("list_policy_scope.sql"),
                {"scope": scope, "target_agent_name": target_agent_name or ""},
            ).fetchall()
            current = [self._record(row) for row in rows]
            if _policy_values(current) != _policy_values(expected_rules):
                raise ValueError("Policy changed since it was read; read it again before updating")
            incoming_names = [item["name"] for item in incoming]
            connection.execute(
                _sql("archive_removed_policies.sql"),
                {
                    "scope": scope,
                    "target_agent_name": target_agent_name or "",
                    "names": incoming_names,
                },
            )
            for item in incoming:
                connection.execute(
                    _sql("save_policy.sql"),
                    {
                        "scope": scope,
                        "target_agent_name": target_agent_name or "",
                        "name": item["name"],
                        "criteria": item["criteria"],
                        "approved": item["approved"],
                    },
                )
            saved = connection.execute(
                _sql("list_policy_scope.sql"),
                {"scope": scope, "target_agent_name": target_agent_name or ""},
            ).fetchall()
        return [self._record(row) for row in saved]


def _policy_values(
    records: list[PolicyRecord] | list[dict[str, Any]],
) -> set[tuple[str, str, bool]]:
    """Compare persisted rule content without depending on a display format."""
    return {
        (record.name, record.criteria, record.approved)
        if isinstance(record, PolicyRecord)
        else (record["name"], record["criteria"], record["approved"])
        for record in records
    }
