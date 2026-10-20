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

from ..models.policies import PolicyRecord, policy_id, policy_id_sort_key


def _sql(name: str) -> str:
    return files("lisa").joinpath("sql", "policies", name).read_text(encoding="utf-8")


def _next_revision_id(records: list[PolicyRecord], identifier: str) -> str:
    """Allocate the next readable revision within a policy's P1.x family."""
    family = ".".join(identifier.split(".")[:2])
    revisions = [
        int(row.id.rsplit(".", 1)[-1])
        for row in records
        if row.id.startswith(family + ".") and row.id != family
    ]
    return f"{family}.{max(revisions, default=0) + 1}"


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

            def effective(rows):
                grouped: dict[str, list[PolicyRecord]] = {}
                for row in rows.values():
                    if row.archived_at is None and row.approved:
                        grouped.setdefault(row.name, []).append(row)
                return {
                    name: max(
                        items,
                        key=lambda row: (
                            row.updated_at or row.created_at or datetime.min.replace(tzinfo=UTC),
                            row.id,
                        ),
                    )
                    for name, items in grouped.items()
                }
            overlaid = effective(workspace)
            overlaid.update(effective(agent))
            return list(overlaid.values())

    def list_all(self) -> list[PolicyRecord]:
        with self._lock:
            return [
                row
                for rows in self._records.values()
                for row in rows.values()
                if row.archived_at is None
            ]

    def list_all_page(self, *, limit: int, offset: int) -> list[PolicyRecord]:
        if not 1 <= limit <= 101 or not 0 <= offset <= 10_000:
            raise ValueError("Policy page limit must be 1–101 and offset must be between 0 and 10000")
        with self._lock:
            rows = [
                row
                for records in self._records.values()
                for row in records.values()
                if row.archived_at is None
            ]
            return sorted(rows, key=lambda row: policy_id_sort_key(row.id))[offset : offset + limit]

    def list_all_history(self) -> list[PolicyRecord]:
        with self._lock:
            return [row for rows in self._records.values() for row in rows.values()]

    def seed_scope(
        self,
        scope: str,
        target_agent_name: str,
        expected_rules: list[dict[str, Any]],
        incoming: list[dict[str, Any]],
    ) -> list[PolicyRecord]:
        key = self._key(scope, target_agent_name)
        with self._lock:
            records = self._records.setdefault(key, {})
            current = [row for row in records.values() if row.archived_at is None]
            if _policy_values(current) != _policy_values(expected_rules):
                raise ValueError("Policy changed since it was read; read it again before updating")
            archived_at = datetime.now(UTC)
            incoming_names = {item["name"] for item in incoming}
            for row in current:
                if row.name not in incoming_names:
                    records[row.id] = replace(row, archived_at=archived_at)
            for item in incoming:
                old = next((row for row in current if row.name == item["name"]), None)
                if old and (old.criteria, old.approved) == (item["criteria"], item["approved"]):
                    continue
                if old is None:
                    identifier = policy_id(self._next_policy_ordinal)
                    self._next_policy_ordinal += 1
                elif not old.approved:
                    identifier = old.id
                else:
                    identifier = _next_revision_id(list(records.values()), old.id)
                    if item["approved"]:
                        records[old.id] = replace(old, archived_at=archived_at)
                if old and not old.approved and item["approved"]:
                    for existing_id, existing in list(records.items()):
                        if (
                            existing.name == old.name
                            and existing.approved
                            and existing.archived_at is None
                        ):
                            records[existing_id] = replace(existing, archived_at=archived_at)
                row = PolicyRecord(
                    id=identifier,
                    scope=scope,
                    target_agent_name=target_agent_name or None,
                    name=item["name"],
                    criteria=item["criteria"],
                    approved=item["approved"],
                )
                records[identifier] = row
            return [row for row in records.values() if row.archived_at is None]

    def write_policy(self, scope: str, target_agent_name: str, name: str, criteria: str,
                     policy_id: str | None = None) -> PolicyRecord:
        key = self._key(scope, target_agent_name)
        with self._lock:
            records = self._records.setdefault(key, {})
            active = [row for row in records.values() if row.archived_at is None and row.name == name]
            pending = next((row for row in active if not row.approved), None)
            approved = next((row for row in active if row.approved), None)
            if policy_id is not None:
                selected = records.get(policy_id)
                if selected is None or selected.archived_at is not None or selected.name != name:
                    raise ValueError("Policy ID was not found in this scope")
                if not selected.approved:
                    identifier = selected.id
                elif pending is not None:
                    identifier = pending.id
                else:
                    identifier = _next_revision_id(list(records.values()), selected.id)
            elif pending is not None:
                identifier = pending.id
            elif approved is not None:
                identifier = _next_revision_id(list(records.values()), approved.id)
            else:
                identifier = f"P1.{self._next_policy_ordinal}"
                self._next_policy_ordinal += 1
            row = PolicyRecord(identifier, scope, target_agent_name or None, name, criteria, False)
            records[identifier] = row
            return row

    def approve_policy(self, identifier: str) -> PolicyRecord:
        with self._lock:
            for records in self._records.values():
                selected = records.get(identifier)
                if selected is None or selected.archived_at is not None:
                    continue
                if selected.approved:
                    return selected
                now = datetime.now(UTC)
                for policy_id, row in list(records.items()):
                    if row.name == selected.name and row.approved and row.archived_at is None:
                        records[policy_id] = replace(row, archived_at=now)
                approved = replace(selected, approved=True)
                records[identifier] = approved
                return approved
        raise ValueError("Policy ID was not found or is archived")


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

    def list_all_page(self, *, limit: int, offset: int) -> list[PolicyRecord]:
        if not 1 <= limit <= 101 or not 0 <= offset <= 10_000:
            raise ValueError("Policy page limit must be 1–101 and offset must be between 0 and 10000")
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_all_policies_page.sql"), {"limit": limit, "offset": offset}
            ).fetchall()
        return [self._record(row) for row in rows]

    def list_all_history(self) -> list[PolicyRecord]:
        with self._connect() as connection:
            rows = connection.execute(_sql("list_all_policy_history.sql")).fetchall()
        return [self._record(row) for row in rows]

    def seed_scope(
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
            history_rows = connection.execute(
                _sql("list_policy_scope_history.sql"),
                {"scope": scope, "target_agent_name": target_agent_name or ""},
            ).fetchall()
            history = [self._record(row) for row in history_rows]
            incoming_names = [item["name"] for item in incoming]
            connection.execute(
                _sql("archive_removed_policies.sql"),
                {
                    "scope": scope,
                    "target_agent_name": target_agent_name or "",
                    "names": incoming_names,
                },
            )
            current_by_name = {item.name: item for item in current}
            for item in incoming:
                old = current_by_name.get(item["name"])
                if old and (old.criteria, old.approved) == (item["criteria"], item["approved"]):
                    continue
                if old is None:
                    ordinal = connection.execute(_sql("next_policy_ordinal.sql")).fetchone()["ordinal"]
                    identifier = policy_id(ordinal)
                elif not old.approved:
                    identifier = old.id
                else:
                    identifier = _next_revision_id(history, old.id)
                    if item["approved"]:
                        connection.execute(_sql("archive_approved_policy.sql"), {
                            "scope": scope,
                            "target_agent_name": target_agent_name or "",
                            "name": item["name"],
                            "id": identifier,
                        })
                if old and not old.approved and item["approved"]:
                    connection.execute(_sql("archive_approved_policy.sql"), {
                        "scope": scope,
                        "target_agent_name": target_agent_name or "",
                        "name": item["name"],
                        "id": identifier,
                    })
                connection.execute(
                    _sql("save_policy.sql"),
                    {
                        "id": identifier,
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

    def write_policy(self, scope: str, target_agent_name: str, name: str, criteria: str,
                     policy_id: str | None = None) -> PolicyRecord:
        lock_key = f"{scope}:{target_agent_name if scope == 'agent' else ''}"
        with self._connect() as connection, connection.transaction():
            connection.execute(_sql("lock_policy_scope.sql"), {"lock_key": lock_key})
            rows = connection.execute(_sql("list_policy_scope_history.sql"), {
                "scope": scope, "target_agent_name": target_agent_name or "",
            }).fetchall()
            history = [self._record(row) for row in rows]
            active = [row for row in history if row.archived_at is None and row.name == name]
            pending = next((row for row in active if not row.approved), None)
            approved = next((row for row in active if row.approved), None)
            if policy_id is not None:
                selected = next((row for row in history if row.id == policy_id and row.archived_at is None), None)
                if selected is None or selected.name != name:
                    raise ValueError("Policy ID was not found in this scope")
                if not selected.approved:
                    identifier = selected.id
                elif pending is not None:
                    identifier = pending.id
                else:
                    identifier = _next_revision_id(history, selected.id)
            elif pending is not None:
                identifier = pending.id
            elif approved is not None:
                identifier = _next_revision_id(history, approved.id)
            else:
                ordinal = connection.execute(_sql("next_policy_ordinal.sql")).fetchone()["ordinal"]
                identifier = f"P1.{ordinal}"
            row = connection.execute(_sql("write_policy.sql"), {
                "id": identifier, "scope": scope,
                "target_agent_name": target_agent_name or "", "name": name,
                "criteria": criteria,
            }).fetchone()
            if row is None:
                raise ValueError("Policy changed while writing; read it again before updating")
        return self._record(row)

    def approve_policy(self, identifier: str) -> PolicyRecord:
        with self._connect() as connection, connection.transaction():
            selected = connection.execute(_sql("get_policy_by_id.sql"), {"id": identifier}).fetchone()
            if selected is None or selected["archived_at"] is not None:
                raise ValueError("Policy ID was not found or is archived")
            lock_key = f"{selected['scope']}:{selected['target_agent_name'] if selected['scope'] == 'agent' else ''}"
            connection.execute(_sql("lock_policy_scope.sql"), {"lock_key": lock_key})
            selected = connection.execute(_sql("get_policy_by_id.sql"), {"id": identifier}).fetchone()
            if selected is None or selected["archived_at"] is not None:
                raise ValueError("Policy ID was not found or is archived")
            if not selected["approved"]:
                connection.execute(_sql("archive_approved_policy.sql"), {
                    "scope": selected["scope"],
                    "target_agent_name": selected["target_agent_name"],
                    "name": selected["name"], "id": identifier,
                })
                selected = connection.execute(_sql("approve_policy.sql"), {"id": identifier}).fetchone()
                if selected is None:
                    raise ValueError("Policy changed while approving; read it again before retrying")
        return self._record(selected)


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
