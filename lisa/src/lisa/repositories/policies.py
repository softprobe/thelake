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

from ..models.policies import PolicyRecord, policy_id_sort_key
from ..models.policies import policy_id as format_policy_id


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
            return list(rows.values())

    def list_rules(self, target_agent_name: str) -> list[PolicyRecord]:
        with self._lock:
            workspace = self._records.get(("workspace", ""), {})
            agent = self._records.get(("agent", target_agent_name), {})

            def effective(rows):
                grouped: dict[str, list[PolicyRecord]] = {}
                for row in rows.values():
                    if row.approved:
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
            ]

    def list_all_page(self, *, limit: int, offset: int) -> list[PolicyRecord]:
        if not 1 <= limit <= 101 or not 0 <= offset <= 10_000:
            raise ValueError("Policy page limit must be 1–101 and offset must be between 0 and 10000")
        with self._lock:
            rows = [
                row
                for records in self._records.values()
                for row in records.values()
            ]
            return sorted(rows, key=lambda row: policy_id_sort_key(row.id))[offset : offset + limit]

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
            current = list(records.values())
            if _policy_values(current) != _policy_values(expected_rules):
                raise ValueError("Policy changed since it was read; read it again before updating")
            incoming_names = {item["name"] for item in incoming}
            if any(row.name not in incoming_names for row in current):
                raise ValueError("Policy seeding cannot remove policy rows")
            for item in incoming:
                old = next((row for row in current if row.name == item["name"]), None)
                if old and (old.criteria, old.approved) == (item["criteria"], item["approved"]):
                    continue
                if old is None:
                    identifier = format_policy_id(self._next_policy_ordinal)
                    self._next_policy_ordinal += 1
                    row = PolicyRecord(
                        id=identifier,
                        scope=scope,
                        target_agent_name=target_agent_name or None,
                        name=item["name"],
                        criteria=item["criteria"],
                        approved=item["approved"],
                    )
                else:
                    identifier = old.id
                    changed = (old.name, old.criteria, old.approved) != (
                        item["name"], item["criteria"], item["approved"]
                    )
                    row = replace(
                        old,
                        name=item["name"],
                        criteria=item["criteria"],
                        approved=item["approved"],
                        updated_at=datetime.now(UTC) if changed else old.updated_at,
                    )
                records[identifier] = row
            return list(records.values())

    def write_policy(self, scope: str, target_agent_name: str, name: str, criteria: str,
                     policy_id: str | None = None) -> PolicyRecord:
        key = self._key(scope, target_agent_name)
        with self._lock:
            records = self._records.setdefault(key, {})
            if policy_id is not None:
                selected = records.get(policy_id)
                if selected is None:
                    raise ValueError("Policy ID was not found in this scope")
                changed = (selected.name, selected.criteria) != (name, criteria)
                row = replace(
                    selected,
                    name=name,
                    criteria=criteria,
                    approved=False if changed else selected.approved,
                    updated_at=datetime.now(UTC) if changed else selected.updated_at,
                )
                if any(item.id != selected.id and item.name == name for item in records.values()):
                    raise ValueError("A policy with this name already exists in this scope")
                records[selected.id] = row
                return row
            if any(row.name == name for row in records.values()):
                raise ValueError("A policy with this name already exists in this scope; update its ID")
            identifier = format_policy_id(self._next_policy_ordinal)
            self._next_policy_ordinal += 1
            row = PolicyRecord(identifier, scope, target_agent_name or None, name, criteria, False)
            records[identifier] = row
            return row

    def approve_policy(self, identifier: str) -> PolicyRecord:
        with self._lock:
            for records in self._records.values():
                selected = records.get(identifier)
                if selected is None:
                    continue
                if selected.approved:
                    return selected
                approved = replace(selected, approved=True, updated_at=datetime.now(UTC))
                records[identifier] = approved
                return approved
        raise ValueError("Policy ID was not found")

    def delete_policy(self, identifier: str) -> None:
        with self._lock:
            for records in self._records.values():
                if identifier in records:
                    del records[identifier]
                    return
        raise ValueError("Policy ID was not found")


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
        )

    def list_scope(self, scope: str, target_agent_name: str = "") -> list[PolicyRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                _sql("list_policy_scope.sql"),
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
            incoming_names = {item["name"] for item in incoming}
            if any(row.name not in incoming_names for row in current):
                raise ValueError("Policy seeding cannot remove policy rows")
            current_by_name = {item.name: item for item in current}
            for item in incoming:
                old = current_by_name.get(item["name"])
                if old and (old.criteria, old.approved) == (item["criteria"], item["approved"]):
                    continue
                if old is None:
                    connection.execute(_sql("lock_policy_id_allocation.sql"))
                    ordinal = connection.execute(_sql("next_policy_ordinal.sql")).fetchone()["ordinal"]
                    identifier = format_policy_id(ordinal)
                else:
                    identifier = old.id
                    if (old.name, old.criteria, old.approved) != (
                        item["name"], item["criteria"], item["approved"]
                    ):
                        connection.execute(
                            _sql("invalidate_policy_triage.sql"), {"policy_id": identifier}
                        )
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
            rows = connection.execute(
                _sql("list_policy_scope.sql"),
                {"scope": scope, "target_agent_name": target_agent_name or ""},
            ).fetchall()
            if policy_id is not None:
                selected = connection.execute(
                    _sql("get_policy_by_id.sql"), {"id": policy_id}
                ).fetchone()
                if (
                    selected is None
                    or selected["scope"] != scope
                    or selected["target_agent_name"] != (target_agent_name or "")
                ):
                    raise ValueError("Policy ID was not found in this scope")
                changed = selected["name"] != name or selected["criteria"] != criteria
                row = connection.execute(_sql("write_policy.sql"), {
                    "id": policy_id,
                    "scope": scope,
                    "target_agent_name": target_agent_name or "",
                    "name": name,
                    "criteria": criteria,
                }).fetchone()
                if row is None:
                    raise ValueError("Policy changed while writing; read it again before updating")
                if changed:
                    connection.execute(
                        _sql("invalidate_policy_triage.sql"), {"policy_id": policy_id}
                    )
            else:
                if any(row["name"] == name for row in rows):
                    raise ValueError("A policy with this name already exists in this scope; update its ID")
                connection.execute(_sql("lock_policy_id_allocation.sql"))
                ordinal = connection.execute(_sql("next_policy_ordinal.sql")).fetchone()["ordinal"]
                row = connection.execute(_sql("create_policy.sql"), {
                    "id": format_policy_id(ordinal),
                    "scope": scope,
                    "target_agent_name": target_agent_name or "",
                    "name": name,
                    "criteria": criteria,
                }).fetchone()
        return self._record(row)

    def approve_policy(self, identifier: str) -> PolicyRecord:
        with self._connect() as connection, connection.transaction():
            selected = connection.execute(_sql("get_policy_by_id.sql"), {"id": identifier}).fetchone()
            if selected is None:
                raise ValueError("Policy ID was not found")
            lock_key = f"{selected['scope']}:{selected['target_agent_name'] if selected['scope'] == 'agent' else ''}"
            connection.execute(_sql("lock_policy_scope.sql"), {"lock_key": lock_key})
            selected = connection.execute(_sql("get_policy_by_id.sql"), {"id": identifier}).fetchone()
            if selected is None:
                raise ValueError("Policy ID was not found")
            if not selected["approved"]:
                selected = connection.execute(_sql("approve_policy.sql"), {"id": identifier}).fetchone()
                if selected is None:
                    raise ValueError("Policy changed while approving; read it again before retrying")
        return self._record(selected)

    def delete_policy(self, identifier: str) -> None:
        with self._connect() as connection, connection.transaction():
            selected = connection.execute(
                _sql("get_policy_by_id.sql"), {"id": identifier}
            ).fetchone()
            if selected is None:
                raise ValueError("Policy ID was not found")
            lock_key = f"{selected['scope']}:{selected['target_agent_name'] if selected['scope'] == 'agent' else ''}"
            connection.execute(_sql("lock_policy_scope.sql"), {"lock_key": lock_key})
            selected = connection.execute(
                _sql("lock_policy_for_update.sql"), {"id": identifier}
            ).fetchone()
            if selected is None:
                raise ValueError("Policy ID was not found")
            if connection.execute(
                _sql("policy_has_evaluation_history.sql"), {"policy_id": identifier}
            ).fetchone()["has_history"]:
                raise ValueError("Policy has evaluation history and cannot be deleted")
            connection.execute(_sql("invalidate_policy_triage.sql"), {"policy_id": identifier})
            deleted = connection.execute(
                _sql("delete_policy.sql"),
                {
                    "id": identifier,
                    "scope": selected["scope"],
                    "target_agent_name": selected["target_agent_name"],
                },
            ).fetchone()
            if deleted is None:
                raise ValueError("Policy changed while deleting; read it again before retrying")


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
