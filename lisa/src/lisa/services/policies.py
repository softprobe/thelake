"""Policy rules, approval checks, and the typed database-facing policy API."""

from __future__ import annotations

from ..context import WorkspaceContext
from ..models.policies import PolicyRecord, PolicyRule, is_policy_id, policy_id_sort_key
from ..security import redact_text


class PolicyService:
    """Apply policy lifecycle rules and expose policy operations to controllers."""

    def __init__(self, context: WorkspaceContext, repository):
        self.context = context
        self.repository = repository

    @staticmethod
    def _record_rule(record: PolicyRecord) -> PolicyRule:
        return PolicyRule(
            record.name,
            record.criteria,
            record.approved,
            record.scope,
            record.id,
        )

    def approved_rules(self, target_agent_name: str) -> list[PolicyRule]:
        return [self._record_rule(row) for row in self.repository.list_rules(target_agent_name)]

    @staticmethod
    def _scope(scope: str, agent_name: str) -> str:
        if scope == "workspace":
            return ""
        if scope == "agent" and agent_name.strip():
            return agent_name.strip()
        raise ValueError("Use workspace scope or provide the exact agent name for agent scope")

    def list_policy_records(self) -> list[PolicyRecord]:
        """Return active policy rows; the controller chooses their presentation."""
        return sorted(self.repository.list_all(), key=lambda row: policy_id_sort_key(row.id))

    def list_policy_records_page(
        self, *, limit: int = 100, offset: int = 0
    ) -> tuple[list[PolicyRecord], int | None]:
        """Return one bounded page and the offset of its successor, if present."""
        if not 1 <= limit <= 100 or not 0 <= offset <= 10_000:
            raise ValueError("Policy page limit must be 1–100 and offset must be between 0 and 10000")
        rows = self.repository.list_all_page(limit=limit + 1, offset=offset)
        page = rows[:limit]
        candidate_offset = offset + limit
        next_offset = (
            candidate_offset
            if len(rows) > limit and candidate_offset <= 10_000
            else None
        )
        return page, next_offset

    def read_policy_records(
        self, scope: str = "workspace", agent_name: str = ""
    ) -> list[PolicyRecord]:
        """Read workspace policy by default, or one agent's rules by exact name."""
        target_agent_name = self._scope(scope, agent_name)
        return sorted(
            self.repository.list_scope(scope, target_agent_name),
            key=lambda row: policy_id_sort_key(row.id),
        )

    def write_policy(
        self, *, scope: str, agent_name: str, name: str, criteria: str,
        policy_id: str | None = None,
    ) -> PolicyRecord:
        if self.context.turn_source not in {"web", "slack"}:
            raise ValueError("Only an owner conversation can change policies")
        if not self.context.actor_id or self.context.actor_id not in self.context.owner_user_ids:
            raise ValueError("This action requires an authorized Lisa owner identity")
        target = self._scope(scope, agent_name)
        name = redact_text(name).strip()
        criteria = redact_text(criteria).strip()
        if not name or len(name) > 200 or not criteria or len(criteria) > 20_000:
            raise ValueError("Policy name and criteria must be non-empty and within size limits")
        if policy_id and not is_policy_id(policy_id):
            raise ValueError("Use a human-readable policy ID such as P1.2 or P1.2.1")
        return self.repository.write_policy(scope, target, name, criteria, policy_id)

    def approve_policy(self, policy_id: str) -> PolicyRecord:
        if self.context.turn_source not in {"web", "slack"}:
            raise ValueError("Only an owner conversation can approve policies")
        if not self.context.actor_id or self.context.actor_id not in self.context.owner_user_ids:
            raise ValueError("This action requires an authorized Lisa owner identity")
        if not is_policy_id(policy_id):
            raise ValueError("Use a human-readable policy ID such as P1.2 or P1.2.1")
        if self.context.confirmed_policy_id != policy_id:
            raise ValueError(f"The owner must send exactly: Approve policy: {policy_id}")
        return self.repository.approve_policy(policy_id)

    def delete_policy(self, policy_id: str) -> None:
        if self.context.turn_source not in {"web", "slack"}:
            raise ValueError("Only an owner conversation can delete policies")
        if not self.context.actor_id or self.context.actor_id not in self.context.owner_user_ids:
            raise ValueError("This action requires an authorized Lisa owner identity")
        if not is_policy_id(policy_id):
            raise ValueError("Use a human-readable policy ID such as P1.2 or P1.2.1")
        if self.context.confirmed_policy_deletion_id != policy_id:
            raise ValueError(f"The owner must send exactly: Delete policy: {policy_id}")
        self.repository.delete_policy(policy_id)
