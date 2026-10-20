"""Policy rules, approval checks, and the typed database-facing policy API."""

from __future__ import annotations

import re

from ..context import WorkspaceContext
from ..models.policies import PolicyRecord, PolicyRule, policy_id_sort_key
from ..security import redact_text


class PolicyService:
    """Apply policy lifecycle rules and expose policy operations to controllers."""

    def __init__(self, context: WorkspaceContext, repository):
        self.context = context
        self.repository = repository

    @staticmethod
    def _sections(markdown: str) -> list[tuple[str, list[str]]]:
        sections: list[tuple[str, list[str]]] = []
        fence = None
        for line in markdown.splitlines():
            marker = re.match(r"^\s{0,3}(`{3,}|~{3,})", line)
            if marker:
                token = marker.group(1)
                if fence is None:
                    fence = token
                elif token[0] == fence[0] and len(token) >= len(fence):
                    fence = None
                if sections:
                    sections[-1][1].append(line)
                continue
            heading = re.fullmatch(r"##\s+(.+?)\s*", line) if fence is None else None
            if heading:
                title = heading.group(1)
                article_heading = re.fullmatch(r"P1\.[1-9][0-9]*\s+\|\s+(.+)", title)
                sections.append((article_heading.group(1) if article_heading else title, []))
            elif sections:
                sections[-1][1].append(line)
        return sections

    def parse_rules(self, markdown: str, scope: str) -> dict[str, PolicyRule]:
        rules = {}
        for title, lines in self._sections(markdown):
            body = "\n".join(lines).strip()
            content_lines = body.splitlines()
            approved = False
            if content_lines and content_lines[0].lower().startswith("approved:"):
                match = re.fullmatch(r"approved:\s*(yes|no)", content_lines.pop(0), re.IGNORECASE)
                if not match:
                    raise ValueError("Each rule's approved field must be yes or no")
                approved = match.group(1).lower() == "yes"
                remaining = "\n".join(content_lines).strip()
                if remaining.lower().startswith("approved:"):
                    raise ValueError("Each rule must have only one approved field")
            criteria = "\n".join(content_lines).strip()
            if not criteria:
                raise ValueError("Each named policy rule must have readable Markdown content")
            if title in rules:
                raise ValueError("Policy rule names must be unique within a policy scope")
            rules[title] = PolicyRule(title, criteria, approved, scope)
        return rules

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

    @classmethod
    def rules(cls, markdown: str) -> dict[str, str]:
        return {
            title: "\n".join([f"## {title}", *lines]).strip()
            for title, lines in cls._sections(markdown)
        }

    def _validate_policy_markdown(self, markdown: str) -> dict[str, PolicyRule]:
        sections = self._sections(markdown)
        if not sections:
            raise ValueError("Policy Markdown must contain at least one '##' rule section")
        titles = [title.strip().casefold() for title, _ in sections]
        if len(titles) != len(set(titles)):
            raise ValueError("Policy rule names must be unique within a policy scope")
        parsed = self.parse_rules(markdown, "workspace")
        for title, lines in sections:
            if not title.strip() or not "\n".join(lines).strip():
                raise ValueError("Each named policy rule must have readable Markdown content")
        return parsed

    def check_changes(
        self, content: str, current: str, confirmed_title: str | None = None,
    ) -> None:
        before = self.rules(current)
        after = self.rules(content)
        changed_titles = {
            title for title in before.keys() | after.keys() if before.get(title) != after.get(title)
        }
        if not changed_titles:
            return
        if self.context.turn_source not in {"web", "slack"}:
            raise ValueError("Only an owner conversation can save policy changes")
        confirmed_title = confirmed_title or self.context.confirmed_policy_title
        if not confirmed_title:
            raise ValueError(
                "Propose the named policy in chat and wait for the owner's explicit confirmation"
            )
        if changed_titles != {confirmed_title}:
            raise ValueError("Only the exact policy name the owner confirmed this turn may change")

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

    def update_policy(
        self,
        content: str,
        expected_content: str,
        scope: str = "workspace",
        agent_name: str = "",
        confirmed_title: str | None = None,
    ) -> int:
        """Replace a policy scope and return the number of saved rules."""
        content = redact_text(content)
        expected_content = redact_text(expected_content)
        if not content.strip() or len(content) > 20_000:
            raise ValueError("Policy Markdown must contain 1 to 20000 characters")
        parsed = self._validate_policy_markdown(content)
        target_agent_name = self._scope(scope, agent_name)
        current_rules = self.repository.list_scope(scope, target_agent_name)
        expected_parsed = (
            {}
            if expected_content.strip() in {"", "No policy exists yet."}
            else self.parse_rules(expected_content, scope)
        )
        current_values = {row.name: (row.criteria, row.approved) for row in current_rules}
        expected_values = {
            rule.name: (rule.criteria, rule.approved) for rule in expected_parsed.values()
        }
        if current_values != expected_values:
            raise ValueError("Policy changed since it was read; read it again before updating")
        if self.rules(content) != self.rules(expected_content) and self.context.turn_source not in {
            "web",
            "slack",
        }:
            raise ValueError("Only an owner conversation can save policy changes")
        self.check_changes(content, expected_content, confirmed_title)
        expected_rules = [
            {"name": rule.name, "criteria": rule.criteria, "approved": rule.approved}
            for rule in expected_parsed.values()
        ]
        incoming = [
            {"name": rule.name, "criteria": rule.criteria, "approved": rule.approved}
            for rule in parsed.values()
        ]
        saved = self.repository.replace_scope(
            scope,
            target_agent_name,
            expected_rules,
            incoming,
        )
        return len(saved)
