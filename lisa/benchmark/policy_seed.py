"""Fixture-only policy catalog seeding for run-owned benchmark databases."""

from __future__ import annotations

import re

from lisa.security import redact_text


def _sections(markdown: str) -> list[dict[str, object]]:
    sections: list[dict[str, object]] = []
    fence: str | None = None
    for line in markdown.splitlines():
        marker = re.match(r"^\s{0,3}(`{3,}|~{3,})", line)
        if marker:
            token = marker.group(1)
            if fence is None:
                fence = token
            elif token[0] == fence[0] and len(token) >= len(fence):
                fence = None
            if sections:
                sections[-1]["lines"].append(line)
            continue
        heading = re.fullmatch(r"##\s+(.+?)\s*", line) if fence is None else None
        if heading:
            title = re.sub(
                r"^P1\.[1-9][0-9]*(?:\.[1-9][0-9]*)?\s+\|\s+", "", heading.group(1)
            )
            sections.append({"name": title, "lines": []})
        elif sections:
            sections[-1]["lines"].append(line)
    return sections


def _policy_rows(markdown: str) -> list[dict[str, object]]:
    rows = []
    names: set[str] = set()
    for section in _sections(markdown):
        name = str(section["name"]).strip()
        lines = list(section["lines"])
        if not name or name.casefold() in names:
            raise ValueError("Policy seed names must be non-empty and unique")
        names.add(name.casefold())
        approved = False
        if lines and str(lines[0]).strip().lower().startswith("approved:"):
            match = re.fullmatch(r"\s*approved:\s*(yes|no)\s*", str(lines.pop(0)), re.IGNORECASE)
            if not match:
                raise ValueError("Each seeded policy's approved field must be yes or no")
            approved = match.group(1).lower() == "yes"
            if "\n".join(str(line) for line in lines).strip().lower().startswith("approved:"):
                raise ValueError("Each seeded policy may have only one approved field")
        criteria = "\n".join(str(line) for line in lines).strip()
        if not criteria:
            raise ValueError("Each seeded policy needs readable Markdown criteria")
        rows.append({"name": name, "criteria": redact_text(criteria), "approved": approved})
    if not rows:
        raise ValueError("Policy seed Markdown must contain at least one '##' section")
    return rows


def seed_policy_document(policy_service, content: str, expected_content: str,
                         scope: str = "workspace", agent_name: str = "") -> None:
    """Seed an isolated benchmark/test catalog; never used by the Lisa runtime."""
    if len(content) > 20_000:
        raise ValueError("Policy seed Markdown must be at most 20000 characters")
    target = policy_service._scope(scope, agent_name)
    expected = [] if expected_content.strip() in {"", "No policy exists yet."} else _policy_rows(expected_content)
    incoming = _policy_rows(content)
    policy_service.repository.seed_scope(scope, target, expected, incoming)
