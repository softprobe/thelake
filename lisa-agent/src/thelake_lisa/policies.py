from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

from .context import WorkspaceContext
from .security import redact_text
from .storage import atomic_write_text, file_lock


@dataclass(frozen=True)
class PolicyRule:
    """A parsed POLICY.md section, not a separately stored evaluator definition."""

    name: str
    criteria: str
    approved: bool
    scope: str
    snapshot: str

    @property
    def revision(self) -> str:
        # Internal evidence identity: owners edit Markdown, never version numbers.
        return hashlib.sha256(self.snapshot.encode("utf-8")).hexdigest()


class PolicyStore:
    def __init__(self, context: WorkspaceContext):
        self.context = context

    def _policy_path(self, scope: str, agent_name: str = "") -> Path:
        home = self.context.home()
        if scope == "workspace":
            path = home / "POLICY.md"
        elif scope == "agent" and agent_name.strip():
            digest = hashlib.sha256(agent_name.encode("utf-8")).hexdigest()
            path = home / "agents" / digest / "POLICY.md"
        else:
            raise ValueError("Use workspace scope or provide the exact agent name for agent scope")
        if path.is_symlink() or not path.parent.resolve().is_relative_to(home.resolve()):
            raise ValueError("Policy path escapes Lisa's workspace home or uses a symlink")
        return path

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
                sections.append((heading.group(1), []))
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
                match = re.fullmatch(r"approved:\s*(yes|no)", content_lines.pop(0), re.I)
                if not match:
                    raise ValueError("Each rule's approved field must be yes or no")
                approved = match.group(1).lower() == "yes"
                # Metadata belongs immediately below the title, before the prose.
                # A prose/code example later in the section cannot grant approval.
                remaining = "\n".join(content_lines).strip()
                if remaining.lower().startswith("approved:"):
                    raise ValueError("Each rule must have only one approved field")
            criteria = "\n".join(content_lines).strip()
            if not criteria:
                raise ValueError("Each named policy rule must have readable Markdown content")
            snapshot = "\n".join([f"## {title}", *lines]).strip()
            if title in rules:
                raise ValueError("Policy rule names must be unique within a POLICY.md file")
            rules[title] = PolicyRule(title, criteria, approved, scope, snapshot)
        return rules

    def approved_rules(self, target_agent_name: str) -> list[PolicyRule]:
        # Overlay before filtering: an unapproved agent rule suppresses the
        # approved workspace rule with the same title instead of reviving it.
        rules = self.parse_rules(self.read_policy("workspace"), "workspace")
        rules.update(self.parse_rules(self.read_policy("agent", target_agent_name), "agent"))
        return [rule for rule in rules.values() if rule.approved]

    def rules(self, markdown: str) -> dict[str, str]:
        return {
            title: "\n".join([f"## {title}", *lines]).strip()
            for title, lines in self._sections(markdown)
        }

    def _validate_policy_markdown(self, markdown: str) -> None:
        sections = self._sections(markdown)
        if not sections:
            raise ValueError("Policy Markdown must contain at least one '##' rule section")
        titles = [title.strip().casefold() for title, _ in sections]
        if len(titles) != len(set(titles)):
            raise ValueError("Policy rule names must be unique within a POLICY.md file")
        self.parse_rules(markdown, "workspace")
        for title, lines in sections:
            if not title.strip() or not "\n".join(lines).strip():
                raise ValueError("Each named policy rule must have readable Markdown content")

    def check_changes(self, content: str, current: str) -> None:
        before = self.rules(current)
        after = self.rules(content)
        changed_titles = {
            title for title in before.keys() | after.keys() if before.get(title) != after.get(title)
        }
        if not changed_titles:
            return
        if self.context.turn_source not in {"web", "slack"}:
            raise ValueError("Only an owner conversation can save policy changes")
        confirmed_title = self.context.confirmed_policy_title
        if not confirmed_title:
            raise ValueError(
                "Propose the named policy in chat and wait for the owner's explicit confirmation"
            )
        if changed_titles != {confirmed_title}:
            raise ValueError("Only the exact policy name the owner confirmed this turn may change")

    def list_policies(self) -> list[str] | str:
        """List Lisa's workspace and agent-specific POLICY.md memory files."""
        home = self.context.home()
        files = ["POLICY.md"] if (home / "POLICY.md").is_file() else []
        agent_root = home / "agents"
        if agent_root.is_dir() and not agent_root.is_symlink():
            files.extend(
                f"agents/{directory.name}/POLICY.md"
                for directory in agent_root.iterdir()
                if directory.is_dir()
                and not directory.is_symlink()
                and (directory / "POLICY.md").is_file()
                and not (directory / "POLICY.md").is_symlink()
            )
        return files or "No Lisa policy files exist yet."

    def read_policy(self, scope: str = "workspace", agent_name: str = "") -> str:
        """Read workspace policy or the policy for one exact registered agent."""
        path = self._policy_path(scope, agent_name)
        if path.is_symlink():
            raise ValueError("Policy files cannot be symlinks")
        if not path.is_file():
            return "No policy exists yet."
        if path.stat().st_size > 20_000:
            raise ValueError("Policy Markdown exceeds the 20000 character limit")
        content = path.read_text(encoding="utf-8")
        return redact_text(content)

    def update_policy(
        self,
        content: str,
        expected_content: str,
        scope: str = "workspace",
        agent_name: str = "",
    ) -> str:
        """Save complete policy Markdown after reading its current value."""
        content = redact_text(content)
        expected_content = redact_text(expected_content)
        if not content.strip() or len(content) > 20_000:
            raise ValueError("Policy Markdown must contain 1 to 20000 characters")
        self._validate_policy_markdown(content)
        path = self._policy_path(scope, agent_name)
        with file_lock(path.with_suffix(".lock")):
            current = path.read_text(encoding="utf-8") if path.is_file() else ""
            normalized_expected = (
                "" if expected_content == "No policy exists yet." else expected_content
            )
            if redact_text(current) != normalized_expected:
                raise ValueError("Policy changed since it was read; read it again before updating")
            if current != content and self.context.turn_source not in {"web", "slack"}:
                raise ValueError("Only an owner conversation can save policy changes")
            self.check_changes(content, current)
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.is_symlink() or not path.parent.resolve().is_relative_to(
                self.context.home().resolve()
            ):
                raise ValueError("Policy path escapes Lisa's workspace home or uses a symlink")
            atomic_write_text(path, content)
        return f"Saved {scope} policy memory."
