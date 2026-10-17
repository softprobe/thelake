"""Human-readable policy article projection returned by policy tools."""

from ..models.policies import PolicyRecord, policy_id_sort_key


def render_policy_records(records: list[PolicyRecord], scope: str) -> str:
    if not records:
        return "No policy exists yet."
    title = "Workspace policies" if scope == "workspace" else "Agent policies"
    sections = [f"# {title}"]
    for record in sorted(records, key=lambda item: policy_id_sort_key(item.id)):
        approval = "yes" if record.approved else "no"
        sections.append(
            f"## {record.id} | {record.name}\napproved: {approval}\n\n{record.criteria}"
        )
    return "\n\n".join(sections) + "\n"


def render_policy_list(records: list[PolicyRecord]) -> list[str] | str:
    """Render concise scope/applicability summaries for Lisa's policy picker."""
    if not records:
        return "No policies exist yet."
    result = []
    for record in sorted(records, key=lambda item: policy_id_sort_key(item.id)):
        applies_to = (
            "all agents by default"
            if record.scope == "workspace"
            else f"{record.target_agent_name} only"
        )
        result.append(
            f"{record.id} | {record.name} | Applies to: {applies_to} | "
            f"approved: {'yes' if record.approved else 'no'}"
        )
    return result


def render_policy_saved(count: int) -> str:
    """Render the confirmation returned by the policy update tool."""
    return f"Saved {count} policy rule(s) to the Lisa database."
