"""Human-readable projections of evaluation run context."""

from ..evaluation.prompt import format_trajectory_for_llm


def render_evaluation_context(inputs: dict) -> str:
    view = format_trajectory_for_llm(inputs.get("trajectory", {}).get("events", []))
    limitations = list((inputs.get("policy_selection") or {}).get("limitations", []))
    limitations.extend(view.limitations)
    disclosure = (
        "\nPrompt-view limitations (stored trajectory is unchanged):\n"
        + "\n".join(f"- {item}" for item in dict.fromkeys(limitations))
        + "\n"
        if limitations
        else ""
    )
    return (
        "# Evaluation run context\n\n"
        f"Policy: {inputs['policy_name']}\n\n"
        f"Target agent: {inputs['target_agent_name']}\n\n"
        f"Policy ID: {inputs['policy_id']}\n\n"
        f"Session: {inputs['session_id']}\n\n"
        f"Trace: {inputs['trace_id']}\n\n"
        "The compact trajectory is in inputs.json. "
        "The judge loads the current approved rule from lisa.policies "
        "using the stored policy name and ID.\n" + disclosure
    )
