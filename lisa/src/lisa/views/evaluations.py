"""Human-readable projections of evaluation run context."""


def render_evaluation_context(inputs: dict) -> str:
    return (
        "# Evaluation run context\n\n"
        f"Policy: {inputs['policy_name']}\n\n"
        f"Target agent: {inputs['target_agent_name']}\n\n"
        f"Policy ID: {inputs['policy_id']}\n\n"
        f"Session: {inputs['session_id']}\n\n"
        f"Trace: {inputs['trace_id']}\n\n"
        "The compact trajectory is in inputs.json. "
        "The judge loads the current approved rule from lisa.policies "
        "using the stored policy name and ID.\n"
    )
