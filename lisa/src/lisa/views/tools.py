"""Safe presentation helpers for tool-facing results."""


def hide_evaluation_ids(value):
    """Keep implementation identifiers out of Lisa's user-visible evidence."""
    hidden = {
        "evaluation_id",
        "config_id",
        "score_id",
        "run_id",
        "policy_hash",
    }
    if isinstance(value, dict):
        return {key: hide_evaluation_ids(item) for key, item in value.items() if key not in hidden}
    if isinstance(value, list):
        return [hide_evaluation_ids(item) for item in value]
    return value
