"""Tool-facing wording for stored evaluation failures."""


def render_pending_failures(items: list[dict]) -> list[dict] | str:
    if not items:
        return "No evaluation failures are awaiting follow-up."
    return items
