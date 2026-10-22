"""Conversation response projections for Lisa's browser interface."""


def conversation_summary(item: dict) -> dict:
    """Project stored conversation metadata to the internal list response."""
    fields = (
        "conversation_id",
        "title",
        "updated_at",
        "event_source",
        "event_status",
        "event_id",
        "event_error",
    )
    return {key: item[key] for key in fields if key in item}
