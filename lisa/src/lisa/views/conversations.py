"""Conversation response projections for Lisa's browser interface."""


def browser_conversation(item: dict) -> dict:
    """Keep internal scheduled prompts out of the browser response."""
    return {
        **item,
        "messages": [message for message in item.get("messages", []) if not message.get("hidden")],
    }


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
