"""Stable host-local lock files for conversation operations."""

from pathlib import Path


def conversation_lock_path(directory: Path, purpose: str = "turn") -> Path:
    """Return a lock path outside the conversation directory it protects."""
    return directory.parent / ".conversation-locks" / f"{directory.name}-{purpose}.lock"
