"""Conversation identity, turn provenance, and serialization rules."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from ..repositories.conversations import ConversationNotFound, FileConversationRepository
from ..runtime import (
    chat_activity_was_skipped,
    record_fallback_activity_resolution,
    selected_chat_activity_ids,
)
from .chat_activities import record_chat_activity_trajectories

logger = logging.getLogger(__name__)


def tag_turn(
    messages: list[dict[str, Any]],
    activities: list[dict[str, Any]],
    previous_messages: list[dict[str, Any]],
    turn_source: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    known_sources = {item.get("id"): item.get("turn_source") for item in previous_messages}
    previous_ids = set(known_sources)
    for message in messages:
        if message.get("id") not in previous_ids:
            known_sources[message.get("id")] = turn_source
        source = known_sources.get(message.get("id")) or message.get("turn_source") or "web"
        message["turn_source"] = source
    for activity in activities:
        activity["turn_source"] = turn_source
    return messages, activities


async def run_turn_without_releasing_lock_early(run_turn: Callable, **kwargs: Any):
    """Hold a conversation lock until its worker thread finishes, even on cancellation."""
    worker = asyncio.get_running_loop().run_in_executor(None, lambda: run_turn(**kwargs))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        while True:
            try:
                return await asyncio.shield(worker)
            except asyncio.CancelledError:
                if worker.done():
                    if worker.cancelled():
                        raise RuntimeError("Lisa turn worker was cancelled before completion")
                    return worker.result()


class ConversationService:
    def __init__(
        self, repository: FileConversationRepository, activity_store=None,
        owner_user_ids: frozenset[str] = frozenset(),
    ):
        self.repository = repository
        self.activity_store = activity_store
        self.owner_user_ids = owner_user_ids

    def list_conversations(self, workspace_id: str) -> list[dict[str, Any]]:
        records = []
        for directory in self.repository.conversation_directories(workspace_id):
            try:
                records.append(self.repository.load_metadata(directory, workspace_id))
            except ConversationNotFound:
                continue
        return sorted(records, key=lambda item: item.get("updated_at", ""), reverse=True)

    def create_conversation(self, workspace_id: str, conversation_id: str) -> dict[str, Any]:
        normalized_workspace = str(UUID(workspace_id))
        normalized_conversation = str(UUID(conversation_id))
        directory = self.repository.conversation_directory(
            normalized_workspace, normalized_conversation
        )
        if self.repository.metadata_exists(directory):
            metadata = self.repository.load_metadata(directory, normalized_workspace)
            return {**metadata, "messages": metadata.get("messages", [])}
        now = datetime.now(UTC).isoformat()
        item = {
            "conversation_id": normalized_conversation,
            "workspace_id_hash": sha256(normalized_workspace.encode()).hexdigest(),
            "title": "Lisa conversation",
            "created_at": now,
            "updated_at": now,
        }
        self.repository.save_metadata(directory, item)
        return {**item, "messages": [], "activities": []}

    def get_conversation(self, workspace_id: str, conversation_id: str) -> dict[str, Any]:
        directory = self.repository.conversation_directory(workspace_id, conversation_id)
        metadata = self.repository.load_metadata(directory, workspace_id)
        return {
            **metadata,
            "messages": metadata.get("messages", []),
            "activities": metadata.get("activities", []),
        }

    async def delete_conversation(self, workspace_id: str, conversation_id: str) -> None:
        directory = self.repository.conversation_directory(workspace_id, conversation_id)
        self.repository.load_metadata(directory, workspace_id)
        async with self.repository.lock_for(directory):
            self.repository.load_metadata(directory, workspace_id)
            await asyncio.to_thread(self.repository.delete_conversation, directory)

    async def run_user_turn(
        self,
        conversation_id: str,
        workspace_id: str,
        text: str,
        *,
        run_turn: Callable,
        turn_source: str = "web",
        actor_id: str | None = None,
        slack_channel_id: str | None = None,
        slack_thread_ts: str | None = None,
    ) -> dict[str, Any]:
        directory = self.repository.conversation_directory(workspace_id, conversation_id)
        metadata = self.repository.load_metadata(directory, workspace_id)
        chat_turn_id = str(uuid4())
        async with self.repository.lock_for(directory):
            metadata = self.repository.load_metadata(directory, workspace_id)
            previous_messages = metadata.get("messages", [])
            try:
                turn = await run_turn_without_releasing_lock_early(
                    run_turn,
                    conversation_id=conversation_id,
                    directory=str(directory),
                    workspace_id=workspace_id,
                    text=text,
                    turn_source=turn_source,
                    actor_id=actor_id,
                    slack_channel_id=slack_channel_id,
                    slack_thread_ts=slack_thread_ts,
                    chat_turn_id=chat_turn_id,
                )
            except RuntimeError as error:
                self._record_failed_chat_activity(
                    chat_turn_id, turn_source, actor_id, text, error,
                    conversation_id, slack_channel_id, slack_thread_ts,
                )
                raise ConversationTurnError(503, str(error)) from error
            except Exception as error:
                self._record_failed_chat_activity(
                    chat_turn_id, turn_source, actor_id, text, error,
                    conversation_id, slack_channel_id, slack_thread_ts,
                )
                raise ConversationTurnError(502, "Lisa could not complete this turn") from error
            activity_ids: list[str] = []
            if (
                self.activity_store is not None
                and turn_source in {"web", "slack"}
                and actor_id in self.owner_user_ids
            ):
                activity_ids = selected_chat_activity_ids(turn.activities)
                previous_ids = {str(message.get("id")) for message in previous_messages}
                if not activity_ids and not chat_activity_was_skipped(turn.activities):
                    current_user_messages = [
                        message for message in turn.messages
                        if message.get("role") == "user"
                        and str(message.get("id")) not in previous_ids
                    ]
                    user_message = current_user_messages[-1] if current_user_messages else {}
                    activity = self.activity_store.create_chat_activity(
                        turn_id=chat_turn_id,
                        request_key="fallback",
                        title=self._activity_title(text),
                        summary=text,
                        message_id=str(user_message.get("id") or "") or None,
                        source=turn_source,
                        origin_conversation_id=conversation_id,
                        origin_slack_channel_id=slack_channel_id,
                        origin_slack_thread_ts=slack_thread_ts,
                    )
                    activity_ids = [activity.id]
                    record_fallback_activity_resolution(
                        turn.activities, activity_id=activity.id, turn_id=chat_turn_id,
                    )
                new_messages = [
                    {
                        "id": str(message.get("id", "")),
                        "role": message.get("role"),
                        "text": message.get("text"),
                        "sequence": message.get("sequence", 0),
                    }
                    for message in turn.messages
                    if str(message.get("id")) not in previous_ids
                    and message.get("role") in {"user", "assistant"}
                ]
                if activity_ids or new_messages or turn.activities:
                    final_summary = next(
                        (
                            str(message.get("text") or "").strip()
                            for message in reversed(new_messages)
                            if message.get("role") == "assistant"
                            and str(message.get("text") or "").strip()
                        ),
                        "",
                    )
                    record_chat_activity_trajectories(
                        self.activity_store, activity_ids=activity_ids,
                        messages=new_messages, activities=turn.activities,
                        final_summary=final_summary, turn_failed=turn.failed,
                    )
            if metadata["title"] == "Lisa conversation":
                metadata["title"] = "Lisa"
            messages, activities = tag_turn(
                turn.messages, turn.activities, previous_messages, turn_source,
            )
            now = datetime.now(UTC).isoformat()
            metadata["updated_at"] = now
            metadata["provider_consent_at"] = now
            metadata["messages"] = messages
            metadata["activities"] = [*metadata.get("activities", []), *activities][-100:]
            self.repository.save_metadata(directory, metadata)
            return {
                "conversation_id": conversation_id,
                "messages": messages,
                "activities": activities,
                "chat_activity_ids": activity_ids if self.activity_store is not None else [],
            }

    @staticmethod
    def _activity_title(text: str) -> str:
        normalized = " ".join(text.split()).strip()
        return normalized[:120] or "Lisa chat request"

    def _record_failed_chat_activity(
        self, turn_id: str, turn_source: str, actor_id: str | None,
        text: str, error: Exception, conversation_id: str,
        slack_channel_id: str | None, slack_thread_ts: str | None,
    ) -> None:
        if (
            self.activity_store is None
            or turn_source not in {"web", "slack"}
            or actor_id not in self.owner_user_ids
        ):
            return
        try:
            self.activity_store.fail_chat_turn(
                turn_id, text, str(error), source=turn_source,
                origin_conversation_id=conversation_id,
                origin_slack_channel_id=slack_channel_id,
                origin_slack_thread_ts=slack_thread_ts,
            )
        except Exception:
            logger.exception("Could not record failed Lisa chat activity")

    async def main_conversation(self, workspace_id: str) -> dict[str, Any]:
        """Return Lisa's durable workspace conversation, creating it when absent."""
        normalized = str(UUID(workspace_id))
        root = self.repository.ensure_workspace(normalized)
        async with self.repository.lock_for(root / "main-thread.lock"):
            marker = self.repository.main_thread_marker(normalized)
            conversation_id = str(
                uuid5(NAMESPACE_URL, f"lisa:workspace-conversation:{UUID(normalized)}")
            )
            try:
                marked_id = self.repository.read_main_thread_id(marker)
            except ConversationNotFound:
                marked_id = None
            if marked_id == conversation_id:
                try:
                    item = self.repository.load_metadata(
                        self.repository.conversation_directory(normalized, conversation_id), normalized
                    )
                    return self._transcript(item)
                except ConversationNotFound:
                    pass
            try:
                item = self.repository.load_metadata(
                    self.repository.conversation_directory(normalized, conversation_id), normalized
                )
            except ConversationNotFound:
                now = datetime.now(UTC).isoformat()
                item = {
                    "conversation_id": conversation_id,
                    "workspace_id_hash": sha256(normalized.encode()).hexdigest(),
                    "title": "Lisa conversation",
                    "created_at": now,
                    "updated_at": now,
                }
            self.repository.save_metadata(
                self.repository.conversation_directory(normalized, conversation_id), item
            )
            self.repository.save_main_thread_id(marker, conversation_id)
            return self._transcript(item)

    @staticmethod
    def _transcript(item: dict[str, Any]) -> dict[str, Any]:
        return {
            **item,
            "messages": item.get("messages", []),
            "activities": item.get("activities", []),
        }


class ConversationTurnError(RuntimeError):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
