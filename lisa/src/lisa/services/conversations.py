"""Conversation identity, turn provenance, and serialization rules."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from ..repositories.conversations import ConversationNotFound, FileConversationRepository


def tag_turn(
    messages: list[dict[str, Any]],
    activities: list[dict[str, Any]],
    previous_messages: list[dict[str, Any]],
    turn_source: str,
    hidden_message_ids: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    known_sources = {item.get("id"): item.get("turn_source") for item in previous_messages}
    previously_hidden = {item.get("id") for item in previous_messages if item.get("hidden")}
    previous_ids = set(known_sources)
    for message in messages:
        if message.get("id") not in previous_ids:
            known_sources[message.get("id")] = turn_source
        source = known_sources.get(message.get("id")) or message.get("turn_source") or "web"
        message["turn_source"] = source
        if message.get("id") in previously_hidden or (
            source == "scheduled" and message.get("role") == "user"
        ):
            hidden_message_ids.add(str(message.get("id", "")))
    for activity in activities:
        activity["turn_source"] = turn_source
    return messages, activities, hidden_message_ids


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
    def __init__(self, repository: FileConversationRepository):
        self.repository = repository

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
        confirmed_policy_title: str | None,
        turn_source: str = "web",
        actor_id: str | None = None,
        slack_channel_id: str | None = None,
        slack_thread_ts: str | None = None,
        confirmed_policy_proposal_id: str | None = None,
        rejected_policy_proposal_id: str | None = None,
        confirmed_followup_id: str | None = None,
    ) -> dict[str, Any]:
        directory = self.repository.conversation_directory(workspace_id, conversation_id)
        metadata = self.repository.load_metadata(directory, workspace_id)
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
                    confirmed_policy_title=confirmed_policy_title,
                    confirmed_policy_proposal_id=confirmed_policy_proposal_id,
                    rejected_policy_proposal_id=rejected_policy_proposal_id,
                    confirmed_followup_id=confirmed_followup_id,
                )
            except RuntimeError as error:
                raise ConversationTurnError(503, str(error)) from error
            except Exception as error:
                raise ConversationTurnError(502, "Lisa could not complete this turn") from error
            if metadata["title"] == "Lisa conversation":
                metadata["title"] = "Lisa"
            hidden_ids = {
                str(message["id"])
                for message in previous_messages
                if message.get("hidden") and message.get("id")
            }
            messages, activities, hidden_ids = tag_turn(
                turn.messages, turn.activities, previous_messages, turn_source, hidden_ids
            )
            for message in messages:
                message["hidden"] = message.get("id") in hidden_ids
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
            }

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


SCHEDULED_PROMPT = """This scheduled turn is Lisa's QA work in her ongoing workspace conversation. Follow the own-agent-qa skill using workspace tools. Resume pending failures, inspect recent sessions, and evaluate relevant policy table records marked approved: yes. Rules marked approved: no or missing approval are proposals, not enforceable business requirements. Use recent QA updates in this conversation to avoid repeating unchanged proposals. Do not ask the owner to approve routine QA work. Do not create or change policy during a scheduled turn. Report evidenced risks, uncertainty, and coverage limits. Never claim unevaluated sessions passed."""


class ScheduledQAError(RuntimeError):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code


class ConversationTurnError(RuntimeError):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code


class ScheduledQAService:
    def __init__(
        self,
        repository: FileConversationRepository,
        conversations: ConversationService,
    ):
        self.repository = repository
        self.conversations = conversations

    async def run_cycle(
        self,
        workspace_id: str,
        *,
        run_turn: Callable,
        confirm_policy_title: Callable[[str], str | None],
    ) -> dict[str, Any]:
        try:
            normalized = str(UUID(workspace_id))
        except ValueError as error:
            raise ScheduledQAError(400, "Invalid workspace scope") from error
        main = await self.conversations.main_conversation(normalized)
        conversation_id = main["conversation_id"]
        directory = self.repository.conversation_directory(normalized, conversation_id)
        async with self.repository.lock_for(directory):
            metadata = self.repository.load_metadata(directory, normalized)
            previous_messages = metadata.get("messages", [])
            try:
                turn = await run_turn_without_releasing_lock_early(
                    run_turn,
                    conversation_id=conversation_id,
                    directory=str(directory),
                    workspace_id=normalized,
                    text=SCHEDULED_PROMPT,
                    turn_source="scheduled",
                )
            except RuntimeError as error:
                raise ScheduledQAError(503, "Lisa could not start scheduled QA") from error
            except Exception as error:
                raise ScheduledQAError(502, "Lisa could not complete scheduled QA") from error
            hidden_ids = {
                str(message["id"])
                for message in previous_messages
                if message.get("hidden") and message.get("id")
            }
            messages, activities, hidden_ids = tag_turn(
                turn.messages, turn.activities, previous_messages, "scheduled", hidden_ids
            )
            for message in messages:
                message["hidden"] = message.get("id") in hidden_ids
            metadata["updated_at"] = datetime.now(UTC).isoformat()
            metadata["messages"] = messages
            metadata["activities"] = [*metadata.get("activities", []), *activities][-100:]
            self.repository.save_metadata(directory, metadata)
            return {
                "conversation_id": conversation_id,
                "messages": messages,
                "activities": activities,
            }
