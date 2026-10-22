"""Run accepted Slack events inside Lisa's durable conversation workflow."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from ..repositories.activities import PostgresActivityStore
from ..repositories.conversations import FileConversationRepository
from ..repositories.slack_events import SlackEventRepository
from ..runtime import (
    chat_activity_was_skipped,
    record_fallback_activity_resolution,
    selected_chat_activity_ids,
)
from .chat_activities import record_chat_activity_trajectories
from ..security import redact_text
from ..slack import SlackClient

logger = logging.getLogger(__name__)


def _activity_title(text: str) -> str:
    normalized = " ".join(text.split()).strip()
    return normalized[:120] or "Lisa chat request"


class SlackEventService:
    def __init__(
        self,
        conversations: FileConversationRepository,
        events: SlackEventRepository,
        activity_store: PostgresActivityStore | None = None,
        owner_user_ids: frozenset[str] = frozenset(),
    ):
        self.conversations = conversations
        self.events = events
        self.activity_store = activity_store
        self.owner_user_ids = owner_user_ids

    def recover_interrupted(self, workspace_id: str) -> None:
        for path in self.events.pending_paths(workspace_id):
            try:
                receipt = self.events.load(path)
            except (OSError, ValueError):
                continue
            if receipt.get("status") == "processing":
                receipt["status"] = "pending"
                receipt["updated_at"] = datetime.now(UTC).isoformat()
                self.events.save(path, receipt)

    def pending_paths(self, workspace_id: str) -> list[Path]:
        pending = []
        for path in self.events.pending_paths(workspace_id):
            try:
                receipt = self.events.load(path)
            except (OSError, ValueError):
                continue
            if receipt.get("status") == "pending":
                pending.append(path)
        return pending

    def queue(self, event: dict) -> None:
        path = self.events.event_path(event["workspace_id"], event["event_id"])
        if not self.events.exists(path):
            self.events.save(path, event)

    async def process(
        self,
        path: Path,
        *,
        run_turn: Callable,
        confirm_policy_id: Callable[[str], str | None],
        post_reply: Callable[[str, str, str], None],
        confirm_followup_id: Callable[[str], str | None] | None = None,
    ) -> None:
        async with self.conversations.lock_for(path):
            try:
                receipt = self.events.load(path)
            except (OSError, ValueError):
                return
            if receipt.get("status") != "pending":
                return
            receipt["status"] = "processing"
            receipt["updated_at"] = datetime.now(UTC).isoformat()
            self.events.save(path, receipt)


            workspace_id = receipt["workspace_id"]
            channel_id = receipt["channel_id"]
            thread_ts = receipt["thread_ts"]
            conversation_id = str(
                uuid5(
                    NAMESPACE_URL,
                    f"lisa:slack:{UUID(workspace_id)}:{channel_id}:{thread_ts}",
                )
            )
            directory = self.conversations.conversation_directory(workspace_id, conversation_id)
            try:
                async with self.conversations.lock_for(directory):
                    if self.conversations.metadata_exists(directory):
                        metadata = self.conversations.load_metadata(directory, workspace_id)
                    else:
                        now = datetime.now(UTC).isoformat()
                        metadata = {
                            "conversation_id": conversation_id,
                            "workspace_id_hash": sha256(
                                str(UUID(workspace_id)).encode()
                            ).hexdigest(),
                            "title": receipt["text"][:60],
                            "created_at": now,
                            "updated_at": now,
                        }
                    turn = await asyncio.to_thread(
                        run_turn,
                        conversation_id=conversation_id,
                        directory=str(directory),
                        workspace_id=workspace_id,
                        text=receipt["text"],
                        turn_source="slack",
                        actor_id=str(receipt.get("actor_id", "")),
                        confirmed_policy_id=confirm_policy_id(receipt["text"]),
                        confirmed_followup_id=(confirm_followup_id or (lambda _text: None))(receipt["text"]),
                        slack_channel_id=channel_id,
                        slack_thread_ts=thread_ts,
                        chat_turn_id=str(receipt["event_id"]),
                    )
                    is_owner = str(receipt.get("actor_id", "")) in self.owner_user_ids
                    if is_owner and self.activity_store is not None:
                        previous_ids = {
                            str(message.get("id"))
                            for message in metadata.get("messages", [])
                        }
                        activity_ids = selected_chat_activity_ids(turn.activities)
                        if not activity_ids and not chat_activity_was_skipped(turn.activities):
                            user_message = next(
                                (
                                    message for message in reversed(turn.messages)
                                    if message.get("role") == "user"
                                    and str(message.get("id")) not in previous_ids
                                ),
                                {},
                            )
                            activity = self.activity_store.create_chat_activity(
                                turn_id=str(receipt["event_id"]),
                                request_key="fallback",
                                title=_activity_title(receipt["text"]),
                                summary=receipt["text"],
                                message_id=str(user_message.get("id") or "") or None,
                                source="slack",
                                origin_conversation_id=conversation_id,
                                origin_slack_channel_id=channel_id,
                                origin_slack_thread_ts=thread_ts,
                            )
                            activity_ids = [activity.id]
                            record_fallback_activity_resolution(
                                turn.activities, activity_id=activity.id,
                                turn_id=str(receipt["event_id"]),
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
                    latest_reply = next(
                        (
                            message.get("text", "")
                            for message in reversed(turn.messages)
                            if message.get("role") == "assistant"
                        ),
                        "Lisa could not prepare a reply.",
                    )
                    metadata["event_source"] = "slack"
                    metadata["updated_at"] = datetime.now(UTC).isoformat()
                    metadata["messages"] = turn.messages
                    metadata["activities"] = [*metadata.get("activities", []), *turn.activities][
                        -100:
                    ]
                    self.conversations.save_metadata(directory, metadata)
                await asyncio.to_thread(post_reply, channel_id, thread_ts, latest_reply)
                receipt["status"] = "complete"
                receipt["conversation_id"] = conversation_id
                receipt.pop("text", None)
            except Exception as error:
                logger.exception("Lisa Slack event processing failed")
                if (
                    self.activity_store is not None
                    and str(receipt.get("actor_id", "")) in self.owner_user_ids
                ):
                    try:
                        self.activity_store.fail_chat_turn(
                            str(receipt["event_id"]), str(receipt.get("text", "")), str(error),
                            source="slack",
                            origin_conversation_id=conversation_id,
                            origin_slack_channel_id=channel_id,
                            origin_slack_thread_ts=thread_ts,
                        )
                    except Exception:
                        logger.exception("Could not record failed Lisa Slack activity")
                receipt["status"] = "failed"
                receipt["error"] = redact_text(str(error))
            receipt["updated_at"] = datetime.now(UTC).isoformat()
            self.events.save(path, receipt)

class SlackEventWorker:
    """Poll queued receipts and own the in-process Slack event tasks."""

    def __init__(
        self,
        events: SlackEventService,
        slack: SlackClient,
        run_turn: Callable,
        confirm_policy_id: Callable[[str], str | None],
        confirm_followup_id: Callable[[str], str | None] | None = None,
    ):
        self.events = events
        self.slack = slack
        self.run_turn = run_turn
        self.confirm_policy_id = confirm_policy_id
        self.confirm_followup_id = confirm_followup_id or (lambda _text: None)
        self._tasks: dict[str, asyncio.Task] = {}

    async def run_forever(self, workspace_id: str, interval: float = 1.0) -> None:
        self.events.recover_interrupted(workspace_id)
        while True:
            for path in self.events.pending_paths(workspace_id):
                key = str(path)
                task = self._tasks.get(key)
                if task is not None and task.done():
                    self._tasks.pop(key, None)
                    task = None
                if task is None:
                    self._tasks[key] = asyncio.create_task(self._process(path))
            await asyncio.sleep(interval)

    async def _process(self, path: Path) -> None:
        await self.events.process(
            path,
            run_turn=self.run_turn,
            confirm_policy_id=self.confirm_policy_id,
            confirm_followup_id=self.confirm_followup_id,
            post_reply=lambda channel, thread, text: self.slack.post_message(
                channel, text, thread, timeout=15
            ),
        )

    async def shutdown(self) -> None:
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
