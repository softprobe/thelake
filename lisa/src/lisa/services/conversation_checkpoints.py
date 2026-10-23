"""Generate immutable conversation checkpoints from durable PGMQ work."""

from __future__ import annotations

import asyncio
import logging
import re

from ..context import WorkspaceContext
from ..conversation_context import generate_and_publish_checkpoint
from ..llm_recording import recording_scope
from ..repositories.conversation_checkpoint_queue import ConversationCheckpointQueue
from ..repositories.conversations import FileConversationRepository

logger = logging.getLogger(__name__)
MAX_DELIVERIES = 8
_REQUEST_ID = re.compile(r"[0-9a-f]{64}")


class ConversationCheckpointWorker:
    def __init__(
        self,
        queue: ConversationCheckpointQueue,
        conversations: FileConversationRepository,
        llm_factory,
        context: WorkspaceContext,
    ):
        self.queue = queue
        self.conversations = conversations
        self.llm_factory = llm_factory
        self.context = context

    async def run_forever(self) -> None:
        while True:
            try:
                message = await asyncio.to_thread(self.queue.read_one)
                if message is None:
                    await asyncio.sleep(1)
                    continue
                await asyncio.to_thread(self._process, message)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Lisa conversation checkpoint queue poll failed")
                await asyncio.sleep(2)

    def _process(self, message: dict) -> None:
        payload = message.get("message", {})
        message_id = int(message["msg_id"])
        conversation_id = str(payload.get("conversation_id", ""))
        request_id = str(payload.get("request_id", ""))
        terminal_archived = False
        try:
            if not _REQUEST_ID.fullmatch(request_id):
                raise ValueError("Queued conversation checkpoint has an invalid request key")
            directory = self.conversations.conversation_directory(
                self._workspace_id(), conversation_id
            )
            repository = self.conversations.context_repository(directory)
            with repository.lock():
                try:
                    if not directory.is_dir():
                        # A queued checkpoint is obsolete after its chat is deleted.
                        self.queue.delete(message_id)
                        return
                    existing = repository.read_checkpoint(request_id)
                    if existing is not None:
                        repository.publish_checkpoint(request_id, existing, lock_held=True)
                        repository.delete_checkpoint_input(request_id)
                        self.queue.delete(message_id)
                        return
                    checkpoint_input = repository.read_checkpoint_input(request_id)
                    with recording_scope(
                        self.context,
                        conversation_id=conversation_id,
                        checkpoint_request_id=request_id,
                    ):
                        generate_and_publish_checkpoint(
                            repository,
                            request_id,
                            checkpoint_input,
                            self.llm_factory(),
                            lock_held=True,
                        )
                    repository.delete_checkpoint_input(request_id)
                    self.queue.delete(message_id)
                except Exception:
                    if int(message.get("read_ct", 1)) >= MAX_DELIVERIES:
                        # Archive while holding the same lock used by checkpoint capture.
                        self.queue.archive(message_id)
                        terminal_archived = True
                        try:
                            repository.delete_checkpoint_input(request_id)
                        except OSError:
                            logger.exception(
                                "Could not remove archived Lisa conversation checkpoint input"
                            )
                    raise
        except Exception:
            if int(message.get("read_ct", 1)) >= MAX_DELIVERIES:
                if not terminal_archived:
                    self.queue.archive(message_id)
                logger.exception(
                    "Lisa archived a conversation checkpoint after %d attempts",
                    MAX_DELIVERIES,
                )
            else:
                logger.exception("Lisa conversation checkpoint will be retried")

    def _workspace_id(self) -> str:
        # The Lisa database and file repository are bound to one workspace per runtime.
        return self.context.workspace_id()
