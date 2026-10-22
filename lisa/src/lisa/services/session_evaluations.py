"""Consume pushed session evidence from PGMQ and run Lisa's normal evaluator."""

from __future__ import annotations

import asyncio
import logging
from uuid import NAMESPACE_URL, uuid5

from ..repositories.session_evaluation_queue import SessionEvaluationQueue
from ..repositories.session_traces import SessionTraceStore

logger = logging.getLogger(__name__)
MAX_DELIVERIES = 8


class SessionEvaluationWorker:
    def __init__(self, queue: SessionEvaluationQueue, traces: SessionTraceStore, services):
        self.queue = queue
        self.traces = traces
        self.services = services

    async def run_forever(self) -> None:
        while True:
            try:
                message = await asyncio.to_thread(self.queue.read_one)
                if message is None:
                    await asyncio.sleep(1)
                    continue
                await self._process(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Lisa session evaluation queue poll failed")
                await asyncio.sleep(2)

    async def _process(self, message: dict) -> None:
        payload = message["message"]
        message_id = int(message["msg_id"])
        session_id = str(payload.get("session_id", ""))
        agent_name = str(payload.get("agent_name", ""))
        workspace_id = self.services.context.workspace_id()
        activity_id = str(uuid5(NAMESPACE_URL, f"lisa:pushed-session:{workspace_id}:{session_id}"))
        try:
            existing = self.services.activity_store.get_activity_thread(activity_id)
            if existing and existing.get("activity", {}).get("status") in {
                "succeeded", "failed", "cancelled"
            }:
                self.queue.delete(message_id)
                return
            self.services.activity_store.create_activity(
                title=f"Evaluate session {session_id} for {agent_name}",
                source="session_push",
                kind="session_evaluation",
                activity_id=activity_id,
                context_path=f"activities/{activity_id}",
                agent_id=agent_name,
            )
            stored = self.traces.load(session_id)
            if (
                stored["agent_names"] != [agent_name]
                or stored.get("payload_hash") != payload.get("payload_hash")
            ):
                raise ValueError("Queued evaluation does not match immutable session evidence")
            result = await asyncio.to_thread(
                self.services.evaluations.evaluate_session,
                agent_name,
                session_id,
                activity_id=activity_id,
                defer_activity_failure=True,
            )
            if result.get("status") in {"error", "evaluation_error"}:
                raise RuntimeError("Policy evaluation returned a retryable error")
            self.queue.delete(message_id)
        except Exception as error:
            if int(message.get("read_ct", 1)) >= MAX_DELIVERIES:
                activity_id = str(uuid5(
                    NAMESPACE_URL,
                    f"lisa:pushed-session:{self.services.context.workspace_id()}:{session_id}",
                ))
                failed = self.services.activity_store.fail_exhausted_session_evaluation(
                    activity_id,
                    f"Lisa could not evaluate session {session_id} after {MAX_DELIVERIES} attempts ({type(error).__name__}).",
                )
                if failed:
                    self.queue.archive(message_id)
                    logger.error("Lisa archived exhausted session evaluation for %s", session_id)
                else:
                    logger.warning(
                        "Lisa kept exhausted session evaluation for %s queued because its activity still has live work",
                        session_id,
                    )
            else:
                logger.exception("Lisa session evaluation will be retried for %s", session_id)
