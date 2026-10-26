"""Consume pushed session evidence from PGMQ and run Lisa's normal evaluator."""

from __future__ import annotations

import asyncio
import logging
from uuid import NAMESPACE_URL, uuid5

from ..repositories.session_evaluation_queue import SessionEvaluationQueue
from ..repositories.session_traces import SessionTraceStore
from ..security import redact_text

logger = logging.getLogger(__name__)
MAX_DELIVERIES = 8


class SessionEvaluationWorker:
    def __init__(
        self, queue: SessionEvaluationQueue, traces: SessionTraceStore, services,
        *, policy_discovery=None,
    ):
        self.queue = queue
        self.traces = traces
        self.services = services
        self.policy_discovery = policy_discovery

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
            except Exception as error:  # noqa: BLE001 - keep the queue worker alive on poll errors.
                logger.error(
                    "Lisa session evaluation queue poll failed (%s): %s",
                    type(error).__name__,
                    self._failure_reason(error),
                )
                await asyncio.sleep(2)

    async def _process(self, message: dict) -> None:
        payload = message["message"]
        message_id = int(message["msg_id"])
        session_id = str(payload.get("session_id", ""))
        agent_name = str(payload.get("agent_name", ""))
        workspace_id = self.services.context.workspace_id()
        activity_id = str(uuid5(NAMESPACE_URL, f"lisa:pushed-session:{workspace_id}:{session_id}"))
        delivery_attempt = min(max(int(message.get("read_ct", 1)), 1), MAX_DELIVERIES)
        delivery_key = f"delivery-{delivery_attempt}"
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
            discovery_selected = bool(
                self.policy_discovery
                and self.policy_discovery.reserve_sample(
                    session_id=session_id, agent_name=agent_name, activity_id=activity_id
                )
            )
            result = await asyncio.to_thread(
                self.services.evaluations.evaluate_session,
                agent_name,
                session_id,
                activity_id=activity_id,
                failure_delivery_key=delivery_key,
                defer_activity_failure=True,
                policy_discovery_enabled=discovery_selected,
            )
            if result.get("status") in {"error", "evaluation_error"}:
                raise RuntimeError(self._evaluation_failure_reason(result))
            if discovery_selected:
                signal_was_returned = False
                for item in result.get("evaluations", []):
                    if "policy_gap_signal" not in item or item.get("policy_gap_signal") is None:
                        continue
                    signal_was_returned = self.policy_discovery.record_judge_signal(
                        session_id=session_id,
                        activity_id=activity_id,
                        agent_name=agent_name,
                        signal=item["policy_gap_signal"],
                    )
                    if signal_was_returned:
                        break
                self.policy_discovery.repository.complete_sample_evaluation(
                    session_id, gap_reviewed=signal_was_returned
                )
            try:
                self.services.activity_store.clear_session_evaluation_retry_summary(activity_id)
            except Exception as error:  # noqa: BLE001 - a notice-clear error must not replay success.
                logger.error(
                    "Lisa completed session evaluation but could not clear its retry notice "
                    "for %s (%s): %s",
                    session_id,
                    type(error).__name__,
                    self._failure_reason(error),
                )
            self.queue.delete(message_id)
        except Exception as error:  # noqa: BLE001 - every attempt failure must remain retryable.
            failure_reason = self._failure_reason(error)
            try:
                self.services.activity_store.record_session_evaluation_attempt_failure(
                    activity_id,
                    attempt=delivery_attempt,
                    max_attempts=MAX_DELIVERIES,
                    error=failure_reason,
                    delivery_key=delivery_key,
                )
            except Exception as record_error:  # noqa: BLE001 - preserve queue custody on any DB error.
                # Preserve queue retry if the activity database is temporarily
                # unavailable; the sanitized reason remains in the worker log.
                logger.error(
                    "Lisa could not record the failed evaluation attempt for %s "
                    "(%s): %s",
                    session_id,
                    type(record_error).__name__,
                    self._failure_reason(record_error),
                )
            if int(message.get("read_ct", 1)) >= MAX_DELIVERIES:
                activity_id = str(uuid5(
                    NAMESPACE_URL,
                    f"lisa:pushed-session:{self.services.context.workspace_id()}:{session_id}",
                ))
                failed = self.services.activity_store.fail_exhausted_session_evaluation(
                    activity_id,
                    f"Lisa could not evaluate session {session_id} after {MAX_DELIVERIES} attempts. "
                    f"Last error: {failure_reason}",
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
                logger.error(
                    "Lisa session evaluation attempt %d/%d failed for %s; it will be retried: %s",
                    delivery_attempt,
                    MAX_DELIVERIES,
                    session_id,
                    failure_reason,
                )

    @staticmethod
    def _failure_reason(error: Exception) -> str:
        reason = redact_text(str(error)).strip()
        if not reason:
            reason = type(error).__name__
        return reason[:1200]

    @classmethod
    def _evaluation_failure_reason(cls, result: dict) -> str:
        message = result.get("message")
        if message:
            return cls._failure_reason(RuntimeError(str(message)))
        failures = []
        for evaluation in result.get("evaluations", []):
            if evaluation.get("status") not in {"error", "already_running", "policy_unavailable"}:
                continue
            reason = evaluation.get("rationale") or evaluation.get("message")
            label = evaluation.get("evaluation_name") or evaluation.get("policy_id")
            if reason:
                failures.append(f"{label}: {reason}" if label else str(reason))
            else:
                failures.append(f"{label}: {evaluation['status']}" if label else evaluation["status"])
        if failures:
            return cls._failure_reason(RuntimeError("; ".join(failures)))
        status = result.get("status", "unknown")
        return f"Policy evaluation returned {status}."
