"""Shared durable intake path for HTTP producers and Lisa's TheLake sampler."""

from __future__ import annotations

from typing import Any

from ..repositories.session_evaluation_queue import SessionEvaluationQueue
from ..repositories.session_traces import SessionTraceStore


class SessionTraceQueueUnavailable(RuntimeError):
    pass


class SessionTraceIntake:
    def __init__(
        self,
        traces: SessionTraceStore,
        queue: SessionEvaluationQueue | None,
    ):
        self.traces = traces
        self.queue = queue

    def has_session(self, session_id: str) -> bool:
        return self.traces.has_manifest(session_id)

    def accept(self, bundle: dict[str, Any]) -> dict[str, str]:
        if self.queue is None:
            raise SessionTraceQueueUnavailable("Lisa evaluation queue is not configured")
        stored = self.traces.persist(bundle)
        enqueued = self.queue.send(stored.session_id, stored.agent_name, stored.payload_hash)
        return {
            "session_id": stored.session_id,
            "agent_name": stored.agent_name,
            "status": "queued" if enqueued else "already_queued",
        }
