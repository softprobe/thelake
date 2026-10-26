"""Bounded policy-gap review, daily synthesis, and owner decisions."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Protocol

from ..evaluation.policy_discovery import analyze_policy_gap, synthesize_policy_suggestions
from ..models.policy_discovery import PolicyGapSignal, PolicySuggestionDraft
from ..repositories.policy_discovery import PolicyDiscoveryRepository
from ..security import redact_text

logger = logging.getLogger(__name__)


class PolicyGapAnalyzer(Protocol):
    """Replaceable provider capability for reviewing one sampled trajectory."""

    def __call__(self, trajectory: dict) -> PolicyGapSignal: ...


class PolicySuggestionSynthesizer(Protocol):
    """Replaceable provider capability for consolidating stored gap signals."""

    def __call__(self, signals: list[dict], policies: list[dict], decisions: list[dict]) -> list[PolicySuggestionDraft]: ...


class PolicyDiscoveryService:
    def __init__(
        self,
        repository: PolicyDiscoveryRepository,
        policy_service,
        sessions,
        *,
        hourly_sample_limit: int = 5,
        gap_analyzer: PolicyGapAnalyzer = analyze_policy_gap,
        synthesizer: PolicySuggestionSynthesizer = synthesize_policy_suggestions,
        clock=lambda: datetime.now(UTC),
    ):
        if not 1 <= hourly_sample_limit <= 100:
            raise ValueError("Policy discovery samples per hour must be between 1 and 100")
        self.repository = repository
        self.policy_service = policy_service
        self.sessions = sessions
        self.hourly_sample_limit = hourly_sample_limit
        self.clock = clock
        self.gap_analyzer = gap_analyzer
        self.synthesizer = synthesizer

    def reserve_sample(self, *, session_id: str, agent_name: str, activity_id: str) -> bool:
        return self.repository.reserve_sample(
            session_id=session_id, agent_name=agent_name, activity_id=activity_id,
            hourly_limit=self.hourly_sample_limit, now=self.clock(),
        )

    def record_judge_signal(
        self, *, session_id: str, activity_id: str, agent_name: str, signal: dict | None
    ) -> bool:
        if signal is None:
            return False
        parsed = PolicyGapSignal.model_validate(signal)
        if not parsed.detected:
            return True
        self._save_signal(session_id, activity_id, agent_name, parsed)
        return True

    def review_pending_samples(self) -> int:
        reviewed = 0
        for sample in self.repository.pending_samples(limit=20):
            try:
                trajectory = self.sessions.get_trajectory(sample["session_id"])
                if (
                    not trajectory.get("complete")
                    or not trajectory.get("message_order_certain")
                    or not trajectory.get("tool_order_certain")
                ):
                    self.repository.mark_sample_reviewed(sample["session_id"])
                    reviewed += 1
                    continue
                if not self.repository.has_signal(sample["session_id"]):
                    signal = self.gap_analyzer(trajectory)
                    if signal.detected:
                        self._save_signal(
                            sample["session_id"], str(sample["activity_id"]),
                            sample["agent_name"], signal,
                        )
                self.repository.mark_sample_reviewed(sample["session_id"])
                reviewed += 1
            except Exception as error:  # noqa: BLE001 - retry discovery independently of evaluation.
                logger.warning(
                    "Policy-gap review for sampled session failed (%s): %s",
                    type(error).__name__, redact_text(str(error))[:300],
                )
        return reviewed

    def _save_signal(self, session_id: str, activity_id: str, agent_name: str, signal):
        trajectory = self.sessions.get_trajectory(session_id)
        self.validate_references(signal.span_ids, {event["span_id"] for event in trajectory["events"]})
        cited = set(signal.span_ids)
        evidence = [
            {
                "span_id": event["span_id"], "kind": event["kind"],
                "name": event.get("name"), "content": event.get("content"),
                "payload": event.get("payload"), "status": event.get("status"),
            }
            for event in trajectory["events"] if event["span_id"] in cited
        ]
        for item in evidence:
            if item.get("content"):
                item["content"] = str(item["content"])[:2_000]
            if item.get("payload") is not None:
                encoded = json.dumps(item["payload"], ensure_ascii=False)
                item["payload"] = encoded[:2_000]
        return self.repository.save_signal(
            session_id=session_id, activity_id=activity_id, agent_name=agent_name,
            summary=redact_text(signal.summary)[:2_000], span_ids=signal.span_ids,
            evidence=evidence,
        )

    @staticmethod
    def validate_references(references: list[str], known: set[str]) -> None:
        if not references or any(reference not in known for reference in references):
            raise ValueError("Policy discovery cited evidence outside the selected session")

    def synthesize_daily(self) -> list[dict]:
        if not self.repository.claim_daily_synthesis():
            return []
        try:
            signals = self.repository.pending_signals(limit=20)
            if not signals:
                self.repository.release_daily_synthesis()
                return []
            policies = [
                {
                    "id": item.id, "scope": item.scope,
                    "target_agent_name": item.target_agent_name or "",
                    "name": item.name, "criteria": item.criteria,
                }
                for item in self.policy_service.list_policy_records()
                if item.approved
            ]
            reviewed = self.repository.reviewed_decisions(limit=50)
            drafts = self.synthesizer(signals, policies, reviewed)
            known_ids = {str(item["id"]) for item in signals}
            known_policy_ids = {item["id"] for item in policies}
            for draft in drafts:
                if any(signal_id not in known_ids for signal_id in draft.signal_ids):
                    raise ValueError("Policy suggestion referenced a signal outside the batch")
                if any(overlap.policy_id not in known_policy_ids for overlap in draft.overlaps):
                    raise ValueError("Policy suggestion referenced an unknown policy overlap")
            suggestions = self.repository.create_suggestions(
                drafts=drafts,
                signal_rows=signals,
                overlaps_by_draft=[
                    [item.model_dump(mode="json") for item in draft.overlaps]
                    for draft in drafts
                ],
            )
            self.repository.finish_daily_synthesis([str(item["id"]) for item in signals])
            return suggestions
        except Exception:
            # A failed provider attempt also consumes today's synthesis slot;
            # leave signals pending for the next day without repeated token use.
            self.repository.finish_daily_synthesis([])
            raise

    def list_suggestions(self, *, status: str | None = "pending", limit: int = 50):
        return self.repository.list_suggestions(status=status, limit=limit)

    def get_suggestion(self, suggestion_id: str):
        result = self.repository.get_suggestion(suggestion_id)
        if result is None:
            raise ValueError("Policy suggestion was not found")
        return result

    def approve(
        self, *, suggestion_id: str, actor_id: str, name: str | None = None,
        criteria: str | None = None,
    ):
        self.policy_service.require_owner_actor(actor_id)
        suggestion, policy = self.repository.approve_suggestion(
            policy_service=self.policy_service, suggestion_id=suggestion_id,
            actor_id=actor_id, name=name, criteria=criteria
        )
        return {"suggestion": suggestion, "policy": {
            "id": policy.id, "scope": policy.scope,
            "target_agent_name": policy.target_agent_name, "name": policy.name,
            "criteria": policy.criteria, "approved": policy.approved,
        }}

    def dismiss(self, *, suggestion_id: str, actor_id: str):
        self.policy_service.require_owner_actor(actor_id)
        return self.repository.dismiss_suggestion(
            suggestion_id=suggestion_id, actor_id=actor_id
        )


class PolicyDiscoveryWorker:
    """Retry sampled gap review and perform at most one daily synthesis."""

    def __init__(self, service: PolicyDiscoveryService, poll_seconds: int = 60):
        self.service = service
        self.poll_seconds = max(10, poll_seconds)

    async def run_forever(self) -> None:
        while True:
            try:
                await asyncio.to_thread(self.service.review_pending_samples)
                await asyncio.to_thread(self.service.synthesize_daily)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - continue scheduled work after provider/database errors.
                logger.error(
                    "Policy discovery worker failed (%s): %s",
                    type(error).__name__, redact_text(str(error))[:500],
                )
            await asyncio.sleep(self.poll_seconds)
