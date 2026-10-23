"""Load approved policies and judge them against captured sessions.

There is no evaluator registry or numeric threshold. Owners approve named rules
in Postgres; Lisa flags suspected policy violations and passes each live policy to the
judge with a compact trajectory.

See ``docs/architecture/lisa-terms.md``: the session ID identifies the input;
the trajectory is a derived projection, and evaluation pairs use the session
ID with the readable policy ID.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import psycopg

from ..context import WorkspaceContext
from ..evaluation.judge import run_evaluation
from ..evaluation.policy_triage import OpenAIPolicyTriager
from ..evaluation.scores import (
    EVALUATION_PIPELINE_VERSION,
    evaluation_run_identity,
)
from ..evaluation.trajectory import build_trajectory_events, session_timestamp
from ..llm_recording import recording_scope
from ..models.activities import Activity, EvaluationRun, ScoreOutboxItem, ActivityClaim
from ..repositories.evaluation_artifacts import EvaluationArtifactRepository
from ..repositories.activities import PostgresActivityStore, StaleLeaseError
from ..security import redact_evidence, redact_text
from .failures import FailureService
from .notifications import SlackNotifier
from .policies import PolicyService
from .sessions import SessionService

logger = logging.getLogger(__name__)


class EvaluationService:
    def __init__(
        self,
        context: WorkspaceContext,
        policies: PolicyService,
        failures: FailureService,
        notifier: SlackNotifier,
        judge=run_evaluation,
        build_events=build_trajectory_events,
        *,
        sessions: SessionService,
        policy_triage: OpenAIPolicyTriager,
        activities: PostgresActivityStore | None = None,
        artifact_repository: EvaluationArtifactRepository | None = None,
        render_context: Callable[[dict], str] | None = None,
    ):
        self.context = context
        self.policies = policies
        self.failures = failures
        self.notifier = notifier
        self.judge = judge
        self.sessions = sessions
        self.activities = activities
        self.policy_triage = policy_triage
        self.artifact_repository = artifact_repository
        self.render_context = render_context

    def _triage_policies_for_session(
        self,
        target_agent_name: str,
        session_id: str,
        session_detail: dict | None = None,
        activity_id: str | None = None,
    ) -> tuple[dict, dict | None]:
        """Triage approved policies for a session and return its compact trajectory.

        Returns ``(triage, trajectory)``. ``trajectory`` is built from session
        detail when available, else ``None``.

        ``session_detail`` is the locally loaded OTLP session projection;
        ``trajectory`` is the compact event list used for triage and judging.
        """
        rules = self.policies.approved_rules(target_agent_name)
        if not rules:
            return (
                {
                    "status": "no_approved_policies",
                    "session_id": session_id,
                    "target_agent_name": target_agent_name,
                    "selected_policies": [],
                },
                None,
            )
        session_detail = session_detail or self._session_detail(session_id)
        if target_agent_name not in session_detail.get("agent_names", []):
            return (
                {
                    "status": "skipped_agent_mismatch",
                    "session_id": session_id,
                    "expected_agent": target_agent_name,
                    "observed_agents": session_detail.get("agent_names", []),
                    "selected_policies": [],
                },
                None,
            )
        trajectory = self._trajectory(session_detail)
        if not trajectory["events"]:
            return (
                {
                    "status": "insufficient_evidence",
                    "session_id": session_id,
                    "target_agent_name": target_agent_name,
                    "selected_policies": [],
                    "limitations": trajectory["limitations"],
                },
                trajectory,
            )
        if not (
            trajectory["complete"]
            and trajectory["tool_order_certain"]
            and trajectory["message_order_certain"]
        ):
            return (
                {
                    "status": "insufficient_evidence",
                    "session_id": session_id,
                    "target_agent_name": target_agent_name,
                    "candidate_count": len(rules),
                    "selected_policies": [],
                    "unresolved_policies": [rule.name for rule in rules],
                    "limitations": trajectory["limitations"],
                },
                trajectory,
            )
        known_decisions = (
            self.activities.policy_triage_decisions(
                session_id, [rule.id for rule in rules]
            )
            if self.activities
            else {}
        )
        unseen_rules = [rule for rule in rules if rule.id not in known_decisions]
        selection = {"selected_policies": [], "policy_decisions": []}
        if unseen_rules:
            # Triage only session-policy pairs that have no completed decision yet.
            with recording_scope(
                self.context, session_id=session_id, activity_id=activity_id
            ):
                selection = self.policy_triage.triage(trajectory, unseen_rules)
            selected_ids = {
                item.get("id") for item in selection.get("selected_policies", [])
            }
            expected_ids = {rule.id for rule in unseen_rules}
            if not selected_ids <= expected_ids:
                unknown_ids = ", ".join(
                    sorted(str(policy_id) for policy_id in selected_ids - expected_ids)
                )
                raise RuntimeError(
                    f"Selected policy {unknown_ids} is not in the approved candidate set"
                )
            decision_items = selection.get("policy_decisions")
            if decision_items is None:
                raise RuntimeError("Policy triage did not return one decision per candidate")
            if (
                len(decision_items) != len(expected_ids)
                or {item.get("id") for item in decision_items} != expected_ids
            ):
                raise RuntimeError("Policy triage did not return one decision per candidate")
            valid_decisions = {
                "suspected_violation",
                "no_violation_found",
                "insufficient_evidence",
            }
            decision_selected_ids = set()
            for item in decision_items:
                if item.get("decision") not in valid_decisions:
                    raise RuntimeError("Policy triage returned an invalid candidate decision")
                if item["decision"] != "no_violation_found":
                    decision_selected_ids.add(item["id"])
            if selected_ids != decision_selected_ids:
                raise RuntimeError("Policy triage selections and decisions do not match")
            if self.activities:
                self.activities.record_policy_triage_decisions(session_id, decision_items)
            known_decisions.update(
                {item["id"]: item["decision"] for item in decision_items}
            )

        if self.activities:
            known_decisions = self.activities.policy_triage_decisions(
                session_id, [rule.id for rule in rules]
            )
            if set(known_decisions) != {rule.id for rule in rules}:
                raise RuntimeError("Policy triage decisions were not durably recorded")

        decision_by_id = {
            rule.id: {
                "id": rule.id,
                "name": rule.name,
                "scope": rule.scope,
                "decision": known_decisions.get(rule.id),
            }
            for rule in rules
        }
        policy_decisions = [decision_by_id[rule.id] for rule in rules]
        selected = [
            {key: decision_by_id[rule.id][key] for key in ("id", "name", "scope")}
            for rule in rules
            if decision_by_id[rule.id]["decision"] != "no_violation_found"
        ]
        return (
            {
                "session_id": session_id,
                "target_agent_name": target_agent_name,
                "candidate_count": len(rules),
                "selected_policies": selected,
                **{
                    key: selection[key]
                    for key in ("limitations", "prompt_truncations", "policy_decisions")
                    if key in selection and key != "policy_decisions"
                },
                "policy_decisions": policy_decisions,
            },
            trajectory,
        )

    def evaluate_session(
        self,
        target_agent_name: str,
        session_id: str,
        *,
        activity_id: str | None = None,
        defer_activity_failure: bool = False,
        defer_activity_finalization: bool = False,
    ) -> dict:
        """Investigate suspected violations and unresolved evidence in one local session."""
        if self.activities:
            pending = self.activities.find_pending_evaluations_for_session(
                target_agent_name, session_id
            )
            if pending:
                recovered = [
                    self._recover_pending_score(
                        *item, defer_activity_finish=defer_activity_finalization
                    )
                    for item in pending
                ]
                recovery_complete = all(item.get("stored") is True for item in recovered)
                result = {
                    "status": (
                        "recovered_pending_evaluations"
                        if recovery_complete
                        else "evaluation_error"
                    ),
                    "session_id": session_id,
                    "target_agent_name": target_agent_name,
                    "evaluations": recovered,
                }
                if not recovery_complete:
                    result["message"] = (
                        "At least one pending evaluation is still owned by another worker. "
                        "Retry after that evaluation finishes."
                    )
                if activity_id and self.activities:
                    summary = (
                        "Recovered the pending evaluation result."
                        if recovery_complete
                        else "Pending evaluation recovery is still in progress."
                    )
                    self.activities.record_activity_event(
                        activity_id,
                        "evaluation_result_recovered"
                        if recovery_complete
                        else "evaluation_recovery_pending",
                        title=(
                            "Evaluation result recovered"
                            if recovery_complete
                            else "Evaluation recovery is still in progress"
                        ),
                        summary=summary,
                    )
                    result["activity_finalized"] = (
                        False
                        if defer_activity_finalization or not recovery_complete
                        else self.activities.finish_activity_without_claim(activity_id, summary)
                    )
                    result["activity_id"] = activity_id
                return result
        try:
            session_detail = self._session_detail(session_id)
        except Exception as error:
            if activity_id and self.activities:
                summary = (
                    f"Session lookup failed for {session_id} "
                    f"({type(error).__name__})."
                )
                self.activities.record_activity_event(
                    activity_id,
                    "evaluation_session_lookup_failed",
                    title="Session lookup failed",
                    summary=summary,
                )
                activity_finalized = (
                    False
                    if defer_activity_failure or defer_activity_finalization
                    else self.activities.finish_activity_without_claim(
                        activity_id, summary, status="failed",
                    )
                )
                return {
                    "status": "error",
                    "session_id": session_id,
                    "target_agent_name": target_agent_name,
                    "message": "Session lookup failed.",
                    "evaluations": [],
                    "activity_id": activity_id,
                    "activity_finalized": activity_finalized,
                }
            raise
        rules = self.policies.approved_rules(target_agent_name)
        if not rules:
            result = {
                "status": "no_approved_policies",
                "session_id": session_id,
                "target_agent_name": target_agent_name,
                "selected_policies": [],
                "evaluations": [],
            }
            if activity_id and self.activities:
                summary = "No approved policies apply to the requested session."
                self.activities.record_activity_event(
                    activity_id, "policy_review_completed",
                    title="Policy review completed", summary=summary,
                )
                activity_finalized = (
                    False
                    if defer_activity_finalization
                    else self.activities.finish_activity_without_claim(activity_id, summary)
                )
                result["activity_finalized"] = activity_finalized
                result["activity_id"] = activity_id
            return result
        activity = None
        if self.activities:
            activity = (
                Activity(id=activity_id, created=False)
                if activity_id
                else self.activities.create_activity(
                    title=f"Evaluate {target_agent_name} session {session_id}",
                    source=self.context.turn_source,
                    kind="policy_evaluation",
                )
            )
        try:
            selection, trajectory = self._triage_policies_for_session(
                target_agent_name,
                session_id,
                session_detail=session_detail,
                activity_id=activity.id if activity else None,
            )
        except Exception as error:
            if activity:
                self.activities.record_activity_event(
                    activity.id,
                    "policy_triage_failed",
                    title="Policy triage failed",
                    summary=f"Policy triage failed with {type(error).__name__}.",
                )
                if not defer_activity_failure and not defer_activity_finalization:
                    self.activities.finish_activity_without_claim(
                        activity.id, "Policy triage failed.", status="failed"
                    )
            raise
        selected = selection["selected_policies"]
        if activity:
            for item in selection.get("policy_decisions", []):
                self.activities.record_activity_event(
                    activity.id,
                    "policy_triage_decision",
                    title=f"Triaged policy {item['id']}: {item.get('name') or item['id']}",
                    summary=item["decision"],
                )
        if not selected:
            if activity:
                status = selection.get("status", "triaged")
                summary = (
                    "Triage completed; no policies required judging."
                    if status == "triaged"
                    else f"Policy evaluation ended with status {status}."
                )
                self.activities.record_activity_event(
                    activity.id,
                    "policy_review_completed",
                    title="Policy review completed",
                    summary=summary,
                )
                activity_finalized = (
                    False
                    if defer_activity_finalization
                    else self.activities.finish_activity_without_claim(activity.id, summary)
                )
            else:
                activity_finalized = None
            return {
                **selection,
                "activity_id": activity.id if activity else None,
                "activity_finalized": activity_finalized,
                "evaluations": [],
            }

        rules_by_id = {rule.id: rule for rule in rules}
        selection_context = {
            "model": self.policy_triage.model,
            "candidate_count": selection["candidate_count"],
            "selected_policies": selected,
            **{
                key: selection[key]
                for key in ("limitations", "prompt_truncations", "policy_decisions")
                if key in selection
            },
        }
        evaluations = []
        for decision in selected:
            rule = rules_by_id.get(decision["id"])
            if rule is None:
                raise RuntimeError(
                    f"Selected policy {decision['id']} is not in the approved candidate set"
                )
            evaluation = self._evaluate_session(
                target_agent_name,
                rule.name,
                session_id,
                policy_id=rule.id,
                _session_detail=session_detail,
                _trajectory=trajectory,
                _selection=selection_context,
                _activity_id=activity.id if activity else None,
                _defer_activity_finish=True,
            )
            fencing_token = evaluation.get("_activity_fencing_token")
            if defer_activity_finalization and fencing_token is not None:
                evaluation.pop("_activity_fencing_token", None)
                self.activities.release_chat_evaluation_claim(
                    evaluation["activity_id"],
                    actor="lisa-evaluation",
                    fencing_token=fencing_token,
                )
            evaluations.append(evaluation)
        finish_claims = {}
        for item in evaluations:
            fencing_token = item.pop("_activity_fencing_token", None)
            if fencing_token is not None and item.get("activity_id"):
                finish_claims[item["activity_id"]] = fencing_token
        evaluation_activity_ids = {activity.id} if activity else set()
        evaluation_activity_ids.update(
            item["activity_id"] for item in evaluations if item.get("activity_id")
        )
        if activity and not defer_activity_finalization and all(
            item.get("status") not in {"error", "already_running", "policy_unavailable"}
            for item in evaluations
        ):
            for evaluation_activity_id in evaluation_activity_ids:
                token = finish_claims.get(evaluation_activity_id)
                self.activities.finish_activity_when_complete(
                    evaluation_activity_id,
                    actor="lisa-evaluation" if token is not None else None,
                    fencing_token=token,
                )
        result = {
            **selection,
            "status": (
                "evaluation_error"
                if any(
                    item.get("status")
                    in {"error", "already_running", "policy_unavailable"}
                    for item in evaluations
                )
                else "evaluated"
            ),
            "activity_id": activity.id if activity else None,
            "evaluations": evaluations,
        }
        if defer_activity_finalization and activity:
            result["activity_finalized"] = False
        return result

    def _policy(
        self,
        target_agent_name: str,
        name: str,
        policy_id: str | None = None,
    ) -> dict:
        rule = next(
            (
                rule
                for rule in self.policies.approved_rules(target_agent_name)
                if rule.name == name and (policy_id is None or rule.id == policy_id)
            ),
            None,
        )
        if rule is None:
            if policy_id is not None:
                raise ValueError("The selected policy ID is not currently approved for this agent")
            raise ValueError("No approved policy with that exact name applies to this agent")
        identity = json.dumps(
            [
                self.context.workspace_id(),
                target_agent_name,
                rule.id,
                rule.scope,
                rule.name,
                rule.criteria,
            ],
            separators=(",", ":"),
        )
        return {
            "name": rule.name,
            "id": rule.id,
            "policy_id": rule.id,
            "target_agent_name": target_agent_name,
            "scope": rule.scope,
            "criteria": rule.criteria,
            "policy_hash": sha256(identity.encode()).hexdigest(),
            "slack_channel_id": self.context.slack_channel or None,
            "slack_thread_ts": self.context.slack_thread or None,
        }

    def _evaluate_session(
        self,
        target_agent_name: str,
        name: str,
        session_id: str,
        policy_id: str | None = None,
        *,
        _session_detail: dict | None = None,
        _trajectory: dict | None = None,
        _selection: dict | None = None,
        _activity_id: str | None = None,
        _defer_activity_finish: bool = False,
    ) -> dict:
        """Evaluate one approved named policy against a session and store its verdict."""
        policy = None
        if self.activities:
            if policy_id is not None:
                pending = self.activities.find_pending_evaluation_for_policy(
                    policy_id, target_agent_name, session_id
                )
                if pending:
                    self._validate_pending_identity(pending, target_agent_name, name, policy_id)
                    return self._recover_pending_score(
                        *pending, defer_activity_finish=_defer_activity_finish
                    )
            else:
                policy = self._policy(target_agent_name, name)
                pending = self.activities.find_pending_evaluation_for_policy(
                    policy["policy_id"],
                    target_agent_name,
                    session_id,
                )
                if pending:
                    return self._recover_pending_score(
                        *pending, defer_activity_finish=_defer_activity_finish
                    )
        policy = policy or self._policy(target_agent_name, name, policy_id)
        if self.artifact_repository is None:
            raise RuntimeError("Evaluation artifact repository is not configured")
        with self.artifact_repository.lock(policy["policy_hash"], session_id):
            return self._evaluate_session_locked(
                policy,
                session_id,
                session_detail_override=_session_detail,
                trajectory_override=_trajectory,
                selection=_selection,
                activity_id=_activity_id,
                defer_activity_finish=_defer_activity_finish,
            )

    @staticmethod
    def _validate_pending_identity(
        pending: tuple[EvaluationRun, ScoreOutboxItem],
        target_agent_name: str,
        name: str,
        policy_id: str | None = None,
    ) -> None:
        payload = pending[1].score_payload
        metadata = payload.get("metadata") or {}
        if payload.get("name") != name:
            raise ValueError("The selected policy name does not match the pending policy")
        recorded_agent = metadata.get("target_agent_name")
        if recorded_agent is not None and recorded_agent != target_agent_name:
            raise ValueError("The selected target agent does not match the pending policy")
        recorded_policy_id = metadata.get("policy_id")
        persisted_policy_id = pending[0].policy_id
        if (
            policy_id is not None
            and policy_id != persisted_policy_id
            and recorded_policy_id not in (None, policy_id)
        ):
            raise ValueError("The selected policy ID does not match the pending policy")

    def _recover_pending_score(
        self, activity, outbox, *, defer_activity_finish: bool = False,
    ) -> dict:
        assert self.activities is not None
        owner = "lisa-evaluation"
        claim = self.activities.claim_activity(activity.activity_id, owner, lease_seconds=300)
        if claim is None:
            return {
                "status": "already_running",
                "session_id": activity.session_id,
                "stored": False,
                "activity_id": activity.activity_id,
                "evaluation_id": activity.id,
            }
        with self._lease_heartbeat(activity.activity_id, owner, claim):
            return self._recover_pending_score_claimed(
                activity, outbox, owner, claim,
                defer_activity_finish=defer_activity_finish,
            )

    def _recover_pending_score_claimed(
        self,
        activity: EvaluationRun,
        outbox: ScoreOutboxItem,
        owner: str,
        claim: ActivityClaim,
        *,
        defer_activity_finish: bool = False,
    ) -> dict:
        payload = outbox.score_payload
        result = self._result_from_score_payload(payload)
        metadata = payload.get("metadata") or {}
        failure_policy = {
            "name": payload["name"],
            "id": metadata.get("policy_id", ""),
            "policy_id": metadata.get("policy_id", ""),
            "target_agent_name": metadata["target_agent_name"],
            "policy_hash": metadata["policy_hash"],
            "scope": metadata["policy_scope"],
            "criteria": metadata.get("criteria", ""),
            "slack_channel_id": metadata.get("slack_channel_id") or None,
            "slack_thread_ts": metadata.get("slack_thread_ts") or None,
        }
        if result["status"] == "fail":
            self.failures.record(
                failure_policy,
                activity.session_id,
                payload["trace_id"],
                str(result.get("rationale") or "Policy failed."),
                payload.get("timestamp"),
            )
        self.activities.acknowledge_score(
            activity.activity_id,
            activity.id,
            outbox.id,
            score_id=payload["score_id"],
            verdict=payload["string_value"],
            actor=owner,
            fencing_token=claim.fencing_token,
            summary=str(payload.get("comment") or payload["string_value"]),
            auto_finish=not defer_activity_finish,
        )
        if defer_activity_finish:
            self.activities.release_chat_evaluation_claim(
                activity.activity_id,
                actor=owner,
                fencing_token=claim.fencing_token,
            )
        return {
            **result,
            "evaluation_name": payload["name"],
            "session_id": activity.session_id,
            "stored": True,
            "new_score": False,
            "activity_id": activity.activity_id,
            "evaluation_id": activity.id,
        }

    def _evaluate_session_locked(
        self,
        policy: dict,
        session_id: str,
        *,
        session_detail_override: dict | None = None,
        trajectory_override: dict | None = None,
        selection: dict | None = None,
        activity_id: str | None = None,
        defer_activity_finish: bool = False,
    ) -> dict:
        # The local session service returns bounded span evidence. Redact secrets
        # before building the trajectory and forwarding anything to a provider.
        session_detail = session_detail_override or self._session_detail(session_id)
        if policy["target_agent_name"] not in session_detail.get("agent_names", []):
            return {
                "status": "skipped_agent_mismatch",
                "session_id": session_id,
                "expected_agent": policy["target_agent_name"],
                "observed_agents": session_detail.get("agent_names", []),
                "stored": False,
            }
        spans = session_detail.get("spans", [])
        trajectory = trajectory_override or self._trajectory(session_detail)
        # Current sampling evaluates one session trajectory and associates its
        # result with the first captured trace; it is not a per-trace coverage loop.
        trace_id = next(iter(trajectory["trace_ids"]), "")
        if not trace_id:
            return {"status": "insufficient_evidence", "session_id": session_id, "stored": False}
        judge_model = os.getenv("GEMINI_MODEL_NAME", "gemini-3.8-flash")
        trajectory_hash, run_id, request_key = evaluation_run_identity(
            session_id=session_id,
            policy_hash=policy["policy_hash"],
            trajectory=trajectory,
            triage_model=self.policy_triage.model,
            judge_model=judge_model,
        )
        policy["trajectory_hash"] = trajectory_hash
        existing = (
            self.activities.get_evaluation_by_request(
                policy["policy_hash"], session_id, request_key
            )
            if self.activities
            else None
        )
        outbox = self.activities.get_score_outbox(existing.id) if existing else None
        pending = (
            (existing, outbox)
            if existing and outbox and outbox.status == "pending"
            else None
        )
        # Lisa's Postgres request identity is the dedupe authority for local results.
        if existing and existing.status == "succeeded" and not pending:
            if activity_id:
                self.activities.record_activity_event(
                    activity_id,
                    "evaluation_reused",
                    title=f"Reused result for {policy['name']}",
                    summary=f"Existing evaluation {existing.id} has verdict {existing.verdict}.",
                )
            return {
                "status": "already_evaluated",
                "verdict": existing.verdict,
                "session_id": session_id,
                "stored": True,
                "activity_id": activity_id or existing.activity_id,
                "evaluation_id": existing.id,
            }
        if not trajectory["events"] and not pending:
            return {"status": "insufficient_evidence", "session_id": session_id, "stored": False}
        if self.activities:
            activity_id = activity_id or str(uuid4())
            if not self.activities.activity_exists(activity_id):
                self.activities.create_activity(
                    title=f"Evaluate {policy['name']} for {policy['target_agent_name']}",
                    source=self.context.turn_source,
                    kind="policy_evaluation",
                    activity_id=activity_id,
                    context_path=f"activities/{activity_id}",
                )
            return self._evaluate_tracked(
                policy,
                session_id,
                trace_id,
                spans,
                trajectory,
                run_id,
                request_key,
                existing,
                pending[1] if pending else None,
                selection,
                activity_id,
                defer_activity_finish,
            )

        return self._evaluate_untracked(
            policy, session_id, trace_id, spans, trajectory, run_id, selection
        )

    def _run_judge(self, request: dict, *, session_id: str, activity_id: str | None = None):
        with recording_scope(self.context, session_id=session_id, activity_id=activity_id):
            return self.judge(request)

    def _evaluate_untracked(
        self,
        policy: dict,
        session_id: str,
        trace_id: str,
        spans: list,
        trajectory: dict,
        run_id: str,
        selection: dict | None = None,
    ) -> dict:
        limitations = list(trajectory["limitations"])
        limitations.extend((selection or {}).get("limitations", []))
        result = redact_evidence(
            self._run_judge(
                self._judge_request(run_id, policy, trajectory, trace_id), session_id=session_id
            )
        )
        # Provider/validation errors are not verdicts. Leave the ledger unchanged
        # so Lisa can retry later; incomplete evidence is a stored, explicit outcome.
        if result.get("status") == "error":
            return {
                **result,
                "evaluation_name": policy["name"],
                "session_id": session_id,
                "stored": False,
            }
        limitations.extend(result.get("limitations", []))
        timestamp = session_timestamp(spans)
        payload = self._score_payload(
            policy, session_id, trace_id, run_id, timestamp, result, limitations
        )
        # Save the failure and score payload before the remote write. Recovery can
        # finish an interrupted write after restart without invoking the judge again.
        if result["status"] == "fail":
            self.failures.record(
                policy,
                session_id,
                trace_id,
                str(result.get("rationale") or "Policy failed."),
                timestamp,
            )
        return {
            **result,
            "limitations": list(dict.fromkeys(limitations)),
            "evaluation_name": policy["name"],
            "session_id": session_id,
            "stored": True,
            "new_score": False,
        }

    def _evaluate_tracked(
        self,
        policy: dict,
        session_id: str,
        trace_id: str,
        spans: list,
        trajectory: dict,
        run_id: str,
        request_key: str,
        existing: EvaluationRun | None,
        outbox,
        selection: dict | None = None,
        requested_activity_id: str | None = None,
        defer_activity_finish: bool = False,
    ) -> dict:
        assert self.activities is not None
        if self.artifact_repository is None or self.render_context is None:
            raise RuntimeError("Evaluation artifact storage and context view are not configured")
        activity_id = (
            existing.activity_id if existing else (requested_activity_id or str(uuid4()))
        )
        context_path = existing.context_path if existing and existing.context_path else f"activities/{activity_id}"
        if not self.activities.activity_exists(activity_id):
            self.activities.create_activity(
                title=f"Evaluate {policy['name']} for {policy['target_agent_name']}",
                source=self.context.turn_source,
                kind="policy_evaluation",
                activity_id=activity_id,
                context_path=context_path,
            )
        run_dir = self.artifact_repository.prepare_run_directory(context_path, activity_id)
        evaluation = self.activities.create_evaluation(
            title=f"Evaluate {policy['name']} for {policy['target_agent_name']}",
            source=self.context.turn_source,
            target_agent_name=policy["target_agent_name"],
            session_id=session_id,
            policy_name=policy["name"],
            policy_id=policy["policy_id"],
            policy_hash=policy["policy_hash"],
            judge_config={
                "pipeline_version": EVALUATION_PIPELINE_VERSION,
                "triage_model": self.policy_triage.model,
                "judge_model": os.getenv("GEMINI_MODEL_NAME", "gemini-3.8-flash"),
            },
            request_key=request_key,
            activity_id=activity_id,
            context_path=context_path,
        )
        activity_id = evaluation.activity_id
        if requested_activity_id and requested_activity_id != activity_id:
            self.activities.record_activity_event(
                requested_activity_id,
                "evaluation_resumed",
                title=f"Resumed evaluation for {policy['name']}",
                summary=f"Evaluation {evaluation.id} continues on activity {activity_id}.",
            )
        owner = "lisa-evaluation"
        claim = self.activities.claim_activity(activity_id, owner, lease_seconds=300)
        if claim is None:
            return {
                "status": "already_running",
                "session_id": session_id,
                "stored": False,
                "activity_id": activity_id,
                "evaluation_id": evaluation.id,
            }
        with self._lease_heartbeat(activity_id, owner, claim):
            return self._evaluate_tracked_claimed(
                policy,
                session_id,
                trace_id,
                spans,
                trajectory,
                run_id,
                evaluation,
                outbox,
                selection,
                requested_activity_id,
                defer_activity_finish,
                activity_id,
                owner,
                claim,
                run_dir,
            )

    def _evaluate_tracked_claimed(
        self,
        policy: dict,
        session_id: str,
        trace_id: str,
        spans: list,
        trajectory: dict,
        run_id: str,
        evaluation: EvaluationRun,
        outbox,
        selection: dict | None,
        requested_activity_id: str | None,
        defer_activity_finish: bool,
        activity_id: str,
        owner: str,
        claim: ActivityClaim,
        run_dir: Path,
    ) -> dict:
        assert self.activities is not None

        context_kind = f"evaluation_inputs_{policy['policy_id']}"
        saved = self.activities.get_context_item_for_activity(activity_id, context_kind)
        if saved:
            inputs = saved.content
            trajectory = inputs["trajectory"]
            trace_id = inputs["trace_id"]
            selection = inputs.get("policy_selection")
        else:
            inputs = {
                "session_id": session_id,
                "trace_id": trace_id,
                "policy_id": policy["policy_id"],
                "policy_name": policy["name"],
                "target_agent_name": policy["target_agent_name"],
                "trajectory": trajectory,
                "policy_selection": selection,
            }
            self.activities.save_context_item(
                activity_id,
                context_kind,
                inputs,
                actor=owner,
                fencing_token=claim.fencing_token,
                source_ref=f"session:{session_id}",
            )
        self.artifact_repository.save_inputs(
            run_dir, inputs, self.render_context(inputs), artifact_key=policy["policy_id"]
        )

        if outbox is None:
            trajectory_hash = policy.get("trajectory_hash")
            try:
                policy = self._policy(
                    inputs["target_agent_name"], inputs["policy_name"], inputs["policy_id"]
                )
                policy["trajectory_hash"] = trajectory_hash
            except ValueError as error:
                self._release_retryable(activity_id, owner, claim, str(error))
                return {
                    "status": "policy_unavailable", "rationale": str(error),
                    "session_id": session_id, "stored": False,
                    "activity_id": activity_id,
                    "evaluation_id": evaluation.id,
                }
            self.activities.append_event(
                activity_id, "judging_started", title=f"Judging {policy['name']}",
                actor=owner, fencing_token=claim.fencing_token,
            )
            try:
                result = redact_evidence(
                    self._run_judge(
                        self._judge_request(run_id, policy, trajectory, trace_id),
                        session_id=session_id, activity_id=activity_id,
                    )
                )
            except Exception:
                self._release_retryable(activity_id, owner, claim, "Judge call interrupted")
                raise
            if result.get("status") == "error":
                self._release_retryable(
                    activity_id, owner, claim, str(result.get("rationale") or "Judge unavailable")
                )
                return {
                    **result, "evaluation_name": policy["name"], "session_id": session_id,
                    "stored": False, "activity_id": activity_id,
                    "evaluation_id": evaluation.id,
                }
            limitations = list(trajectory.get("limitations", []))
            limitations.extend((selection or {}).get("limitations", []))
            limitations.extend(result.get("limitations", []))
            payload = self._score_payload(
                policy, session_id, trace_id, run_id, session_timestamp(spans), result, limitations
            )
            if result["status"] == "fail":
                self.failures.record(
                    policy, session_id, trace_id,
                    str(result.get("rationale") or "Policy failed."),
                    payload["timestamp"],
                )
            outbox = self.activities.prepare_score_outbox(
                evaluation.id, payload, actor=owner, fencing_token=claim.fencing_token
            )
        else:
            payload = outbox.score_payload
            result = self._result_from_score_payload(payload)
            if result["status"] == "fail":
                self.failures.record(
                    policy, session_id, trace_id,
                    str(result.get("rationale") or "Policy failed."),
                    payload.get("timestamp"),
                )

        self.activities.acknowledge_score(
            activity_id, evaluation.id, outbox.id,
            score_id=payload["score_id"], verdict=payload["string_value"], actor=owner,
            fencing_token=claim.fencing_token,
            summary=str(payload.get("comment") or payload["string_value"]),
            auto_finish=not defer_activity_finish,
        )
        return {
            **result,
            "limitations": self._result_from_score_payload(payload)["limitations"],
            "evaluation_name": policy["name"], "session_id": session_id,
            "stored": True, "new_score": False,
            "activity_id": activity_id,
            "evaluation_id": evaluation.id,
            "_activity_fencing_token": claim.fencing_token,
        }

    def _score_payload(
        self,
        policy: dict,
        session_id: str,
        trace_id: str,
        run_id: str,
        timestamp: str,
        result: dict,
        limitations: list[str],
    ) -> dict:
        return {
            "score_id": run_id,
            "timestamp": timestamp,
            "trace_id": trace_id,
            "session_id": session_id,
            "name": policy["name"],
            "data_type": "categorical",
            "numeric_value": None,
            "string_value": result["status"],
            "boolean_value": None,
            "source": "evaluator",
            "comment": redact_text(str(result.get("rationale", ""))),
            "config_id": None,
            "author_id": None,
            "metadata": {
                "policy_hash": policy["policy_hash"],
                "trajectory_hash": policy.get("trajectory_hash", ""),
                "policy_id": policy["policy_id"],
                "policy_scope": policy["scope"],
                "criteria": policy["criteria"],
                "target_agent_name": policy["target_agent_name"],
                "framework": result.get("framework", "gemini"),
                "references": json.dumps(result.get("references", [])),
                "limitations": json.dumps(limitations),
                "slack_channel_id": policy.get("slack_channel_id") or "",
                "slack_thread_ts": policy.get("slack_thread_ts") or "",
            },
        }

    def _judge_request(self, run_id: str, policy: dict, trajectory: dict, trace_id: str) -> dict:
        return {
            "run_id": run_id,
            "policy": {
                "name": policy["name"],
                "scope": policy["scope"],
                "approved": True,
                "criteria": policy["criteria"],
            },
            "trajectory": {
                "trace_id": trace_id,
                "events": trajectory["events"],
                "complete": trajectory["complete"],
                "tool_events_captured": trajectory["tool_events_captured"],
                "tool_order_certain": trajectory["tool_order_certain"],
                "message_order_certain": trajectory["message_order_certain"],
                "limitations": trajectory["limitations"],
            },
        }

    def _result_from_score_payload(self, payload: dict) -> dict:
        metadata = payload.get("metadata") or {}
        try:
            references = json.loads(metadata.get("references", "[]"))
        except json.JSONDecodeError:
            references = []
        try:
            limitations = json.loads(metadata.get("limitations", "[]"))
        except json.JSONDecodeError:
            limitations = []
        return {
            "status": payload["string_value"],
            "rationale": payload.get("comment") or "",
            "references": references,
            "limitations": limitations,
            "framework": metadata.get("framework", "gemini"),
        }

    def _release_retryable(
        self, activity_id: str, owner: str, claim: ActivityClaim, reason: str
    ) -> None:
        if self.activities:
            try:
                self.activities.release_activity(
                    activity_id,
                    actor=owner,
                    fencing_token=claim.fencing_token,
                    reason=reason,
                )
            except StaleLeaseError:
                # The lease may have expired; recovery can reclaim it by fencing token.
                return

    @contextmanager
    def _lease_heartbeat(
        self, activity_id: str, owner: str, claim: ActivityClaim
    ) -> Iterator[None]:
        """Renew a claimed lease while its operation runs, then stop the worker."""
        assert self.activities is not None
        stop = threading.Event()

        def renew() -> None:
            while not stop.wait(60):
                try:
                    current = self.activities.renew_lease(
                        activity_id,
                        owner,
                        claim.fencing_token,
                        lease_seconds=300,
                    )
                except (psycopg.Error, RuntimeError) as error:
                    logger.warning("Evaluation lease renewal failed (%s)", type(error).__name__)
                    return
                if current is None:
                    return

        thread = threading.Thread(
            target=renew,
            name=f"lisa-evaluation-lease-{activity_id}",
            daemon=True,
        )
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join()

    def _session_detail(self, session_id: str) -> dict:
        return self.sessions.detail(session_id)

    def _trajectory(self, session_detail: dict) -> dict:
        """Build the shared compact session trajectory."""
        return self.sessions.trajectory(session_detail)

    def get_session_trajectory(self, session_id: str) -> dict:
        """Fetch the compact trajectory policy triage and the judge use."""
        return self.sessions.get_trajectory(session_id)

    def notify_evaluation_failure(
        self, target_agent_name: str, name: str, session_id: str, failure_key: str = ""
    ) -> dict:
        """Notify Slack only after Lisa reviews a stored policy failure."""
        # Stored history, not today's policy row, identifies pending failures.
        # This also permits finishing an older pending failure after a rule is edited or paused.
        pending = self.failures.reconcile()
        policy = next(
            (
                item
                for item in pending
                if (not failure_key or self.failures.key(item) == failure_key)
                and item.get("target_agent_name") == target_agent_name
                and item.get("name") == name
                and item.get("session_id") == session_id
            ),
            None,
        )
        if policy is None:
            return {"status": "not_pending", "session_id": session_id}
        trace_id = str(policy.get("trace_id") or "")
        return self.notifier.notify(
            policy,
            {"status": "fail", "rationale": policy.get("rationale", "Policy failed.")},
            session_id,
            trace_id,
        )
