"""Load approved policies and judge them against captured sessions.

There is no evaluator registry or numeric threshold. Owners approve named rules
in Postgres; Lisa selects applicable rules and passes each live policy to the
judge with a compact trajectory.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from collections.abc import Callable
from hashlib import sha256
from uuid import NAMESPACE_URL, uuid4, uuid5

import psycopg

from ..context import WorkspaceContext
from ..evaluation.judge import run_evaluation
from ..evaluation.policy_selection import OpenAIPolicySelector
from ..evaluation.scores import (
    matches_policy_score,
    next_evaluation_score_id,
    successful_policy_score,
)
from ..evaluation.trajectory import build_trajectory_events, session_timestamp
from ..llm_recording import recording_scope
from ..models.work_items import EvaluationWorkItem, ScoreOutboxItem, WorkItemClaim
from ..repositories.evaluation_artifacts import EvaluationArtifactRepository
from ..repositories.work_items import PostgresWorkItemStore, StaleLeaseError
from ..security import redact_evidence, redact_text
from ..thelake_client import TheLakeClient
from .failures import FailureService
from .notifications import SlackNotifier
from .policies import PolicyService
from .sessions import SessionService

logger = logging.getLogger(__name__)


class EvaluationService:
    def __init__(
        self,
        context: WorkspaceContext,
        client: TheLakeClient,
        policies: PolicyService,
        failures: FailureService,
        notifier: SlackNotifier,
        judge=run_evaluation,
        build_events=build_trajectory_events,
        *,
        sessions: SessionService,
        policy_selector: OpenAIPolicySelector,
        work_items: PostgresWorkItemStore | None = None,
        artifact_repository: EvaluationArtifactRepository | None = None,
        render_context: Callable[[dict], str] | None = None,
    ):
        self.context = context
        self.client = client
        self.policies = policies
        self.failures = failures
        self.notifier = notifier
        self.judge = judge
        self.sessions = sessions
        self.work_items = work_items
        self.policy_selector = policy_selector
        self.artifact_repository = artifact_repository
        self.render_context = render_context

    def _select_policies_for_session(
        self, target_agent_name: str, session_id: str, session_detail: dict | None = None
    ) -> tuple[dict, dict | None]:
        """Prepare one session for policy routing and coordinate the selection step.

        Returns ``(selection, trajectory)``. ``selection`` is the public result;
        ``trajectory`` is built from session detail when available, else ``None``.

        ``session_detail`` is the raw TheLake session GET; ``trajectory`` is the
        compact event list used for selection and judging.
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
        # The selector answers “which rules apply?”; it does not judge compliance.
        with recording_scope(self.context, session_id=session_id):
            selection = self.policy_selector.select(trajectory, rules)
        return (
            {
                "session_id": session_id,
                "target_agent_name": target_agent_name,
                "candidate_count": len(rules),
                "selected_policies": selection["selected_policies"],
                "minimum_confidence": selection["minimum_confidence"],
                **{key: selection[key] for key in ('limitations', 'prompt_truncations')
                   if key in selection},
            },
            trajectory,
        )

    def evaluate_session(self, target_agent_name: str, session_id: str) -> dict:
        """Route one captured session to applicable rules and judge each result."""
        if self.work_items:
            pending = self.work_items.find_pending_evaluations_for_session(
                target_agent_name, session_id
            )
            if pending:
                recovered = [self._recover_pending_score(*item) for item in pending]
                return {
                    "status": "recovered_pending_evaluations",
                    "session_id": session_id,
                    "target_agent_name": target_agent_name,
                    "evaluations": recovered,
                }
        rules = self.policies.approved_rules(target_agent_name)
        if not rules:
            return {
                "status": "no_approved_policies",
                "session_id": session_id,
                "target_agent_name": target_agent_name,
                "selected_policies": [],
                "evaluations": [],
            }
        session_detail = self._session_detail(session_id)
        selection, trajectory = self._select_policies_for_session(
            target_agent_name, session_id, session_detail=session_detail
        )
        selected = selection["selected_policies"]
        if not selected:
            return {**selection, "evaluations": []}

        rules_by_id = {rule.id: rule for rule in rules}
        selection_context = {
            "model": self.policy_selector.model,
            "minimum_confidence": selection["minimum_confidence"],
            "candidate_count": selection["candidate_count"],
            "selected_policies": selected,
            **{key: selection[key] for key in ('limitations', 'prompt_truncations')
               if key in selection},
        }
        evaluations = []
        for decision in selected:
            rule = rules_by_id.get(decision["id"])
            if rule is None:
                raise RuntimeError(
                    f"Selected policy {decision['id']} is not in the approved candidate set"
                )
            evaluations.append(
                self._evaluate_session(
                    target_agent_name,
                    rule.name,
                    session_id,
                    policy_id=rule.id,
                    _session_detail=session_detail,
                    _trajectory=trajectory,
                    _selection=selection_context,
                )
            )
        return {
            **selection,
            "status": (
                "evaluation_error"
                if any(item.get("status") in {"error", "already_running"} for item in evaluations)
                else "evaluated"
            ),
            "evaluations": evaluations,
        }

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

    def _evaluation_score_id(self, policy: dict, trace_id: str) -> str:
        return sha256(f"{policy['policy_hash']}:{trace_id}".encode()).hexdigest()

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
    ) -> dict:
        """Evaluate one approved named policy against a session and store its verdict."""
        policy = None
        if self.work_items:
            if policy_id is not None:
                pending = self.work_items.find_pending_evaluation_for_policy(
                    policy_id, target_agent_name, session_id
                )
                if pending:
                    self._validate_pending_identity(pending, target_agent_name, name, policy_id)
                    return self._recover_pending_score(*pending)
            else:
                policy = self._policy(target_agent_name, name)
                pending = self.work_items.find_pending_evaluation_for_policy(
                    policy["policy_id"],
                    target_agent_name,
                    session_id,
                )
                if pending:
                    return self._recover_pending_score(*pending)
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
            )

    @staticmethod
    def _validate_pending_identity(
        pending: tuple[EvaluationWorkItem, ScoreOutboxItem],
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

    def _recover_pending_score(self, work_item, outbox) -> dict:
        assert self.work_items is not None
        owner = "lisa-evaluation"
        claim = self.work_items.claim_work_item(work_item.id, owner, lease_seconds=300)
        if claim is None:
            return {
                "status": "already_running",
                "session_id": work_item.session_id,
                "stored": False,
                "work_item_id": work_item.id,
            }
        self._start_lease_heartbeat(work_item.id, owner, claim)
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
                work_item.session_id,
                payload["trace_id"],
                str(result.get("rationale") or "Policy failed."),
                payload.get("timestamp"),
                score_payload=payload,
            )
        try:
            _, status_code = self.client.post("/v1/scores", payload, include_status=True)
        except Exception:
            self._release_retryable(work_item.id, owner, claim, "TheLake score write interrupted")
            raise
        self.work_items.acknowledge_score(
            work_item.id,
            outbox.id,
            score_id=payload["score_id"],
            verdict=payload["string_value"],
            actor=owner,
            fencing_token=claim.fencing_token,
            summary=str(payload.get("comment") or payload["string_value"]),
        )
        if result["status"] == "fail":
            self.failures.mark_ready(failure_policy, work_item.session_id, payload["trace_id"])
        return {
            **result,
            "evaluation_name": payload["name"],
            "session_id": work_item.session_id,
            "stored": True,
            "new_score": status_code == 201,
            "work_item_id": work_item.id,
        }

    def _evaluate_session_locked(
        self,
        policy: dict,
        session_id: str,
        *,
        session_detail_override: dict | None = None,
        trajectory_override: dict | None = None,
        selection: dict | None = None,
    ) -> dict:
        # TheLake returns session detail with a bounded span list. Redact secrets before
        # building the trajectory and before anything can be forwarded to the provider.
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
        scores = session_detail.get("scores", [])
        base_run_id = self._evaluation_score_id(policy, trace_id)
        pending = (
            self.work_items.find_pending_evaluation(policy["policy_hash"], session_id)
            if self.work_items
            else None
        )
        run_id = pending[1].score_payload["score_id"] if pending else base_run_id
        if not pending and any(score.get("score_id") == run_id for score in scores):
            run_id = next_evaluation_score_id(run_id, scores)
        request_key = str(uuid5(NAMESPACE_URL, f"lisa-evaluation:{run_id}"))
        existing = (
            self.work_items.get_evaluation_by_request(
                policy["policy_hash"], session_id, request_key
            )
            if self.work_items and not pending
            else pending[0]
            if pending
            else None
        )
        # Reuse a stored non-error outcome for the same policy and trace.
        completed = successful_policy_score(scores, policy, trace_id)
        if completed and not pending:
            if completed.get("string_value") == "fail":
                metadata = completed.get("metadata") or {}
                failure_policy = {
                    **policy,
                    "slack_channel_id": metadata.get("slack_channel_id") or None,
                    "slack_thread_ts": metadata.get("slack_thread_ts") or None,
                }
                self.failures.record(
                    failure_policy,
                    session_id,
                    trace_id,
                    str(completed.get("comment") or "Policy failed."),
                    completed.get("timestamp"),
                )
            result = {
                "status": "already_evaluated",
                "verdict": completed.get("string_value"),
                "rationale": completed.get("comment"),
                "timestamp": completed.get("timestamp"),
                "session_id": session_id,
                "stored": True,
            }
            if existing:
                result["work_item_id"] = existing.id
            return result
        if existing and existing.status == "succeeded" and not pending:
            return {
                "status": "already_evaluated",
                "verdict": existing.verdict,
                "session_id": session_id,
                "stored": True,
                "work_item_id": existing.id,
            }
        if not trajectory["events"] and not pending:
            return {"status": "insufficient_evidence", "session_id": session_id, "stored": False}
        if self.work_items:
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
            )

        return self._evaluate_untracked(
            policy, session_id, trace_id, spans, trajectory, run_id, selection
        )

    def _run_judge(self, request: dict, *, session_id: str, work_item_id: str | None = None):
        with recording_scope(self.context, session_id=session_id, work_item_id=work_item_id):
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
                score_payload=payload,
            )
        _, status_code = self.client.post("/v1/scores", payload, include_status=True)
        if result["status"] == "fail":
            self.failures.mark_ready(policy, session_id, trace_id)
        return {
            **result,
            "limitations": list(dict.fromkeys(limitations)),
            "evaluation_name": policy["name"],
            "session_id": session_id,
            "stored": True,
            "new_score": status_code == 201,
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
        existing: EvaluationWorkItem | None,
        outbox,
        selection: dict | None = None,
    ) -> dict:
        assert self.work_items is not None
        if self.artifact_repository is None or self.render_context is None:
            raise RuntimeError("Evaluation artifact storage and context view are not configured")
        work_item_id = existing.id if existing else str(uuid4())
        context_path = (
            existing.context_path
            if existing and existing.context_path
            else f"work-items/{work_item_id}"
        )
        run_dir = self.artifact_repository.prepare_run_directory(context_path, work_item_id)
        work_item = self.work_items.create_evaluation(
            title=f"Evaluate {policy['name']} for {policy['target_agent_name']}",
            source=self.context.turn_source,
            target_agent_name=policy["target_agent_name"],
            session_id=session_id,
            policy_name=policy["name"],
            policy_id=policy["policy_id"],
            policy_hash=policy["policy_hash"],
            judge_config={"model": os.getenv("GEMINI_MODEL_NAME", "gemini-3.8-flash")},
            request_key=request_key,
            work_item_id=work_item_id,
            context_path=context_path,
        )
        if work_item.id != work_item_id:
            run_dir = self.artifact_repository.prepare_run_directory(
                work_item.context_path or f"work-items/{work_item.id}", work_item.id
            )
        owner = "lisa-evaluation"
        claim = self.work_items.claim_work_item(work_item.id, owner, lease_seconds=300)
        if claim is None:
            return {
                "status": "already_running",
                "session_id": session_id,
                "stored": False,
                "work_item_id": work_item.id,
            }
        self._start_lease_heartbeat(work_item.id, owner, claim)

        saved = self.work_items.get_context_item_for_work_item(
            work_item.id, "evaluation_inputs"
        )
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
            self.work_items.save_context_item(
                work_item.id,
                "evaluation_inputs",
                inputs,
                actor=owner,
                fencing_token=claim.fencing_token,
                source_ref=f"session:{session_id}",
            )
        self.artifact_repository.save_inputs(run_dir, inputs, self.render_context(inputs))

        if outbox is None:
            # Load the live approved rule for judging; do not copy policy text into the run.
            try:
                policy = self._policy(
                    inputs["target_agent_name"],
                    inputs["policy_name"],
                    inputs["policy_id"],
                )
            except ValueError as error:
                self._release_retryable(work_item.id, owner, claim, str(error))
                return {
                    "status": "policy_unavailable",
                    "rationale": str(error),
                    "session_id": session_id,
                    "stored": False,
                    "work_item_id": work_item.id,
                }
            self.work_items.append_event(
                work_item.id,
                "judging_started",
                title="Judging session trajectory",
                actor=owner,
                fencing_token=claim.fencing_token,
            )
            try:
                result = redact_evidence(
                    self._run_judge(
                        self._judge_request(run_id, policy, trajectory, trace_id),
                        session_id=session_id, work_item_id=work_item.id,
                    )
                )
            except Exception:
                self._release_retryable(work_item.id, owner, claim, "Judge call interrupted")
                raise
            if result.get("status") == "error":
                self._release_retryable(
                    work_item.id, owner, claim, str(result.get("rationale") or "Judge unavailable")
                )
                return {
                    **result,
                    "evaluation_name": policy["name"],
                    "session_id": session_id,
                    "stored": False,
                    "work_item_id": work_item.id,
                }
            limitations = list(trajectory.get("limitations", []))
            limitations.extend((selection or {}).get("limitations", []))
            limitations.extend(result.get("limitations", []))
            payload = self._score_payload(
                policy,
                session_id,
                trace_id,
                run_id,
                session_timestamp(spans),
                result,
                limitations,
            )
            if result["status"] == "fail":
                self.failures.record(
                    policy,
                    session_id,
                    trace_id,
                    str(result.get("rationale") or "Policy failed."),
                    payload["timestamp"],
                    score_payload=payload,
                )
            outbox = self.work_items.prepare_score_outbox(
                work_item.id,
                payload,
                actor=owner,
                fencing_token=claim.fencing_token,
            )
        else:
            payload = outbox.score_payload
            result = self._result_from_score_payload(payload)
            if result["status"] == "fail":
                self.failures.record(
                    policy,
                    session_id,
                    trace_id,
                    str(result.get("rationale") or "Policy failed."),
                    payload.get("timestamp"),
                    score_payload=payload,
                )

        try:
            _, status_code = self.client.post("/v1/scores", payload, include_status=True)
        except Exception:
            self._release_retryable(work_item.id, owner, claim, "TheLake score write interrupted")
            raise
        self.work_items.acknowledge_score(
            work_item.id,
            outbox.id,
            score_id=payload["score_id"],
            verdict=payload["string_value"],
            actor=owner,
            fencing_token=claim.fencing_token,
            summary=str(payload.get("comment") or payload["string_value"]),
        )
        if result["status"] == "fail":
            self.failures.mark_ready(policy, session_id, trace_id)
        return {
            **result,
            "limitations": self._result_from_score_payload(payload)["limitations"],
            "evaluation_name": policy["name"],
            "session_id": session_id,
            "stored": True,
            "new_score": status_code == 201,
            "work_item_id": work_item.id,
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
        self, work_item_id: str, owner: str, claim: WorkItemClaim, reason: str
    ) -> None:
        if self.work_items:
            try:
                self.work_items.release_work_item(
                    work_item_id,
                    actor=owner,
                    fencing_token=claim.fencing_token,
                    reason=reason,
                )
            except StaleLeaseError:
                # The lease may have expired; recovery can reclaim it by fencing token.
                return

    def _start_lease_heartbeat(self, work_item_id: str, owner: str, claim: WorkItemClaim) -> None:
        """Keep long provider calls fenced; a stale worker still cannot commit."""
        assert self.work_items is not None
        stop = threading.Event()

        def renew() -> None:
            while not stop.wait(60):
                try:
                    current = self.work_items.renew_lease(
                        work_item_id,
                        owner,
                        claim.fencing_token,
                        lease_seconds=300,
                    )
                except (psycopg.Error, RuntimeError) as error:
                    logger.warning("Evaluation lease renewal failed (%s)", type(error).__name__)
                    return
                if current is None:
                    return

        threading.Thread(
            target=renew,
            name=f"lisa-evaluation-lease-{work_item_id}",
            daemon=True,
        ).start()

    def _session_detail(self, session_id: str) -> dict:
        return self.sessions.detail(session_id)

    def _trajectory(self, session_detail: dict) -> dict:
        """Build the shared compact session trajectory."""
        return self.sessions.trajectory(session_detail)

    def get_session_trajectory(self, session_id: str) -> dict:
        """Fetch the compact trajectory the selector and judge use."""
        return self.sessions.get_trajectory(session_id)

    def notify_evaluation_failure(
        self, target_agent_name: str, name: str, session_id: str, failure_key: str = ""
    ) -> dict:
        """Notify Slack only after Lisa reviews a stored policy failure."""
        # Stored history, not today's policy row, identifies pending failures.
        # This also permits finishing old follow-ups after a rule is edited/paused.
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
        session_detail = self._session_detail(session_id)
        if target_agent_name not in session_detail.get("agent_names", []):
            return {"status": "skipped_agent_mismatch", "session_id": session_id}
        trace_id = str(policy.get("trace_id") or "")
        if not any(
            str(span.get("trace_id") or "") == trace_id
            for span in session_detail.get("spans", [])
        ):
            return {"status": "trace_not_found", "session_id": session_id}
        score = next(
            (
                score
                for score in session_detail.get("scores", [])
                if score.get("string_value") == "fail"
                and matches_policy_score(score, policy, trace_id)
            ),
            None,
        )
        if score is None:
            return {"status": "not_a_stored_failure", "session_id": session_id}
        return self.notifier.notify(
            policy,
            {"status": "fail", "rationale": score.get("comment", "Policy failed.")},
            session_id,
            trace_id,
        )
