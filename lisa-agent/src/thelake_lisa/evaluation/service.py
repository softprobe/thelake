"""Apply approved POLICY.md rules directly to captured sessions.

There is no evaluator registry, draft, activation, or numeric threshold. Owners
edit named Markdown rules with `approved: yes/no`; Lisa runs only approved rules.
A content hash and the exact section are saved with each result so editing a rule
never changes the meaning of an old finding. These snapshots are evidence, not
another configuration store.
"""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from urllib.parse import quote

from ..context import WorkspaceContext
from ..notifications import SlackNotifier
from ..policies import PolicyStore
from ..presentation import hide_evaluation_ids
from ..security import redact_evidence, redact_text
from ..storage import file_lock
from ..thelake_client import TheLakeClient
from .evidence import SESSION_SPAN_LIMIT, normalize_events, session_timestamp
from .failures import FailureStore
from .judge import run_evaluation
from .scores import matches_policy_score, next_evaluation_score_id, successful_policy_score


class EvaluationService:
    def __init__(
        self,
        context: WorkspaceContext,
        client: TheLakeClient,
        policies: PolicyStore,
        failures: FailureStore,
        notifier: SlackNotifier,
        judge=run_evaluation,
        normalizer=normalize_events,
    ):
        self.context = context
        self.client = client
        self.policies = policies
        self.failures = failures
        self.notifier = notifier
        self.judge = judge
        self.normalizer = normalizer

    def list_approved_policies(self, target_agent_name: str) -> list[dict] | str:
        """List approved workspace and agent policies applicable to an exact agent."""
        return [
            {"name": rule.name, "scope": rule.scope, "criteria": rule.criteria}
            for rule in self.policies.approved_rules(target_agent_name)
        ] or "No approved policies apply to this agent."

    def _policy_snapshot(self, target_agent_name: str, name: str) -> dict:
        rule = next(
            (rule for rule in self.policies.approved_rules(target_agent_name) if rule.name == name),
            None,
        )
        if rule is None:
            raise ValueError("No approved policy with that exact name applies to this agent")
        # Capture once: file edits during the run cannot change its judge input,
        # lock, score identity, or recoverable follow-up. Scope and agent distinguish
        # identically named rules in different files and different agent sessions.
        identity = json.dumps(
            [self.context.workspace_id(), target_agent_name, rule.scope, rule.name, rule.revision],
            separators=(",", ":"),
        )
        return {
            "name": rule.name,
            "target_agent_name": target_agent_name,
            "scope": rule.scope,
            "revision": rule.revision,
            "criteria": rule.criteria,
            "policy_snapshot": rule.snapshot,
            "policy_hash": sha256(identity.encode()).hexdigest(),
            # Delivery context is separate from policy text and its content hash.
            "slack_channel_id": self.context.slack_channel or None,
            "slack_thread_ts": self.context.slack_thread or None,
        }

    def _evaluation_lock_path(self, policy: dict, session_id: str) -> Path:
        home = self.context.home()
        directory = home / "evaluation-locks"
        if directory.is_symlink() or not directory.resolve().is_relative_to(home.resolve()):
            raise ValueError("Evaluation lock path escapes Lisa's workspace home or uses a symlink")
        digest = sha256(json.dumps([policy["policy_hash"], session_id]).encode()).hexdigest()
        return directory / f"{digest}.lock"

    def _evaluation_score_id(self, policy: dict, trace_id: str) -> str:
        return sha256(f"{policy['policy_hash']}:{trace_id}".encode()).hexdigest()

    def evaluate_session(self, target_agent_name: str, name: str, session_id: str) -> dict:
        """Evaluate one approved named policy against a session and store its verdict."""
        policy = self._policy_snapshot(target_agent_name, name)
        with file_lock(self._evaluation_lock_path(policy, session_id)):
            return self._evaluate_session_locked(policy, session_id)

    def _evaluate_session_locked(self, policy: dict, session_id: str) -> dict:
        # TheLake returns session detail with a bounded span list. Redact secrets before
        # normalization and before anything can be forwarded to the provider.
        evidence = self._session_evidence(session_id)
        if policy["target_agent_name"] not in evidence.get("agent_names", []):
            return {
                "status": "skipped_agent_mismatch",
                "session_id": session_id,
                "expected_agent": policy["target_agent_name"],
                "observed_agents": evidence.get("agent_names", []),
                "stored": False,
            }
        spans = evidence.get("spans", [])
        trajectory = self._trajectory(evidence)
        # Current sampling evaluates session evidence once and associates its
        # result with the first captured trace; it is not a per-trace coverage loop.
        trace_id = next(iter(trajectory["trace_ids"]), "")
        if not trace_id:
            return {"status": "insufficient_evidence", "session_id": session_id, "stored": False}
        scores = evidence.get("scores", [])
        run_id = self._evaluation_score_id(policy, trace_id)
        # Reuse a stored non-error outcome for the same policy content and trace.
        # Changed Markdown has a new hash and is eligible for a fresh evaluation.
        completed = successful_policy_score(scores, policy, trace_id)
        if completed:
            if completed.get("string_value") == "fail":
                # A later chat must not redirect a historical notification.
                # Restore delivery context from the score that produced this failure.
                metadata = completed.get("metadata") or {}
                historical_policy = {
                    **policy,
                    "slack_channel_id": metadata.get("slack_channel_id") or None,
                    "slack_thread_ts": metadata.get("slack_thread_ts") or None,
                }
                self.failures.record(
                    historical_policy,
                    session_id,
                    trace_id,
                    str(completed.get("comment") or "Policy failed."),
                    completed.get("timestamp"),
                )
            return {
                "status": "already_evaluated",
                "verdict": completed.get("string_value"),
                "rationale": completed.get("comment"),
                "timestamp": completed.get("timestamp"),
                "session_id": session_id,
                "stored": True,
            }
        if any(score.get("score_id") == run_id for score in scores):
            run_id = next_evaluation_score_id(run_id, scores)
        if not trajectory["events"]:
            return {"status": "insufficient_evidence", "session_id": session_id, "stored": False}
        truncated = not trajectory["complete"]
        limitations = list(trajectory["limitations"])
        # Judge the policy text directly. Approval metadata is excluded from the
        # rubric, and no generated evaluator definition or threshold is involved.
        result = redact_evidence(
            self.judge(
                {
                    "run_id": run_id,
                    "policy": {
                        "name": policy["name"],
                        "scope": policy["scope"],
                        "revision": policy["revision"],
                        "approved": True,
                        "criteria": policy["criteria"],
                    },
                    "evidence": {
                        "trace_id": trace_id,
                        "events": trajectory["events"],
                        "complete": not truncated,
                        "tool_events_captured": trajectory["tool_events_captured"],
                        "tool_order_certain": trajectory["tool_order_certain"],
                        "message_order_certain": trajectory["message_order_certain"],
                        "limitations": limitations,
                    },
                }
            )
        )
        result = hide_evaluation_ids(result)
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
        payload = {
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
                "policy_scope": policy["scope"],
                "policy_revision": policy["revision"],
                "policy_snapshot": policy["policy_snapshot"],
                "target_agent_name": policy["target_agent_name"],
                "framework": result.get("framework", "gemini"),
                "evidence": json.dumps(result.get("evidence", [])),
                "limitations": json.dumps(limitations),
                "slack_channel_id": policy.get("slack_channel_id") or "",
                "slack_thread_ts": policy.get("slack_thread_ts") or "",
            },
        }
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
            "evaluation_name": policy["name"],
            "session_id": session_id,
            "stored": True,
            "new_score": status_code == 201,
        }

    def _session_evidence(self, session_id: str) -> dict:
        return redact_evidence(
            self.client.get(
                f"/v1/sessions/{quote(session_id, safe='')}?limit={SESSION_SPAN_LIMIT}"
            )
        )

    def _trajectory(self, evidence: dict) -> dict:
        spans = evidence.get("spans", [])
        normalized = self.normalizer(spans)
        limitations = list(normalized.limitations)
        try:
            span_count = int(evidence.get("span_count", len(spans)))
        except (TypeError, ValueError):
            span_count = len(spans)
        if span_count > len(spans):
            limitations.append("The session detail span limit was reached.")
        if evidence.get("evidence_limited", False) or any(
            "[truncated]" in str(event.get("content", ""))
            or "[truncated]" in str(event.get("payload", ""))
            for event in normalized.events
        ):
            limitations.append("The session evidence was bounded or truncated.")
        return {
            "session_id": evidence.get("session_id", ""),
            "agent_names": evidence.get("agent_names", []),
            "trace_ids": list(
                dict.fromkeys(str(span["trace_id"]) for span in spans if span.get("trace_id"))
            ),
            "events": normalized.events,
            "complete": not limitations,
            "tool_events_captured": normalized.tool_events_captured,
            "tool_order_certain": normalized.tool_order_certain,
            "message_order_certain": normalized.message_order_certain,
            "limitations": list(dict.fromkeys(limitations)),
        }

    def get_session_trajectory(self, session_id: str) -> dict:
        """Fetch the same trajectory representation that the evaluator judges."""
        return self._trajectory(self._session_evidence(session_id))

    def notify_evaluation_failure(
        self, target_agent_name: str, name: str, session_id: str, failure_key: str = ""
    ) -> dict:
        """Notify Slack only after Lisa reviews a stored policy failure."""
        # Stored history, not today's policy file, identifies pending failures.
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
        evidence = self._session_evidence(session_id)
        if target_agent_name not in evidence.get("agent_names", []):
            return {"status": "skipped_agent_mismatch", "session_id": session_id}
        trace_id = str(policy.get("trace_id") or "")
        if not any(
            str(span.get("trace_id") or "") == trace_id for span in evidence.get("spans", [])
        ):
            return {"status": "trace_not_found", "session_id": session_id}
        score = next(
            (
                score
                for score in evidence.get("scores", [])
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
