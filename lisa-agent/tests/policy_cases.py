"""Shared policy/evidence fixtures for the approval and execution contracts."""

from thelake_lisa.evaluation.contracts import EvaluationRequest


def save_rule(
    services, approved="yes", text="Check eligibility before refunding.", scope="workspace"
):
    store = services.policies
    agent = "agent" if scope == "agent" else ""
    current = store.read_policy(scope, agent)
    store.update_policy(f"## Rule\napproved: {approved}\n\n{text}", current, scope, agent)


def evidence():
    return {
        "session_id": "session",
        "span_count": 1,
        "agent_names": ["agent"],
        "scores": [],
        "spans": [
            {
                "span_id": "span",
                "trace_id": "trace",
                "start_time": "2026-10-09T12:00:00Z",
                "input": "Refund my ticket",
                "output": "Refund issued",
            }
        ],
    }


def evaluation_request(**evidence_fields):
    return EvaluationRequest.model_validate(
        {
            "run_id": "run",
            "policy": {
                "name": "Rule",
                "scope": "workspace",
                "revision": "hash",
                "approved": True,
                "criteria": "Check eligibility before refunding.",
            },
            "evidence": {
                "trace_id": "trace",
                "events": [
                    {
                        "sequence": 0,
                        "span_id": "span",
                        "timestamp": "2026-10-09T12:00:00Z",
                        "kind": "user_message",
                        "content": "Refund",
                    }
                ],
                "complete": True,
                "tool_events_captured": False,
                **evidence_fields,
            },
        }
    )
