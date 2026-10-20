"""Shared policy/session-detail fixtures for the approval and execution contracts."""

from benchmark.policy_seed import seed_policy_document

from lisa.evaluation.contracts import EvaluationRequest
from lisa.views.policies import render_policy_list, render_policy_records


def read_policy(services, scope="workspace", agent_name=""):
    records = services.policy_service.read_policy_records(scope, agent_name)
    return render_policy_records(records, scope)


def list_policies(services):
    return render_policy_list(services.policy_service.list_policy_records())


def save_rule(
    services, approved="yes", text="Check eligibility before refunding.", scope="workspace"
):
    store = services.policy_service
    agent = "agent" if scope == "agent" else ""
    current = read_policy(services, scope, agent)
    seed_policy_document(store, f"## Rule\napproved: {approved}\n\n{text}", current, scope, agent)


def session_detail():
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


def evaluation_request(**trajectory_fields):
    return EvaluationRequest.model_validate(
        {
            "run_id": "run",
            "policy": {
                "name": "Rule",
                "scope": "workspace",
                "approved": True,
                "criteria": "Check eligibility before refunding.",
            },
            "trajectory": {
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
                **trajectory_fields,
            },
        }
    )
