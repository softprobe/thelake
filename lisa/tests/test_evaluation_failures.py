from benchmark.policy_seed import seed_policy_document

from dataclasses import replace

import pytest
from policy_cases import save_rule

from lisa.bootstrap import build_services


@pytest.mark.parametrize("accepted_before_interruption", [False, True])
def test_failure_recovery_survives_interrupted_score_write(
    monkeypatch, accepted_before_interruption, lisa_test_context
):
    context = lisa_test_context(confirmed_policy_id="Rule")
    services = build_services(context)
    save_rule(services)
    scores = []
    writes = []
    interrupted = True

    def get(path):
        assert path == "/v1/sessions/session?limit=500"
        return {
            "agent_names": ["agent"],
            "span_count": 1,
            "scores": scores,
            "spans": [
                {
                    "span_id": "span",
                    "trace_id": "trace",
                    "input": "request",
                    "start_time": "2026-10-09T12:00:00Z",
                }
            ],
        }

    def post(path, body, *, include_status=False):
        assert path == "/v1/scores"
        writes.append(body)
        if interrupted:
            if accepted_before_interruption:
                scores.append(body)
            raise RuntimeError("Score write interrupted")
        scores.append(body)
        return (body, 201) if include_status else body

    monkeypatch.setattr(services.client, "get", get)
    monkeypatch.setattr(services.client, "post", post)
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda request: {
            "status": "fail",
            "rationale": "Rule failed",
            "limitations": [],
        },
    )
    with pytest.raises(RuntimeError, match="interrupted"):
        services.evaluations._evaluate_session("agent", "Rule", "session")

    interrupted = False
    # A fresh composition models a restarted MCP process using the durable workspace home.
    restarted = build_services(context)
    monkeypatch.setattr(restarted.client, "get", get)
    monkeypatch.setattr(restarted.client, "post", post)
    failures = restarted.failures.list_pending_evaluation_failures()
    assert len(failures) == 1
    assert failures[0]["rationale"] == "Rule failed"
    assert len(writes) == (1 if accepted_before_interruption else 2)
    assert len(scores) == 1
    assert restarted.failures.repository.list_all()[0]["state"] == "ready"
    key = failures[0]["failure_key"]
    assert restarted.failures.complete_evaluation_failure_followup(key)["status"] == "completed"
    assert restarted.failures.complete_evaluation_failure_followup(key)["status"] == "not_pending"


def test_scheduled_turn_cannot_change_policy_or_create_checks(lisa_test_context):
    context = lisa_test_context(
        turn_source="scheduled",
        actor_id="owner-1",
        owner_user_ids=frozenset({"owner-1"}),
        confirmed_policy_id="P1.1",
    )
    services = build_services(replace(context, turn_source="scheduled"))
    with pytest.raises(ValueError, match="owner conversation"):
        services.policy_service.write_policy(
            scope="workspace", agent_name="", name="Rule", criteria="Rule content"
        )
    assert not hasattr(services.evaluations, "create_evaluation_draft")
    assert not hasattr(services.evaluations, "activate_evaluation")
