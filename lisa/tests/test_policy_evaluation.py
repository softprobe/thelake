from dataclasses import replace
from types import SimpleNamespace

import pytest
from policy_cases import evaluation_request, save_rule, session_detail

from lisa.bootstrap import build_services
from lisa.evaluation import evaluator


@pytest.fixture
def services(lisa_test_context):
    return build_services(lisa_test_context(confirmed_policy_title="Rule"))


@pytest.mark.parametrize("approval", ["no", "missing"])
def test_only_explicitly_approved_rules_can_run(services, monkeypatch, approval):
    if approval == "missing":
        services.policy_service.update_policy("## Rule\n\nCheck eligibility.", "")
    else:
        save_rule(services, approval)
    monkeypatch.setattr(services.client, "post", lambda *a, **k: pytest.fail("No API call allowed"))
    assert services.policy_service.approved_rules("agent") == []
    with pytest.raises(ValueError, match="approved policy"):
        services.evaluations._evaluate_session("agent", "Rule", "session")


def test_agent_override_is_applied_before_approval_filter(services):
    save_rule(services)
    save_rule(services, "no", scope="agent")
    assert services.policy_service.approved_rules("agent") == []
    assert services.policy_service.approved_rules("other-agent")[0].name == "Rule"


@pytest.mark.parametrize("header", ["approved: maybe", "approved: yes\napproved: no"])
def test_malformed_or_duplicate_approval_is_rejected(services, header):
    with pytest.raises(ValueError, match="approved"):
        services.policy_service.update_policy(f"## Rule\n{header}\n\nCheck eligibility.", "")


def test_approval_in_prose_or_code_is_not_metadata(services):
    services.policy_service.update_policy(
        "## Rule\n\nExample:\n```md\napproved: yes\n## Nested heading\n```", ""
    )
    assert services.policy_service.approved_rules("agent") == []


def test_approval_changes_require_owner_confirmation(services):
    save_rule(services, "no")
    unconfirmed = build_services(replace(services.context, confirmed_policy_title=""))
    with pytest.raises(ValueError, match="confirmation"):
        save_rule(unconfirmed)
    scheduled = build_services(replace(services.context, turn_source="scheduled"))
    with pytest.raises(ValueError, match="owner conversation"):
        save_rule(scheduled)


def test_run_stores_policy_id_without_a_registry(services, monkeypatch):
    save_rule(services)
    calls, judged = [], []
    scores = []

    def get(path):
        calls.append(path)
        return {**session_detail(), "scores": list(scores)}

    def post(path, body, *, include_status=False):
        assert path == "/v1/scores"
        scores.append(body)
        return ({}, 201) if include_status else {}

    def judge(request):
        judged.append(request)
        save_rule(services, text="Wait for customer confirmation before refunding.")
        return {
            "status": "fail",
            "rationale": "No eligibility check",
            "references": [],
            "limitations": [],
        }

    monkeypatch.setattr(services.client, "get", get)
    monkeypatch.setattr(services.client, "post", post)
    monkeypatch.setattr(services.evaluations, "judge", judge)
    result = services.evaluations._evaluate_session("agent", "Rule", "session")
    assert result["status"] == "fail"
    assert judged[0]["policy"]["criteria"] == "Check eligibility before refunding."
    assert "threshold" not in judged[0]["policy"]
    assert "evaluator" not in judged[0]
    assert scores[0]["config_id"] is None
    assert scores[0]["metadata"]["policy_id"]
    assert "Check eligibility before refunding." in scores[0]["metadata"]["criteria"]
    assert calls == ["/v1/sessions/session?limit=500"]
    assert (
        services.failures.repository.list_all()[0]["policy_hash"]
        == scores[0]["metadata"]["policy_hash"]
    )


def test_same_policy_and_session_reuses_score(services, monkeypatch):
    save_rule(services)
    scores, runs = [], []

    def get(path):
        return {**session_detail(), "scores": list(scores)}

    def post(path, body, *, include_status=False):
        assert path == "/v1/scores"
        scores.append(body)
        return ({}, 201) if include_status else {}

    monkeypatch.setattr(services.client, "get", get)
    monkeypatch.setattr(services.client, "post", post)
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda r: (
            runs.append(r)
            or {
                "status": "pass",
                "rationale": "Policy satisfied",
                "limitations": [],
            }
        ),
    )
    services.evaluations._evaluate_session("agent", "Rule", "session")
    assert (
        services.evaluations._evaluate_session("agent", "Rule", "session")["status"]
        == "already_evaluated"
    )
    assert len(runs) == 1
    assert len(scores) == 1


@pytest.mark.parametrize("verdict", ["pass", "fail", "uncertain"])
def test_judge_returns_categorical_verdict_without_threshold(monkeypatch, verdict):
    captured = {}

    class Models:
        def generate_content(self, **kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                parsed={
                    "status": verdict,
                    "rationale": "Evidence explanation",
                    "span_ids": ["e1"],
                }
            )

    class Judge:
        models = Models()

        def close(self):
            pass

    monkeypatch.setattr(evaluator, "create_judge_client", lambda: Judge())
    request = evaluation_request()
    response = evaluator.evaluate(request)
    assert response.status == verdict
    assert "threshold" not in response.model_dump()
    assert "score" not in response.model_dump()
    assert response.references[0].span_id == "span"
    assert request.policy.criteria in captured["contents"]


def test_tied_messages_from_different_spans_are_inconclusive(services, monkeypatch):
    save_rule(services)
    session = session_detail()
    session["spans"].append({**session["spans"][0], "span_id": "another-span", "input": "Confirm"})
    monkeypatch.setattr(
        services.client,
        "get",
        lambda p: session,
    )
    monkeypatch.setattr(
        services.client,
        "post",
        lambda p, b, *, include_status=False: ({}, 201) if include_status else {},
    )
    result = services.evaluations._evaluate_session("agent", "Rule", "session")
    assert result["status"] == "insufficient_evidence"
