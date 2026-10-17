from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from lisa.context import WorkspaceContext
from lisa.evaluation.policy_selection import OpenAIPolicySelector
from lisa.models.policies import PolicyRule
from lisa.repositories.evaluation_artifacts import EvaluationArtifactRepository
from lisa.repositories.policies import InMemoryPolicyRepository
from lisa.services.evaluations import EvaluationService
from lisa.services.policies import PolicyService
from lisa.services.sessions import SessionService

ROOT = Path(__file__).parent
POLICY_FILE = ROOT / "fixtures" / "state_bench_policies" / "POLICY.md"
GOLD_FILE = ROOT / "fixtures" / "state_bench_gold.jsonl"


def test_state_bench_policy_file_contains_all_gold_rules():
    policies = PolicyService(context=None, repository=InMemoryPolicyRepository()).parse_rules(
        POLICY_FILE.read_text(encoding="utf-8"),
        "workspace",
    )
    gold = [
        json.loads(line)
        for line in GOLD_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    expected = {row["input"]["policy"]["name"]: row["input"]["policy"]["criteria"] for row in gold}
    assert set(policies) == set(expected)
    assert {name: rule.criteria for name, rule in policies.items()} == expected
    assert all(rule.approved for rule in policies.values())


class FakeDecisionsClient:
    def __init__(self, choices, confidences=None):
        self.choices = choices
        self.confidences = confidences or [0.99] * len(choices)
        self.request = None

    def post(self, _url, *, json):
        self.request = json
        return FakeResponse(
            {
                "answers": [
                    {
                        "type": "choice",
                        "name": question["name"],
                        "choice": self.choices[index],
                        "confidence": self.confidences[index],
                    }
                    for index, question in enumerate(json["questions"])
                ]
            }
        )


class FakeResponse:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class FakePolicyStore:
    def __init__(self, rules):
        self.rules = rules

    def approved_rules(self, target_agent_name):
        return self.rules


class FakeSessionClient:
    def __init__(self, session_detail):
        self.session_detail = session_detail
        self.get_calls = []
        self.posts = []

    def get(self, path):
        self.get_calls.append(path)
        return self.session_detail

    def post(self, path, payload, *, include_status=False):
        self.posts.append((path, payload))
        return ({}, 201) if include_status else {}


class FakeSelector:
    def __init__(self, selected):
        self.selected = selected
        self.calls = []
        self.model = "test-decisions-model"
        self.minimum_confidence = 0.8

    def select(self, trajectory, rules):
        self.calls.append((trajectory, rules))
        return {
            "selected_policies": self.selected,
            "minimum_confidence": self.minimum_confidence,
        }


def make_evaluation_service(**kwargs):
    client = kwargs.pop("client")
    build_events = kwargs.pop("build_events", None)
    selector = kwargs.pop("policy_selector", FakeSelector([]))
    sessions = SessionService(client, build_events) if build_events else SessionService(client)
    return EvaluationService(
        client=client,
        sessions=sessions,
        policy_selector=selector,
        **kwargs,
    )


def test_decisions_selects_the_matching_policy_from_one_shared_file():
    policies = PolicyService(context=None, repository=InMemoryPolicyRepository()).parse_rules(
        POLICY_FILE.read_text(encoding="utf-8"), "workspace"
    )
    gold = [
        json.loads(line)
        for line in GOLD_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    for row in gold:
        expected_name = row["expected"]["selected_policy"]
        choices = [
            "applicable" if rule.name == expected_name else "not_applicable"
            for rule in policies.values()
        ]
        client = FakeDecisionsClient(choices)
        selector = OpenAIPolicySelector(client=client)

        selection = selector.select(row["input"]["trajectory"], list(policies.values()))

        assert [item["name"] for item in selection["selected_policies"]] == [expected_name]
        assert len(client.request["questions"]) == 5
        assert len({q["name"] for q in client.request["questions"]}) == 5
        assert client.request["model"] == "gpt-6-luna"


def test_only_confident_applicable_policy_is_sent_to_the_judge():
    rules = [
        SimpleNamespace(
            id="P1.1",
            name="Low confidence cancellation policy",
            criteria="Cancellation rules",
            scope="workspace",
        ),
        SimpleNamespace(
            id="P1.2",
            name="Confident refund policy",
            criteria="Refund rules",
            scope="workspace",
        ),
    ]
    selector = OpenAIPolicySelector(
        client=FakeDecisionsClient(
            ["applicable", "applicable"],
            confidences=[0.79, 0.8],
        ),
    )

    selection = selector.select({"events": []}, rules)

    assert selection["selected_policies"] == [
        {
            "id": "P1.2",
            "name": "Confident refund policy",
            "scope": "workspace",
            "confidence": 0.8,
        }
    ]
    assert set(selection) == {"selected_policies", "minimum_confidence"}


def test_selector_uses_binary_choices_and_incomplete_answers_fail_closed():
    rule = SimpleNamespace(
        id="P1.2",
        name="Cancellation policy",
        criteria="Cancellation rules",
        scope="workspace",
    )
    selector = OpenAIPolicySelector(
        client=FakeDecisionsClient(["applicable"]),
    )
    selection = selector.select({"events": []}, [rule])
    assert selection["selected_policies"][0]["id"] == "P1.2"
    assert [choice["value"] for choice in selector.client.request["questions"][0]["choices"]] == [
        "applicable",
        "not_applicable",
    ]

    class IncompleteClient(FakeDecisionsClient):
        def post(self, _url, *, json):
            self.request = json
            return FakeResponse({"answers": []})

    with pytest.raises(RuntimeError, match="incomplete policy selection"):
        OpenAIPolicySelector(client=IncompleteClient([])).select({"events": []}, [rule])


def test_missing_decision_confidence_fails_closed():
    rule = SimpleNamespace(
        id="P1.2",
        name="Cancellation policy",
        criteria="Cancellation rules",
        scope="workspace",
    )

    class MissingConfidenceClient(FakeDecisionsClient):
        def post(self, _url, *, json):
            self.request = json
            return FakeResponse(
                {
                    "answers": [
                        {
                            "type": "choice",
                            "name": "policy_0",
                            "choice": "applicable",
                        }
                    ]
                }
            )

    with pytest.raises(RuntimeError, match="invalid confidence"):
        OpenAIPolicySelector(client=MissingConfidenceClient(["applicable"])).select(
            {"events": []}, [rule]
        )


@pytest.mark.parametrize("confidence", [True, "0.9", float("nan"), float("inf"), -0.1, 1.01])
def test_invalid_decision_confidence_fails_closed(confidence):
    rule = SimpleNamespace(
        id="P1.2",
        name="Cancellation policy",
        criteria="Cancellation rules",
        scope="workspace",
    )

    with pytest.raises(RuntimeError, match="invalid confidence"):
        OpenAIPolicySelector(client=FakeDecisionsClient(["applicable"], [confidence])).select(
            {"events": []}, [rule]
        )


def test_policy_selection_confidence_cutoff_can_be_configured(monkeypatch):
    monkeypatch.setenv("LISA_POLICY_SELECTION_MIN_CONFIDENCE", "0.9")
    selector = OpenAIPolicySelector(client=FakeDecisionsClient(["applicable"], [0.85]))
    rule = SimpleNamespace(
        id="P1.2",
        name="Cancellation policy",
        criteria="Cancellation rules",
        scope="workspace",
    )

    result = selector.select({"events": []}, [rule])

    assert selector.minimum_confidence == 0.9
    assert result["minimum_confidence"] == 0.9
    assert result["selected_policies"] == []


def test_session_selection_checks_agent_and_returns_only_selected_rules():
    rule = SimpleNamespace(name="Cancellation policy", scope="workspace")
    selector = FakeSelector([{"name": rule.name, "scope": rule.scope}])
    detail = {"agent_names": ["travel-agent"], "spans": [{"trace_id": "trace-1"}]}
    service = make_evaluation_service(
        context=None,
        client=FakeSessionClient(detail),
        policies=FakePolicyStore([rule]),
        failures=None,
        notifier=None,
        build_events=lambda _spans: SimpleNamespace(
            events=[{"kind": "user_message", "content": "cancel this flight"}],
            limitations=[],
            tool_events_captured=True,
            tool_order_certain=True,
            message_order_certain=True,
        ),
        policy_selector=selector,
    )

    result, trajectory = service._select_policies_for_session("travel-agent", "session-1")

    assert "status" not in result
    assert result["selected_policies"] == selector.selected
    assert result["candidate_count"] == 1
    assert trajectory["events"] == [
        {"kind": "user_message", "content": "cancel this flight"}
    ]
    assert selector.calls[0][0]["events"] == trajectory["events"]


def test_session_policy_selection_skips_a_different_agent():
    selector = FakeSelector([])
    service = make_evaluation_service(
        context=None,
        client=FakeSessionClient({"agent_names": ["other-agent"], "spans": []}),
        policies=FakePolicyStore([SimpleNamespace(name="Rule", scope="workspace")]),
        failures=None,
        notifier=None,
        policy_selector=selector,
    )

    result, trajectory = service._select_policies_for_session("travel-agent", "session-1")

    assert result["status"] == "skipped_agent_mismatch"
    assert trajectory is None
    assert selector.calls == []


def test_session_with_no_selected_policies_returns_empty_selection():
    rule = SimpleNamespace(
        id="P1.1",
        name="Cancellation policy",
        criteria="Cancellation rules",
        scope="workspace",
    )
    detail = {"agent_names": ["travel-agent"], "spans": [{"trace_id": "trace-1"}]}
    service = make_evaluation_service(
        context=None,
        client=FakeSessionClient(detail),
        policies=FakePolicyStore([rule]),
        failures=None,
        notifier=None,
        build_events=lambda _spans: SimpleNamespace(
            events=[{"kind": "user_message", "content": "cancel this flight"}],
            limitations=[],
            tool_events_captured=True,
            tool_order_certain=True,
            message_order_certain=True,
        ),
        policy_selector=OpenAIPolicySelector(
            client=FakeDecisionsClient(["applicable"], [0.5]),
        ),
    )

    result, trajectory = service._select_policies_for_session("travel-agent", "session-1")

    assert result["selected_policies"] == []
    assert "status" not in result
    assert trajectory is not None
    assert set(result) >= {
        "session_id",
        "target_agent_name",
        "candidate_count",
        "selected_policies",
        "minimum_confidence",
    }
    assert "not_selected_policies" not in result


def test_selected_policy_missing_from_candidates_fails_closed():
    rule = PolicyRule(
        name="Check refund eligibility",
        criteria="Check eligibility before refunding.",
        approved=True,
        scope="workspace",
        id="P1.1",
    )
    selector = FakeSelector(
        [
            {
                "id": "missing-policy",
                "name": rule.name,
                "scope": rule.scope,
                "confidence": 0.99,
            }
        ]
    )
    service = make_evaluation_service(
        context=None,
        client=FakeSessionClient(
            {
                "session_id": "session-1",
                "agent_names": ["refund-agent"],
                "spans": [{"trace_id": "trace-1"}],
                "scores": [],
            }
        ),
        policies=FakePolicyStore([rule]),
        failures=None,
        notifier=None,
        build_events=lambda _spans: SimpleNamespace(
            events=[{"kind": "tool_call", "name": "check_eligibility"}],
            limitations=[],
            tool_events_captured=True,
            tool_order_certain=True,
            message_order_certain=True,
        ),
        policy_selector=selector,
    )

    with pytest.raises(RuntimeError, match="not in the approved candidate set"):
        service.evaluate_session("refund-agent", "session-1")


def test_evaluation_selection_keeps_the_confidence_cutoff():
    rule = PolicyRule(
        name="Check refund eligibility",
        criteria="Check eligibility before refunding.",
        approved=True,
        scope="workspace",
        id="P1.1",
    )
    decision = {
        "id": rule.id,
        "name": rule.name,
        "scope": rule.scope,
        "confidence": 0.94,
    }
    selector = FakeSelector([decision])
    selector.minimum_confidence = 0.9
    service = make_evaluation_service(
        context=None,
        client=FakeSessionClient(
            {
                "session_id": "session-1",
                "agent_names": ["refund-agent"],
                "spans": [{"trace_id": "trace-1"}],
                "scores": [],
            }
        ),
        policies=FakePolicyStore([rule]),
        failures=None,
        notifier=None,
        build_events=lambda _spans: SimpleNamespace(
            events=[{"kind": "tool_call", "name": "check_eligibility"}],
            limitations=[],
            tool_events_captured=True,
            tool_order_certain=True,
            message_order_certain=True,
        ),
        policy_selector=selector,
    )
    evaluation_calls = []
    service._evaluate_session = lambda *args, **kwargs: (
        evaluation_calls.append(kwargs) or {"status": "pass"}
    )

    result = service.evaluate_session("refund-agent", "session-1")

    assert result["status"] == "evaluated"
    assert result["minimum_confidence"] == 0.9
    assert evaluation_calls[0]["_selection"] == {
        "model": "test-decisions-model",
        "minimum_confidence": 0.9,
        "candidate_count": 1,
        "selected_policies": [decision],
    }


def test_incomplete_trajectory_does_not_exclude_any_approved_policy():
    rules = [
        SimpleNamespace(name="Cancellation policy", scope="workspace"),
        SimpleNamespace(name="Upgrade policy", scope="workspace"),
    ]
    selector = FakeSelector([])
    detail = {"agent_names": ["travel-agent"], "spans": [{"trace_id": "trace-1"}]}
    service = make_evaluation_service(
        context=None,
        client=FakeSessionClient(detail),
        policies=FakePolicyStore(rules),
        failures=None,
        notifier=None,
        build_events=lambda _spans: SimpleNamespace(
            events=[{"kind": "user_message", "content": "cancel this flight"}],
            limitations=["The session detail was bounded or truncated."],
            tool_events_captured=True,
            tool_order_certain=False,
            message_order_certain=True,
        ),
        policy_selector=selector,
    )

    result, trajectory = service._select_policies_for_session("travel-agent", "session-1")

    assert result["status"] == "insufficient_evidence"
    assert result["unresolved_policies"] == [rule.name for rule in rules]
    assert result["limitations"] == ["The session detail was bounded or truncated."]
    assert trajectory is not None
    assert selector.calls == []


def test_evaluate_session_is_one_call_and_reuses_the_selected_evidence(tmp_path):
    rule = PolicyRule(
        name="Check refund eligibility",
        criteria="Check eligibility before refunding.",
        approved=True,
        scope="workspace",
        id="P1.1",
    )
    session_detail = {
        "session_id": "session-1",
        "agent_names": ["refund-agent"],
        "spans": [{"trace_id": "trace-1", "start_time": "2026-10-12T00:00:00Z"}],
        "scores": [],
    }
    unrelated_rule = PolicyRule(
        name="Do not cancel delivered orders",
        criteria="Do not cancel an order after delivery.",
        approved=True,
        scope="workspace",
        id="P1.2",
    )
    client = FakeSessionClient(session_detail)
    selector = FakeSelector(
        [
            {
                "id": rule.id,
                "name": rule.name,
                "scope": rule.scope,
                "confidence": 0.99,
            }
        ],
    )
    judge_requests = []
    context = WorkspaceContext(workspace="11111111-1111-4111-8111-111111111111", data_dir=tmp_path)
    service = make_evaluation_service(
        context=context,
        client=client,
        policies=FakePolicyStore([rule, unrelated_rule]),
        failures=None,
        notifier=None,
        judge=lambda request: (
            judge_requests.append(request)
            or {
                "status": "pass",
                "rationale": "Eligibility was checked.",
                "references": [],
                "limitations": [],
            }
        ),
        build_events=lambda _spans: SimpleNamespace(
            events=[{"kind": "tool_call", "name": "check_eligibility"}],
            limitations=[],
            tool_events_captured=True,
            tool_order_certain=True,
            message_order_certain=True,
        ),
        policy_selector=selector,
        artifact_repository=EvaluationArtifactRepository(context),
    )

    result = service.evaluate_session("refund-agent", "session-1")

    assert result["status"] == "evaluated"
    assert result["candidate_count"] == 2
    assert len(result["evaluations"]) == 1
    assert result["evaluations"][0]["status"] == "pass"
    assert result["selected_policies"][0]["id"] == rule.id
    assert unrelated_rule.id not in {item["id"] for item in result["selected_policies"]}
    assert len(client.get_calls) == 1
    assert len(selector.calls) == 1
    assert client.posts[0][1]["metadata"]["policy_id"] == rule.id
    assert judge_requests[0]["trajectory"]["events"] == selector.calls[0][0]["events"]
    assert len(judge_requests) == 1
