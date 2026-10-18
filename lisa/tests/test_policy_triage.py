from __future__ import annotations

from types import SimpleNamespace

import pytest

from lisa.context import WorkspaceContext
from lisa.evaluation.dataset import load_dataset
from lisa.evaluation.policy_triage import OpenAIPolicyTriager
from lisa.models.policies import PolicyRule
from lisa.repositories.evaluation_artifacts import EvaluationArtifactRepository
from lisa.services.evaluations import EvaluationService
from lisa.services.sessions import SessionService


class FakeDecisionsClient:
    def __init__(self, choices, confidences=None):
        self.choices = choices
        self.confidences = confidences
        self.request = None

    def post(self, _url, *, json):
        self.request = json
        answers = []
        for index, question in enumerate(json["questions"]):
            answer = {
                "type": "choice",
                "name": question["name"],
                "choice": self.choices[index],
            }
            if self.confidences is not None:
                answer["confidence"] = self.confidences[index]
            answers.append(answer)
        return FakeResponse({"answers": answers})


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


class FakeTriager:
    def __init__(self, selected):
        self.selected = selected
        self.calls = []
        self.model = "test-decisions-model"

    def triage(self, trajectory, rules):
        self.calls.append((trajectory, rules))
        return {"selected_policies": self.selected}


def make_evaluation_service(**kwargs):
    client = kwargs.pop("client")
    build_events = kwargs.pop("build_events", None)
    triager = kwargs.pop("policy_triage", FakeTriager([]))
    sessions = SessionService(client, build_events) if build_events else SessionService(client)
    return EvaluationService(
        client=client,
        sessions=sessions,
        policy_triage=triager,
        **kwargs,
    )


def test_decisions_routes_each_catalog_choice_independently():
    data = load_dataset()
    batch = data.batches(source="synthetic")[0]
    rules = [
        PolicyRule(id=ref, **data.policies[ref].policy.model_dump()) for ref in batch.policy_ids
    ]
    client = FakeDecisionsClient(["suspected_violation", *(["no_violation_found"] * 5)])
    result = OpenAIPolicyTriager(client=client).triage(
        data.trajectories[batch.session_id].trajectory.model_dump(), rules
    )
    assert [p["id"] for p in result["selected_policies"]] == [rules[0].id]
    assert len(client.request["questions"]) == len(rules)
    assert len({q["name"] for q in client.request["questions"]}) == len(rules)
    assert client.request["model"] == "gpt-6-luna"


def test_triager_prompt_preserves_scope_and_uncertainty():
    client = FakeDecisionsClient(["no_violation_found"])
    OpenAIPolicyTriager(client=client).triage(
        {"events": []},
        [
            PolicyRule(
                name="Economy to First upgrade",
                criteria="Only this direction.",
                approved=True,
                scope="workspace",
                id="p1",
            )
        ],
    )
    question = client.request["questions"][0]
    assert "Match every named type, source, destination, and trigger" in question["instructions"]
    assert "suspected violation" in question["instructions"]
    assert "insufficient_evidence" in {c["value"] for c in question["choices"]}


def test_every_suspected_violation_is_sent_to_the_judge_regardless_of_confidence():
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
    triager = OpenAIPolicyTriager(
        client=FakeDecisionsClient(
            ["suspected_violation", "suspected_violation"],
            confidences=[0.69, 0.7],
        ),
    )

    selection = triager.triage({"events": []}, rules)

    assert selection["selected_policies"] == [
        {
            "id": "P1.1",
            "name": "Low confidence cancellation policy",
            "scope": "workspace",
        },
        {
            "id": "P1.2",
            "name": "Confident refund policy",
            "scope": "workspace",
        },
    ]
    assert set(selection) == {"selected_policies", "policy_decisions"}


def test_suspected_violation_selection_does_not_require_confidence():
    triager = OpenAIPolicyTriager(client=FakeDecisionsClient(["suspected_violation"]))
    rule = SimpleNamespace(
        id="P1.1",
        name="Same-day connected itinerary disclosure",
        criteria="Disclose the stranding risk before cancellation.",
        scope="workspace",
    )

    selection = triager.triage({"events": []}, [rule])

    assert selection["selected_policies"][0]["id"] == "P1.1"


def test_triager_uses_triage_choices_and_incomplete_answers_fail_closed():
    rule = SimpleNamespace(
        id="P1.2",
        name="Cancellation policy",
        criteria="Cancellation rules",
        scope="workspace",
    )
    triager = OpenAIPolicyTriager(
        client=FakeDecisionsClient(["suspected_violation"]),
    )
    selection = triager.triage({"events": []}, [rule])
    assert selection["selected_policies"][0]["id"] == "P1.2"
    assert (
        "A policy lookup alone is not a violation"
        in (triager.client.request["questions"][0]["instructions"])
    )
    assert [choice["value"] for choice in triager.client.request["questions"][0]["choices"]] == [
        "suspected_violation",
        "no_violation_found",
        "insufficient_evidence",
    ]

    class IncompleteClient(FakeDecisionsClient):
        def post(self, _url, *, json):
            self.request = json
            return FakeResponse({"answers": []})

    with pytest.raises(RuntimeError, match="incomplete policy triage"):
        OpenAIPolicyTriager(client=IncompleteClient([])).triage({"events": []}, [rule])


def test_missing_decision_confidence_does_not_change_triage():
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
                            "choice": "suspected_violation",
                        }
                    ]
                }
            )

    selection = OpenAIPolicyTriager(client=MissingConfidenceClient(["suspected_violation"])).triage(
        {"events": []}, [rule]
    )
    assert selection["selected_policies"] == [
        {"id": "P1.2", "name": rule.name, "scope": rule.scope}
    ]


@pytest.mark.parametrize(
    "confidence", [True, "not-a-number", float("nan"), float("inf"), -0.1, 1.01]
)
def test_decision_confidence_is_ignored(confidence):
    rule = SimpleNamespace(
        id="P1.2",
        name="Cancellation policy",
        criteria="Cancellation rules",
        scope="workspace",
    )

    selection = OpenAIPolicyTriager(
        client=FakeDecisionsClient(["suspected_violation"], [confidence])
    ).triage({"events": []}, [rule])
    assert selection["selected_policies"] == [
        {"id": "P1.2", "name": rule.name, "scope": rule.scope}
    ]


def test_policy_triage_result_has_no_confidence_cutoff_metadata():
    result = OpenAIPolicyTriager(client=FakeDecisionsClient(["no_violation_found"])).triage(
        {"events": []},
        [SimpleNamespace(name="Cancellation", criteria="Cancel", scope="workspace")],
    )

    assert result["selected_policies"] == []
    assert result["policy_decisions"][0]["decision"] == "no_violation_found"


def test_session_selection_checks_agent_and_returns_only_selected_rules():
    rule = SimpleNamespace(name="Cancellation policy", scope="workspace")
    triager = FakeTriager([{"name": rule.name, "scope": rule.scope}])
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
        policy_triage=triager,
    )

    result, trajectory = service._triage_policies_for_session("travel-agent", "session-1")

    assert "status" not in result
    assert result["selected_policies"] == triager.selected
    assert result["candidate_count"] == 1
    assert trajectory["events"] == [{"kind": "user_message", "content": "cancel this flight"}]
    assert triager.calls[0][0]["events"] == trajectory["events"]


def test_session_policy_triage_skips_a_different_agent():
    triager = FakeTriager([])
    service = make_evaluation_service(
        context=None,
        client=FakeSessionClient({"agent_names": ["other-agent"], "spans": []}),
        policies=FakePolicyStore([SimpleNamespace(name="Rule", scope="workspace")]),
        failures=None,
        notifier=None,
        policy_triage=triager,
    )

    result, trajectory = service._triage_policies_for_session("travel-agent", "session-1")

    assert result["status"] == "skipped_agent_mismatch"
    assert trajectory is None
    assert triager.calls == []


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
        policy_triage=OpenAIPolicyTriager(
            client=FakeDecisionsClient(["no_violation_found"]),
        ),
    )

    result, trajectory = service._triage_policies_for_session("travel-agent", "session-1")

    assert result["selected_policies"] == []
    assert "status" not in result
    assert trajectory is not None
    assert set(result) >= {
        "session_id",
        "target_agent_name",
        "candidate_count",
        "selected_policies",
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
    triager = FakeTriager(
        [
            {
                "id": "missing-policy",
                "name": rule.name,
                "scope": rule.scope,
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
        policy_triage=triager,
    )

    with pytest.raises(RuntimeError, match="not in the approved candidate set"):
        service.evaluate_session("refund-agent", "session-1")


def test_evaluation_selection_passes_triage_to_judge():
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
    }
    triager = FakeTriager([decision])
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
        policy_triage=triager,
    )
    evaluation_calls = []
    service._evaluate_session = lambda *args, **kwargs: (
        evaluation_calls.append(kwargs) or {"status": "pass"}
    )

    result = service.evaluate_session("refund-agent", "session-1")

    assert result["status"] == "evaluated"
    assert evaluation_calls[0]["_selection"] == {
        "model": "test-decisions-model",
        "candidate_count": 1,
        "selected_policies": [decision],
    }


def test_incomplete_trajectory_does_not_exclude_any_approved_policy():
    rules = [
        SimpleNamespace(name="Cancellation policy", scope="workspace"),
        SimpleNamespace(name="Upgrade policy", scope="workspace"),
    ]
    triager = FakeTriager([])
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
        policy_triage=triager,
    )

    result, trajectory = service._triage_policies_for_session("travel-agent", "session-1")

    assert result["status"] == "insufficient_evidence"
    assert result["unresolved_policies"] == [rule.name for rule in rules]
    assert result["limitations"] == ["The session detail was bounded or truncated."]
    assert trajectory is not None
    assert triager.calls == []


def test_evaluate_session_judges_every_selected_policy_with_the_same_evidence(tmp_path):
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
    second_rule = PolicyRule(
        name="Keep the customer informed",
        criteria="Explain the applicable fee before asking for confirmation.",
        approved=True,
        scope="workspace",
        id="P1.2",
    )
    client = FakeSessionClient(session_detail)
    triager = OpenAIPolicyTriager(
        client=FakeDecisionsClient(["suspected_violation", "insufficient_evidence"])
    )
    judge_requests = []
    context = WorkspaceContext(workspace="11111111-1111-4111-8111-111111111111", data_dir=tmp_path)
    service = make_evaluation_service(
        context=context,
        client=client,
        policies=FakePolicyStore([rule, second_rule]),
        failures=None,
        notifier=None,
        judge=lambda request: (
            judge_requests.append(request)
            or {
                "status": "pass",
                "rationale": f"{request['policy']['name']} was checked.",
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
        policy_triage=triager,
        artifact_repository=EvaluationArtifactRepository(context),
    )

    result = service.evaluate_session("refund-agent", "session-1")

    assert result["status"] == "evaluated"
    assert result["candidate_count"] == 2
    assert len(result["evaluations"]) == 2
    assert {item["evaluation_name"] for item in result["evaluations"]} == {
        rule.name,
        second_rule.name,
    }
    assert all(item["status"] == "pass" for item in result["evaluations"])
    assert {item["id"] for item in result["selected_policies"]} == {rule.id, second_rule.id}
    assert len(client.get_calls) == 1
    assert len(triager.client.request["questions"]) == 2
    assert {body["metadata"]["policy_id"] for _, body in client.posts} == {
        rule.id,
        second_rule.id,
    }
    assert len(judge_requests) == 2
    assert {request["policy"]["name"] for request in judge_requests} == {
        rule.name,
        second_rule.name,
    }
    assert all(
        request["trajectory"]["events"] == judge_requests[0]["trajectory"]["events"]
        for request in judge_requests
    )


def test_uncertain_decision_is_escalated_alongside_suspected_violations():
    rules = [
        SimpleNamespace(id=str(i), name=str(i), scope="workspace", criteria="Rule")
        for i in range(3)
    ]
    result = OpenAIPolicyTriager(
        client=FakeDecisionsClient(
            ["suspected_violation", "no_violation_found", "insufficient_evidence"]
        )
    ).triage({"events": []}, rules)
    assert [p["id"] for p in result["selected_policies"]] == ["0", "2"]


@pytest.mark.parametrize("choice", ["applicable", "not_applicable", "violation", None])
def test_obsolete_or_invalid_triage_choices_are_rejected(choice):
    with pytest.raises(RuntimeError, match="invalid policy choice"):
        OpenAIPolicyTriager(client=FakeDecisionsClient([choice])).triage(
            {"events": []}, [SimpleNamespace(name="Rule", criteria="Rule", scope="workspace")]
        )
