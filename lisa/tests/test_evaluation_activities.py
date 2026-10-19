from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from policy_cases import session_detail, save_rule
from sql_cases import read_sql_case

from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.repositories.evaluation_artifacts import EvaluationArtifactRepository
from lisa.services.evaluations import EvaluationService
from lisa.models.activities import EvaluationRun, ScoreOutboxItem, ActivityClaim
from lisa.repositories.lisa_database import LisaDatabaseReader
from lisa.repositories.activities import PostgresActivityStore
from lisa.repositories.policies import PostgresPolicyRepository
from lisa.views.evaluations import render_evaluation_context

WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"


def test_pending_score_accepts_stale_payload_policy_id():
    activity_id = "00000000-0000-0000-0000-000000000001"
    pending = (
        EvaluationRun(
            id=activity_id,
            activity_id=activity_id,
            session_id="session",
            created=False,
            policy_id="P1.10",
        ),
        ScoreOutboxItem(
            id="00000000-0000-0000-0000-000000000002",
            evaluation_id=activity_id,
            status="pending",
            score_payload={
                "name": "Rule",
                "metadata": {
                    "policy_id": "pol_00000000-0000-0000-0000-000000000003",
                    "target_agent_name": "agent",
                },
            },
            score_id="score",
        ),
    )

    EvaluationService._validate_pending_identity(pending, "agent", "Rule", "P1.10")

    with pytest.raises(ValueError, match="selected policy ID"):
        EvaluationService._validate_pending_identity(pending, "agent", "Rule", "P1.11")


@pytest.fixture
def evaluation_services(lisa_test_context, monkeypatch: pytest.MonkeyPatch):
    context = lisa_test_context(WORKSPACE_ID, confirmed_policy_title="Rule")
    services = build_services(context)
    save_rule(services)
    monkeypatch.setattr(services.client, "get", lambda _: session_detail())
    return services


def test_evaluate_session_persists_run_context_score_and_ordered_timeline(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.client, "get", lambda _: detail)
    scores = []
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda _: {
            "status": "pass",
            "rationale": "The trace checked eligibility before issuing the refund.",
            "references": [],
            "limitations": [],
        },
    )

    def post(path, payload, *, include_status=False):
        assert path == "/v1/scores"
        scores.append(payload)
        return ({}, 201) if include_status else {}

    monkeypatch.setattr(services.client, "post", post)
    result = services.evaluations._evaluate_session("agent", "Rule", session_id)

    assert result["status"] == "pass"
    assert result["activity_id"]

    store = PostgresActivityStore(services.context.database_url, WORKSPACE_ID)
    run = store.get_evaluation(result["evaluation_id"])
    assert run is not None
    assert run.verdict == "pass"
    assert run.policy_id == services.policy_service.approved_rules("agent")[0].id
    assert scores[0]["metadata"]["policy_id"] == run.policy_id
    outbox = store.get_score_outbox(run.id)
    assert outbox.status == "acknowledged"
    assert run.score_id == outbox.score_id == scores[0]["score_id"]
    events = store.list_events(run.activity_id)
    assert [event.event_type for event in events] == [
        "accepted",
        "claimed",
        "context_saved",
        "judging_started",
        "score_prepared",
        "succeeded",
    ]
    context_item = store.get_context_item_for_activity(run.activity_id, f"evaluation_inputs_{run.policy_id}")
    assert context_item.content["session_id"] == session_id
    assert context_item.content["policy_id"] == run.policy_id
    assert "policy" not in context_item.content
    assert context_item.content["trajectory"]["events"]

    run_dir = services.context.home() / "activities" / run.activity_id
    assert (run_dir / f"inputs-{run.policy_id}.json").exists()
    assert (run_dir / f"context-{run.policy_id}.md").exists()
    assert run.context_path == f"activities/{run.activity_id}"

    reader = LisaDatabaseReader(services.context.database_read_url, WORKSPACE_ID)
    visible = reader.query(read_sql_case("readable_activity").replace("ACTIVITY_ID", run.activity_id))
    assert visible["rows"] == [{"status": "succeeded", "verdict": "pass"}]
    with pytest.raises(ValueError, match="approved Lisa views or tables"):
        reader.query(read_sql_case("reject_base_table"))


def test_completed_negative_triage_is_filtered_on_the_next_session_evaluation(
    evaluation_services,
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    policy = services.policy_service.approved_rules("agent")[0]

    class NegativeTriager:
        model = "test-decisions-model"

        def __init__(self):
            self.calls = 0

        def triage(self, trajectory, rules):
            self.calls += 1
            assert [rule.id for rule in rules] == [policy.id]
            return {
                "selected_policies": [],
                "policy_decisions": [
                    {"id": policy.id, "name": policy.name, "scope": policy.scope,
                     "decision": "no_violation_found"}
                ],
            }

    triager = NegativeTriager()
    services.evaluations.policy_triage = triager

    first = services.evaluations.evaluate_session("agent", session_id)
    second = services.evaluations.evaluate_session("agent", session_id)

    assert first["selected_policies"] == second["selected_policies"] == []
    assert first["activity_id"]
    assert triager.calls == 1
    assert services.evaluations.activities.policy_triage_decisions(session_id, [policy.id]) == {
        policy.id: "no_violation_found"
    }
    assert any(
        event.event_type == "policy_triage_decision"
        and policy.name in event.title
        and event.summary == "no_violation_found"
        for event in services.evaluations.activities.list_events(first["activity_id"])
    )
    reader = LisaDatabaseReader(services.context.database_read_url, WORKSPACE_ID)
    readable = reader.query(
        (Path(__file__).parent / "sql" / "activities" / "readable_evaluation_triage.sql")
        .read_text()
        .replace("SESSION_ID", session_id)
    )
    assert readable["rows"] == [
        {"session_id": session_id, "policy_id": policy.id, "decision": "no_violation_found"}
    ]
    events = services.evaluations.activities.list_events(first["activity_id"])
    assert [event.event_type for event in events] == [
        "accepted", "policy_triage_decision", "policy_review_completed", "succeeded"
    ]


def test_incomplete_triage_response_is_recorded_as_a_failed_activity(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    policy = services.policy_service.approved_rules("agent")[0]

    class IncompleteTriager:
        model = "test-decisions-model"

        def triage(self, trajectory, rules):
            return {"selected_policies": []}

    services.evaluations.policy_triage = IncompleteTriager()
    created = []
    original_create_activity = services.evaluations.activities.create_activity

    def create_activity(**kwargs):
        activity = original_create_activity(**kwargs)
        created.append(activity.id)
        return activity

    monkeypatch.setattr(services.evaluations.activities, "create_activity", create_activity)

    with pytest.raises(RuntimeError, match="one decision per candidate"):
        services.evaluations.evaluate_session("agent", session_id)
    assert len(created) == 1
    events = services.evaluations.activities.list_events(created[0])
    assert events[-1].event_type == "failed"
    assert events[-2].event_type == "policy_triage_failed"
    assert services.evaluations.activities.policy_triage_decisions(
        session_id, [policy.id]
    ) == {}

    assert services.evaluations.activities.policy_triage_decisions(session_id, [policy.id]) == {}


def test_selected_pair_is_recorded_before_judging_and_filtered_afterward(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.client, "get", lambda _: detail)
    policy = services.policy_service.approved_rules("agent")[0]

    class SelectedTriager:
        model = "test-decisions-model"

        def __init__(self):
            self.calls = 0

        def triage(self, trajectory, rules):
            self.calls += 1
            return {
                "selected_policies": [
                    {"id": policy.id, "name": policy.name, "scope": policy.scope}
                ],
                "policy_decisions": [
                    {"id": policy.id, "name": policy.name, "scope": policy.scope,
                     "decision": "suspected_violation"}
                ],
            }

    triager = SelectedTriager()
    services.evaluations.policy_triage = triager
    judged = []

    def judge(_request):
        assert services.evaluations.activities.policy_triage_decisions(
            session_id, [policy.id]
        ) == {policy.id: "suspected_violation"}
        judged.append(True)
        return {
            "status": "pass",
            "rationale": "The required check was completed.",
            "references": [],
            "limitations": [],
        }

    monkeypatch.setattr(services.evaluations, "judge", judge)
    monkeypatch.setattr(
        services.client,
        "post",
        lambda path, payload, *, include_status=False: ({}, 201)
        if include_status
        else {},
    )

    first = services.evaluations.evaluate_session("agent", session_id)
    second = services.evaluations.evaluate_session("agent", session_id)

    assert first["evaluations"][0]["status"] == "pass"
    assert second["evaluations"][0]["status"] == "already_evaluated"
    assert triager.calls == len(judged) == 1


def test_one_inspectable_activity_records_triage_and_links_multiple_evaluations(evaluation_services, monkeypatch):
    services = evaluation_services
    repository = PostgresPolicyRepository(services.context.database_url, WORKSPACE_ID)
    current = repository.list_scope("workspace", "")
    repository.replace_scope(
        "workspace",
        "",
        [{"name": rule.name, "criteria": rule.criteria, "approved": rule.approved}
         for rule in current],
        [{"name": rule.name, "criteria": rule.criteria, "approved": rule.approved}
         for rule in current]
        + [{"name": "Second rule", "criteria": "Check the second rule.", "approved": True}],
    )
    rules = services.policy_service.approved_rules("agent")
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.client, "get", lambda _: detail)
    services.evaluations.policy_triage = SimpleNamespace(
        model="test-decisions",
        triage=lambda trajectory, candidates: {
            "selected_policies": [
                {"id": rule.id, "name": rule.name, "scope": rule.scope}
                for rule in candidates
            ],
            "policy_decisions": [
                {"id": rule.id, "name": rule.name, "scope": rule.scope,
                 "decision": "suspected_violation"}
                for rule in candidates
            ],
        },
    )
    judged = []
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda request: (judged.append(request) or {
            "status": "pass", "rationale": "Reviewed.", "references": [], "limitations": []
        }),
    )
    monkeypatch.setattr(services.client, "post", lambda *args, **kwargs: ({}, 201))

    result = services.evaluations.evaluate_session("agent", session_id)

    assert len(judged) == len(rules) == 2, result
    assert len({item["activity_id"] for item in result["evaluations"]}) == 1
    activity_id = result["activity_id"]
    events = services.evaluations.activities.list_events(activity_id)
    assert {
        event.summary for event in events if event.event_type == "policy_triage_decision"
    } == {"suspected_violation"}
    runs = [
        services.evaluations.activities.get_evaluation(item["evaluation_id"])
        for item in result["evaluations"]
    ]
    assert {run.policy_id for run in runs} == {rule.id for rule in rules}
    assert {run.activity_id for run in runs} == {activity_id}


def test_multi_policy_retry_finishes_the_activity_that_owns_the_run(evaluation_services, monkeypatch):
    services = evaluation_services
    repository = PostgresPolicyRepository(services.context.database_url, WORKSPACE_ID)
    current = repository.list_scope("workspace", "")
    repository.replace_scope(
        "workspace",
        "",
        [{"name": rule.name, "criteria": rule.criteria, "approved": rule.approved}
         for rule in current],
        [{"name": rule.name, "criteria": rule.criteria, "approved": rule.approved}
         for rule in current]
        + [{"name": "Second rule", "criteria": "Check the second rule.", "approved": True}],
    )
    rules = services.policy_service.approved_rules("agent")
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.client, "get", lambda _: detail)
    services.evaluations.policy_triage = SimpleNamespace(
        model="test-decisions",
        triage=lambda trajectory, candidates: {
            "selected_policies": [
                {"id": rule.id, "name": rule.name, "scope": rule.scope}
                for rule in candidates
            ],
            "policy_decisions": [
                {"id": rule.id, "name": rule.name, "scope": rule.scope,
                 "decision": "suspected_violation"}
                for rule in candidates
            ],
        },
    )
    judged = []

    def judge(request):
        judged.append(request)
        if request["policy"]["name"] == "Rule" and len(judged) == 1:
            return {"status": "error", "rationale": "Judge unavailable."}
        return {"status": "pass", "rationale": "Reviewed.", "references": [], "limitations": []}

    monkeypatch.setattr(services.evaluations, "judge", judge)
    monkeypatch.setattr(services.client, "post", lambda *args, **kwargs: ({}, 201))

    first = services.evaluations.evaluate_session("agent", session_id)
    original_activity_id = first["activity_id"]
    assert first["status"] == "evaluation_error"
    assert len(first["evaluations"]) == 2
    assert first["evaluations"][1]["status"] == "pass"

    retried = services.evaluations.evaluate_session("agent", session_id)
    assert retried["status"] == "evaluated"
    assert retried["activity_id"] != original_activity_id
    assert len(judged) == 3

    store = services.evaluations.activities
    original_events = store.list_events(original_activity_id)
    retry_events = store.list_events(retried["activity_id"])
    assert original_events[-1].event_type == "succeeded"
    assert retry_events[-1].event_type == "succeeded"
    assert any(event.event_type == "evaluation_resumed" for event in retry_events)


def test_selected_pair_retries_judge_without_repeating_triage(evaluation_services, monkeypatch):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.client, "get", lambda _: detail)
    policy = services.policy_service.approved_rules("agent")[0]

    class SelectedTriager:
        model = "test-decisions-model"

        def __init__(self):
            self.calls = 0

        def triage(self, trajectory, rules):
            self.calls += 1
            return {
                "selected_policies": [
                    {"id": policy.id, "name": policy.name, "scope": policy.scope}
                ],
                "policy_decisions": [
                    {"id": policy.id, "name": policy.name, "scope": policy.scope,
                     "decision": "suspected_violation"}
                ],
            }

    triager = SelectedTriager()
    services.evaluations.policy_triage = triager
    judge_calls = []

    def judge(_request):
        judge_calls.append(True)
        if len(judge_calls) == 1:
            return {"status": "error", "rationale": "Temporary judge failure"}
        return {
            "status": "pass",
            "rationale": "The required check was completed.",
            "references": [],
            "limitations": [],
        }

    monkeypatch.setattr(services.evaluations, "judge", judge)
    monkeypatch.setattr(
        services.client,
        "post",
        lambda path, payload, *, include_status=False: ({}, 201)
        if include_status
        else {},
    )

    first = services.evaluations.evaluate_session("agent", session_id)
    second = services.evaluations.evaluate_session("agent", session_id)

    assert first["status"] == "evaluation_error"
    assert second["evaluations"][0]["status"] == "pass"
    assert triager.calls == 1
    assert len(judge_calls) == 2


def test_retry_reuses_stored_trajectory_and_idempotent_score_payload(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    persisted_scores = []
    judged = []
    monkeypatch.setattr(
        services.client,
        "get",
        lambda _: {**detail, "scores": list(persisted_scores)},
    )
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda request: (
            judged.append(request)
            or {
                "status": "pass",
                "rationale": "The stored trajectory was evaluated.",
                "references": [],
                "limitations": [],
            }
        ),
    )
    lost_response = True
    submitted = []

    def post(path, payload, *, include_status=False):
        nonlocal lost_response
        assert path == "/v1/scores"
        submitted.append(payload)
        if lost_response:
            lost_response = False
            persisted_scores.append(payload)
            raise RuntimeError("score response was lost")
        return ({}, 200) if include_status else {}

    monkeypatch.setattr(services.client, "post", post)
    with pytest.raises(RuntimeError, match="response was lost"):
        services.evaluations._evaluate_session("agent", "Rule", session_id)

    detail["spans"][0]["input"] = "changed telemetry after the first attempt"
    result = services.evaluations._evaluate_session("agent", "Rule", session_id)

    assert result["status"] == "pass"
    assert result["stored"] is True
    assert len(judged) == 1
    assert judged[0]["trajectory"]["events"][0]["content"] == "Refund my ticket"
    assert submitted[0] == submitted[1]
    store = PostgresActivityStore(services.context.database_url, WORKSPACE_ID)
    pending = store.find_pending_evaluations_for_session("agent", session_id)
    assert pending == []
    run = store.get_evaluation(result["evaluation_id"])
    assert run is not None
    assert run.status == "succeeded"
    assert store.find_pending_evaluations_for_session("agent", session_id) == []
    assert [event.event_type for event in store.list_events(run.activity_id)] == [
        "accepted",
        "claimed",
        "context_saved",
        "judging_started",
        "score_prepared",
        "retryable_error",
        "claimed",
        "succeeded",
    ]


def test_changed_trajectory_gets_a_new_tracked_evaluation_despite_existing_score(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    scores = []
    judged = []
    monkeypatch.setattr(
        services.client,
        "get",
        lambda _: {**detail, "scores": list(scores)},
    )
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda request: (
            judged.append(request)
            or {
                "status": "pass",
                "rationale": "The captured trajectory was evaluated.",
                "references": [],
                "limitations": [],
            }
        ),
    )

    def post(path, payload, *, include_status=False):
        scores.append(payload)
        return ({}, 201) if include_status else {}

    monkeypatch.setattr(services.client, "post", post)
    first = services.evaluations._evaluate_session("agent", "Rule", session_id)
    same = services.evaluations._evaluate_session("agent", "Rule", session_id)

    assert same["status"] == "already_evaluated"
    assert same["activity_id"] == first["activity_id"]
    assert len(judged) == len(scores) == 1

    detail["spans"].append(
        {
            **detail["spans"][0],
            "span_id": "new-evidence",
            "start_time": "2026-10-12T03:31:00Z",
            "attributes": {
                "sp.input": "I have now provided additional evidence.",
                "sp.output": "Additional response.",
            },
        }
    )
    second = services.evaluations._evaluate_session("agent", "Rule", session_id)

    assert first["status"] == second["status"] == "pass"
    assert first["activity_id"] != second["activity_id"]
    assert scores[0]["score_id"] != scores[1]["score_id"]
    assert scores[0]["metadata"]["trajectory_hash"] != scores[1]["metadata"]["trajectory_hash"]
    assert len(judged) == 2


def test_pending_score_recovers_after_policy_is_declined_and_telemetry_is_unavailable(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    policy = services.policy_service.approved_rules("agent")[0]
    policy_id = policy.id
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    posts = []
    monkeypatch.setattr(services.client, "get", lambda _: detail)
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda _: {
            "status": "pass",
            "rationale": "The stored trajectory was evaluated.",
            "references": [],
            "limitations": [],
        },
    )

    def fail_first_score_write(path, payload, *, include_status=False):
        posts.append(payload)
        if len(posts) == 1:
            raise RuntimeError("score response was lost")
        return ({}, 200) if include_status else {}

    monkeypatch.setattr(services.client, "post", fail_first_score_write)
    with pytest.raises(RuntimeError, match="response was lost"):
        services.evaluations._evaluate_session(
            "agent", "Rule", session_id, policy_id
        )

    pending = PostgresActivityStore(
        services.context.database_url, WORKSPACE_ID
    ).find_pending_evaluations_for_session("agent", session_id)
    assert len(pending) == 1
    assert pending[0][0].policy_id == policy_id

    save_rule(services, approved="no")
    with pytest.raises(ValueError, match="does not match the pending policy"):
        services.evaluations._evaluate_session(
            "agent", "Wrong name", session_id, policy_id
        )
    monkeypatch.setattr(
        services.client,
        "get",
        lambda _: (_ for _ in ()).throw(RuntimeError("telemetry unavailable")),
    )
    result = services.evaluations._evaluate_session(
        "agent", "Rule", session_id, policy_id
    )

    assert result["status"] == "pass"
    assert result["stored"] is True
    assert posts[0] == posts[1]


def test_evaluate_session_does_not_substitute_a_different_policy_for_selected_id(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda _: pytest.fail("a stale policy ID must not evaluate a different rule"),
    )

    with pytest.raises(ValueError, match="selected policy ID"):
        services.evaluations._evaluate_session("agent", "Rule", "session", "P1.999")


def test_workspace_policy_pending_recovery_is_scoped_to_target_agent(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    policy = services.policy_service.approved_rules("agent")[0]
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.client, "get", lambda _: detail)
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda _: {
            "status": "pass",
            "rationale": "The parent agent passed.",
            "references": [],
            "limitations": [],
        },
    )
    posts = []

    def interrupt_write(path, payload, *, include_status=False):
        posts.append(payload)
        raise RuntimeError("score response was lost")

    monkeypatch.setattr(services.client, "post", interrupt_write)
    with pytest.raises(RuntimeError, match="response was lost"):
        services.evaluations._evaluate_session(
            "agent", "Rule", session_id, policy.id
        )

    result = services.evaluations._evaluate_session(
        "child-agent", "Rule", session_id, policy.id
    )

    assert result["status"] == "skipped_agent_mismatch"
    assert len(posts) == 1


def _tracked_service_with_saved_inputs(tmp_path, *, approved_rules):
    context = WorkspaceContext(
        workspace=WORKSPACE_ID,
        data_dir=tmp_path,
        confirmed_policy_title="Rule",
    )
    activity_id = str(uuid4())
    evaluation_id = str(uuid4())
    claim = ActivityClaim(
        activity_id=activity_id,
        owner="lisa-evaluation",
        fencing_token=1,
        lease_expires_at=datetime.now(timezone.utc),
    )
    activity = EvaluationRun(
        id=evaluation_id,
        activity_id=activity_id,
        session_id="session",
        created=False,
        context_path=f"activities/{activity_id}",
        policy_id="P1.1",
    )
    store = MagicMock()
    store.create_evaluation.return_value = activity
    store.claim_activity.return_value = claim
    store.get_context_item_for_activity.return_value = SimpleNamespace(
        content={
            "session_id": "session",
            "trace_id": "trace",
            "policy_id": "P1.1",
            "policy_name": "Rule",
            "target_agent_name": "agent",
            "trajectory": {
                "events": [{"kind": "user_message", "content": "Refund my ticket"}],
                "complete": True,
                "tool_events_captured": True,
                "tool_order_certain": True,
                "message_order_certain": True,
                "limitations": [],
                "trace_ids": ["trace"],
            },
            "policy_selection": None,
        }
    )
    policies = MagicMock()
    policies.approved_rules.return_value = approved_rules
    service = EvaluationService(
        context=context,
        client=MagicMock(),
        policies=policies,
        failures=MagicMock(),
        notifier=MagicMock(),
        sessions=MagicMock(),
        policy_triage=MagicMock(),
        activities=store,
        artifact_repository=EvaluationArtifactRepository(context),
        render_context=render_evaluation_context,
    )
    return service, store, activity


def test_tracked_live_reload_releases_lease_when_policy_is_unapproved(tmp_path):
    service, store, activity = _tracked_service_with_saved_inputs(tmp_path, approved_rules=[])

    result = service._evaluate_tracked(
        {
            "name": "Rule",
            "policy_id": "P1.1",
            "target_agent_name": "agent",
            "scope": "workspace",
            "criteria": "Check eligibility before refunding.",
            "policy_hash": "hash-1",
        },
        "session",
        "trace",
        [],
        {
            "events": [],
            "complete": True,
            "tool_events_captured": True,
            "tool_order_certain": True,
            "message_order_certain": True,
            "limitations": [],
            "trace_ids": ["trace"],
        },
        "run-1",
        "request-1",
        activity,
        None,
    )

    assert result["status"] == "policy_unavailable"
    assert result["stored"] is False
    store.release_activity.assert_called_once()
    store.append_event.assert_not_called()


def test_tracked_retry_judges_live_criteria_after_interrupt_before_outbox(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.client, "get", lambda _: detail)
    old_policy_hash = services.evaluations._policy("agent", "Rule")["policy_hash"]
    judged = []

    def judge_once(request):
        judged.append(request)
        if len(judged) == 1:
            raise RuntimeError("judge interrupted before outbox")
        return {
            "status": "pass",
            "rationale": "Live criteria were judged.",
            "references": [],
            "limitations": [],
        }

    monkeypatch.setattr(services.evaluations, "judge", judge_once)
    scores = []
    monkeypatch.setattr(
        services.client,
        "post",
        lambda path, payload, *, include_status=False: (
            scores.append(payload) or (({}, 201) if include_status else {})
        ),
    )

    with pytest.raises(RuntimeError, match="judge interrupted"):
        services.evaluations._evaluate_session("agent", "Rule", session_id)

    save_rule(services, text="Require confirmation before refunding.")
    current_policy_hash = services.evaluations._policy("agent", "Rule")["policy_hash"]
    result = services.evaluations._evaluate_session("agent", "Rule", session_id)

    assert result["status"] == "pass"
    assert len(judged) == 2
    assert judged[1]["policy"]["criteria"] == "Require confirmation before refunding."
    assert scores[0]["metadata"]["criteria"] == "Require confirmation before refunding."
    assert scores[0]["metadata"]["policy_hash"] == current_policy_hash
    assert current_policy_hash != old_policy_hash
    store = PostgresActivityStore(services.context.database_url, WORKSPACE_ID)
    run = store.get_evaluation(result["evaluation_id"])
    assert run is not None
    assert run.status == "succeeded"
    assert store.get_context_item_for_activity(run.activity_id, f"evaluation_inputs_{run.policy_id}") is not None


def test_tracked_retry_releases_lease_when_policy_is_unapproved_after_interrupt(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.client, "get", lambda _: detail)
    created = []
    store = services.evaluations.activities
    create_evaluation = store.create_evaluation
    monkeypatch.setattr(
        store,
        "create_evaluation",
        lambda **values: (created.append(create_evaluation(**values)) or created[-1]),
    )
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda _: (_ for _ in ()).throw(RuntimeError("judge interrupted before outbox")),
    )

    with pytest.raises(RuntimeError, match="judge interrupted"):
        services.evaluations._evaluate_session("agent", "Rule", session_id)

    assert len(created) == 1
    activity_id = created[0].activity_id

    run = store.get_evaluation(created[0].id)
    assert run is not None
    assert run.status != "running"

    save_rule(services, approved="no")
    with pytest.raises(ValueError, match="No approved policy"):
        services.evaluations._evaluate_session("agent", "Rule", session_id)
    run = store.get_evaluation(created[0].id)
    assert run is not None
    assert run.status != "running"
    events = [event.event_type for event in store.list_events(activity_id)]
    assert "retryable_error" in events
