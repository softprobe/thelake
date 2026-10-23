from __future__ import annotations

import json
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
from lisa.services.chat_activities import record_chat_activity_trajectories
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
                    "policy_id": "P1.9",
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
    context = lisa_test_context(WORKSPACE_ID)
    services = build_services(context)
    save_rule(services)
    monkeypatch.setattr(services.sessions.traces, "load", lambda _: session_detail())
    return services


def test_evaluate_session_persists_run_context_score_and_ordered_timeline(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.sessions.traces, "load", lambda _: detail)
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

    result = services.evaluations._evaluate_session("agent", "Rule", session_id)

    assert result["status"] == "pass"
    assert result["activity_id"]

    store = PostgresActivityStore(services.context.database_url, WORKSPACE_ID)
    run = store.get_evaluation(result["evaluation_id"])
    assert run is not None
    assert run.verdict == "pass"
    assert run.policy_id == services.policy_service.approved_rules("agent")[0].id
    outbox = store.get_score_outbox(run.id)
    assert outbox.status == "acknowledged"
    assert run.score_id == outbox.score_id
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


def test_session_evaluation_reuses_the_resolved_chat_activity(evaluation_services):
    services = evaluation_services
    store = services.activity_store
    session_id = f"session-{uuid4()}"
    policy = services.policy_service.approved_rules("agent")[0]
    chat_activity = store.create_activity(
        title="Review billing agent session", source="web", kind="chat_request",
    )

    class NegativeTriager:
        model = "test-decisions-model"

        def triage(self, trajectory, rules):
            return {
                "selected_policies": [],
                "policy_decisions": [
                    {"id": policy.id, "name": policy.name, "scope": policy.scope,
                     "decision": "no_violation_found"}
                ],
            }

    services.evaluations.policy_triage = NegativeTriager()
    result = services.evaluations.evaluate_session(
        "agent", session_id, activity_id=chat_activity.id,
    )

    feed = store.list_activity_feed(limit=100)
    assert [item["id"] for item in feed].count(chat_activity.id) == 1
    assert result["activity_id"] == chat_activity.id
    assert {event.event_type for event in store.list_events(chat_activity.id)} >= {
        "policy_triage_decision", "policy_review_completed", "succeeded",
    }


@pytest.mark.parametrize(
    ("outcome", "expected_status"),
    (("error", "error"), ("no_approved_policies", "success")),
)
def test_chat_evaluation_defers_terminal_write_until_trajectory_is_persisted(
    evaluation_services, monkeypatch, outcome, expected_status,
):
    services = evaluation_services
    store = services.activity_store
    turn_id = f"evaluation-chat-turn-{uuid4()}"
    activity = store.resolve_chat_activity(
        turn_id=turn_id,
        request_key="review-session",
        disposition="create",
        source="web",
        title="Review missing session",
        summary="Review the linked session.",
    )

    if outcome == "error":
        def missing_session(_path):
            raise LookupError("not found")

        monkeypatch.setattr(services.sessions.traces, "load", missing_session)
    else:
        monkeypatch.setattr(services.policy_service, "approved_rules", lambda _: [])
    result = services.evaluations.evaluate_session(
        "billing-agent",
        "missing-session",
        activity_id=activity.id,
        defer_activity_finalization=True,
    )

    assert result["status"] == outcome
    assert result["activity_finalized"] is False
    assert store.get_activity_thread(activity.id)["activity"]["status"] == "pending"
    record_chat_activity_trajectories(
        store,
        activity_ids=[activity.id],
        messages=[{
            "id": "assistant-error-reply",
            "role": "assistant",
            "text": "I could not find that session.",
        }],
        activities=[
            {
                "id": "resolved-request-result",
                "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": json.dumps({
                    "activity_id": activity.id,
                    "disposition": "create",
                }),
                "sequence": 1,
            },
            {
                "id": "evaluation-error-result",
                "kind": "evidence",
                "tool_name": "evaluate_session",
                "detail": json.dumps(result),
                "sequence": 2,
            },
        ],
        final_summary="The requested session was not found.",
        turn_id=turn_id,
    )

    detail = store.get_activity_thread(activity.id)
    assert detail["activity"]["status"] == expected_status
    assert any(
        event.event_type == "chat.assistant"
        for event in store.list_events(activity.id)
    )


def test_selected_chat_evaluation_finishes_after_its_final_trajectory(
    evaluation_services, monkeypatch,
):
    services = evaluation_services
    store = services.activity_store
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.sessions.traces, "load", lambda _: detail)
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda _: {
            "status": "pass", "rationale": "The policy was followed.",
            "references": [], "limitations": [],
        },
    )
    policy = services.policy_service.approved_rules("agent")[0]
    turn_id = f"selected-evaluation-chat-turn-{uuid4()}"
    activity = store.resolve_chat_activity(
        turn_id=turn_id,
        request_key="review-session",
        disposition="create",
        source="web",
        title="Review agent session",
        summary="Evaluate the selected session.",
    )

    class SelectedTriager:
        model = "test-decisions-model"

        def triage(self, trajectory, rules):
            return {
                "selected_policies": [
                    {"id": policy.id, "name": policy.name, "scope": policy.scope}
                ],
                "policy_decisions": [
                    {"id": policy.id, "name": policy.name, "scope": policy.scope,
                     "decision": "suspected_violation"}
                ],
            }

    services.evaluations.policy_triage = SelectedTriager()
    result = services.evaluations.evaluate_session(
        "agent",
        session_id,
        activity_id=activity.id,
        defer_activity_finalization=True,
    )

    assert result["status"] == "evaluated"
    assert result["activity_finalized"] is False
    assert store.get_activity_thread(activity.id)["activity"]["status"] == "pending"
    record_chat_activity_trajectories(
        store,
        activity_ids=[activity.id],
        messages=[{
            "id": "assistant-evaluation-reply",
            "role": "assistant",
            "text": "The session passed the policy review.",
        }],
        activities=[
            {
                "id": "resolved-selected-request",
                "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": json.dumps({
                    "activity_id": activity.id,
                    "disposition": "create",
                }),
                "sequence": 1,
            },
            {
                "id": "selected-evaluation-result",
                "kind": "evidence",
                "tool_name": "evaluate_session",
                "detail": json.dumps(result),
                "sequence": 2,
            },
        ],
        final_summary="The session passed the policy review.",
        turn_id=turn_id,
    )

    detail = store.get_activity_thread(activity.id)
    assert detail["activity"]["status"] == "success"
    assert any(
        event.event_type == "chat.assistant"
        for event in store.list_events(activity.id)
    )


def test_unavailable_pending_evaluation_claim_keeps_chat_activity_retryable(
    evaluation_services, monkeypatch,
):
    services = evaluation_services
    store = services.activity_store
    turn_id = f"pending-evaluation-chat-turn-{uuid4()}"
    activity = store.resolve_chat_activity(
        turn_id=turn_id,
        request_key="recover-pending-evaluation",
        disposition="create",
        source="web",
        title="Recover pending evaluation",
        summary="Finish the interrupted evaluation.",
    )
    policy = services.evaluations._policy("agent", "Rule")
    run = store.create_evaluation(
        title="Review session",
        source="web",
        target_agent_name="agent",
        session_id="pending-session",
        policy_name=policy["name"],
        policy_id=policy["policy_id"],
        policy_hash=policy["policy_hash"],
        judge_config={},
        request_key=str(uuid4()),
    )
    store.claim_activity(run.activity_id, "evaluation-worker", lease_seconds=300)
    outbox = ScoreOutboxItem(
        id=str(uuid4()),
        evaluation_id=run.id,
        status="pending",
        score_payload={},
        score_id=None,
    )
    monkeypatch.setattr(
        store,
        "find_pending_evaluations_for_session",
        lambda _agent, _session: [(run, outbox)],
    )

    result = services.evaluations.evaluate_session(
        "agent",
        "pending-session",
        activity_id=activity.id,
        defer_activity_finalization=True,
    )

    assert result["status"] == "evaluation_error"
    assert result["activity_finalized"] is False
    record_chat_activity_trajectories(
        store,
        activity_ids=[activity.id],
        messages=[{
            "id": "assistant-pending-evaluation-reply",
            "role": "assistant",
            "text": "The evaluation is still running; I will retry it.",
        }],
        activities=[
            {
                "id": "resolved-pending-evaluation-request",
                "kind": "evidence",
                "tool_name": "resolve_chat_activity",
                "detail": json.dumps({
                    "activity_id": activity.id,
                    "disposition": "create",
                }),
                "sequence": 1,
            },
            {
                "id": "pending-evaluation-result",
                "kind": "evidence",
                "tool_name": "evaluate_session",
                "detail": json.dumps(result),
                "sequence": 2,
            },
        ],
        final_summary="The evaluation is still running.",
        turn_id=turn_id,
    )

    detail = store.get_activity_thread(activity.id)
    assert detail["activity"]["status"] == "blocked"
    assert detail["activity"]["next_action"] == "Retry the incomplete session evaluation"
    # The other worker owns the still-open evaluation activity (presented as pending).
    assert store.get_activity_thread(run.activity_id)["activity"]["status"] == "pending"


def test_missing_chat_session_finishes_activity_as_error_even_without_approved_policies(
    evaluation_services, monkeypatch,
):
    services = evaluation_services
    activity = services.activity_store.create_activity(
        title="Review missing session", source="web", kind="chat_request",
    )
    services.activity_store.update_activity_progress(
        activity.id, status="waiting", summary="Waiting for session review.",
    )
    monkeypatch.setattr(services.policy_service, "approved_rules", lambda _: [])

    def missing_session(_path):
        raise LookupError("not found")

    monkeypatch.setattr(services.sessions.traces, "load", missing_session)

    result = services.evaluations.evaluate_session(
        "billing-agent", "missing-session", activity_id=activity.id,
    )

    assert result["status"] == "error"
    assert result["activity_id"] == activity.id
    thread = services.activity_store.get_activity_thread(activity.id)
    assert thread["thread"]["status"] == "error"
    assert any(
        "missing-session" in (event.get("summary") or "")
        for event in thread["actions"]
    )


def test_missing_chat_session_reports_when_live_worker_owns_activity(
    evaluation_services, monkeypatch,
):
    services = evaluation_services
    store = services.activity_store
    activity = store.create_activity(
        title="Review missing session", source="web", kind="chat_request",
    )
    claim = store.claim_activity(activity.id, "active-evaluator", lease_seconds=60)
    assert claim is not None

    def missing_session(_path):
        raise LookupError("not found")

    monkeypatch.setattr(services.sessions.traces, "load", missing_session)

    result = services.evaluations.evaluate_session(
        "billing-agent", "missing-session", activity_id=activity.id,
    )

    assert result["status"] == "error"
    assert result["activity_finalized"] is False
    thread = store.get_activity_thread(activity.id)
    assert thread["thread"]["status"] == "pending"
    continued_claim = store.claim_activity(
        activity.id, "active-evaluator", lease_seconds=60,
    )
    assert continued_claim is not None
    assert continued_claim.fencing_token == claim.fencing_token


def test_selected_chat_evaluation_finishes_activity_with_its_worker_claim(
    evaluation_services, monkeypatch,
):
    services = evaluation_services
    store = services.activity_store
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.sessions.traces, "load", lambda _: detail)
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda _: {
            "status": "pass", "rationale": "The policy was followed.",
            "references": [], "limitations": [],
        },
    )
    policy = services.policy_service.approved_rules("agent")[0]
    chat_activity = store.create_activity(
        title="Review agent session", source="web", kind="chat_request",
    )

    class SelectedTriager:
        model = "test-decisions-model"

        def triage(self, trajectory, rules):
            return {
                "selected_policies": [
                    {"id": policy.id, "name": policy.name, "scope": policy.scope}
                ],
                "policy_decisions": [
                    {"id": policy.id, "name": policy.name, "scope": policy.scope,
                     "decision": "suspected_violation"}
                ],
            }

    services.evaluations.policy_triage = SelectedTriager()
    result = services.evaluations.evaluate_session(
        "agent", session_id, activity_id=chat_activity.id,
    )

    assert result["activity_id"] == chat_activity.id
    assert result["evaluations"][0]["status"] == "pass"
    assert "_activity_fencing_token" not in result["evaluations"][0]
    assert store.get_activity_thread(chat_activity.id)["thread"]["status"] == "success"


def test_mixed_chat_evaluation_results_never_return_activity_claim_tokens(
    evaluation_services, monkeypatch,
):
    services = evaluation_services
    store = services.activity_store
    chat_activity = store.create_activity(
        title="Review two policies", source="web", kind="chat_request",
    )
    first, second = services.policy_service.approved_rules("agent") * 2
    second = SimpleNamespace(**{**first.__dict__, "id": "P1.99", "name": "Second policy"})
    rules = [first, second]
    monkeypatch.setattr(services.evaluations.policies, "approved_rules", lambda _: rules)
    monkeypatch.setattr(services.activity_store, "record_policy_triage_decisions", lambda *_: None)
    monkeypatch.setattr(
        services.activity_store,
        "policy_triage_decisions",
        lambda _session, ids: {rule_id: "suspected_violation" for rule_id in ids},
    )
    monkeypatch.setattr(services.sessions.traces, "load", lambda _: session_detail())

    class SelectedTriager:
        model = "test-decisions-model"

        def triage(self, trajectory, candidates):
            return {
                "selected_policies": [
                    {"id": rule.id, "name": rule.name, "scope": rule.scope}
                    for rule in candidates
                ],
                "policy_decisions": [
                    {"id": rule.id, "name": rule.name, "scope": rule.scope,
                     "decision": "suspected_violation"}
                    for rule in candidates
                ],
            }

    services.evaluations.policy_triage = SelectedTriager()
    evaluations = iter([
        {
            "status": "pass", "activity_id": chat_activity.id,
            "_activity_fencing_token": 17,
        },
        {"status": "error", "activity_id": chat_activity.id},
    ])
    monkeypatch.setattr(
        services.evaluations,
        "_evaluate_session",
        lambda *args, **kwargs: next(evaluations),
    )

    result = services.evaluations.evaluate_session(
        "agent", f"session-{uuid4()}", activity_id=chat_activity.id,
    )

    assert result["status"] == "evaluation_error"
    assert all("_activity_fencing_token" not in item for item in result["evaluations"])


def test_chat_evaluation_releases_each_policy_claim_for_chat_finalization(
    evaluation_services, monkeypatch,
):
    services = evaluation_services
    store = services.activity_store
    chat_activity = store.create_activity(
        title="Review two policies", source="web", kind="chat_request",
    )
    first, second = services.policy_service.approved_rules("agent") * 2
    second = SimpleNamespace(**{**first.__dict__, "id": "P1.99", "name": "Second policy"})
    rules = [first, second]
    monkeypatch.setattr(services.evaluations.policies, "approved_rules", lambda _: rules)
    monkeypatch.setattr(services.activity_store, "record_policy_triage_decisions", lambda *_: None)
    monkeypatch.setattr(
        services.activity_store,
        "policy_triage_decisions",
        lambda _session, ids: {rule_id: "suspected_violation" for rule_id in ids},
    )
    monkeypatch.setattr(services.sessions.traces, "load", lambda _: session_detail())

    class SelectedTriager:
        model = "test-decisions-model"

        def triage(self, trajectory, candidates):
            return {
                "selected_policies": [
                    {"id": rule.id, "name": rule.name, "scope": rule.scope}
                    for rule in candidates
                ],
                "policy_decisions": [
                    {"id": rule.id, "name": rule.name, "scope": rule.scope,
                     "decision": "suspected_violation"}
                    for rule in candidates
                ],
            }

    services.evaluations.policy_triage = SelectedTriager()
    evaluation_results = iter([
        {"status": "pass", "activity_id": chat_activity.id, "_activity_fencing_token": 17},
        {"status": "pass", "activity_id": chat_activity.id, "_activity_fencing_token": 18},
    ])
    order = []

    def evaluate(*_args, **_kwargs):
        result = next(evaluation_results)
        order.append(("evaluate", result["_activity_fencing_token"]))
        return result

    def release(activity_id, *, actor, fencing_token):
        assert activity_id == chat_activity.id
        assert actor == "lisa-evaluation"
        order.append(("release", fencing_token))

    monkeypatch.setattr(services.evaluations, "_evaluate_session", evaluate)
    monkeypatch.setattr(store, "release_chat_evaluation_claim", release)

    result = services.evaluations.evaluate_session(
        "agent",
        f"session-{uuid4()}",
        activity_id=chat_activity.id,
        defer_activity_finalization=True,
    )

    assert result["status"] == "evaluated"
    assert result["activity_finalized"] is False
    assert order == [
        ("evaluate", 17), ("release", 17),
        ("evaluate", 18), ("release", 18),
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
    monkeypatch.setattr(services.sessions.traces, "load", lambda _: detail)
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

    first = services.evaluations.evaluate_session("agent", session_id)
    second = services.evaluations.evaluate_session("agent", session_id)

    assert first["evaluations"][0]["status"] == "pass"
    assert second["evaluations"][0]["status"] == "already_evaluated"
    assert triager.calls == len(judged) == 1


def test_one_inspectable_activity_records_triage_and_links_multiple_evaluations(evaluation_services, monkeypatch):
    services = evaluation_services
    repository = PostgresPolicyRepository(services.context.database_url, WORKSPACE_ID)
    current = repository.list_scope("workspace", "")
    repository.seed_scope(
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
    monkeypatch.setattr(services.sessions.traces, "load", lambda _: detail)
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
    repository.seed_scope(
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
    monkeypatch.setattr(services.sessions.traces, "load", lambda _: detail)
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
    monkeypatch.setattr(services.sessions.traces, "load", lambda _: detail)
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

    first = services.evaluations.evaluate_session("agent", session_id)
    second = services.evaluations.evaluate_session("agent", session_id)

    assert first["status"] == "evaluation_error"
    assert second["evaluations"][0]["status"] == "pass"
    assert triager.calls == 1
    assert len(judge_calls) == 2


def test_changed_trajectory_gets_a_new_tracked_evaluation_despite_existing_score(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    judged = []
    monkeypatch.setattr(
        services.sessions.traces,
        "load",
        lambda _: detail,
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

    first = services.evaluations._evaluate_session("agent", "Rule", session_id)
    same = services.evaluations._evaluate_session("agent", "Rule", session_id)

    assert same["status"] == "already_evaluated"
    assert same["activity_id"] == first["activity_id"]
    assert len(judged) == 1

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
    assert len(judged) == 2
    assert first["evaluation_id"] != second["evaluation_id"]


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


def _tracked_service_with_saved_inputs(tmp_path, *, approved_rules):
    context = WorkspaceContext(
        workspace=WORKSPACE_ID,
        data_dir=tmp_path,
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
    monkeypatch.setattr(services.sessions.traces, "load", lambda _: detail)
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
    with pytest.raises(RuntimeError, match="judge interrupted"):
        services.evaluations._evaluate_session("agent", "Rule", session_id)

    save_rule(services, text="Require confirmation before refunding.")
    current_policy_hash = services.evaluations._policy("agent", "Rule")["policy_hash"]
    result = services.evaluations._evaluate_session("agent", "Rule", session_id)

    assert result["status"] == "pass"
    assert len(judged) == 2
    assert judged[1]["policy"]["criteria"] == "Require confirmation before refunding."
    store = PostgresActivityStore(services.context.database_url, WORKSPACE_ID)
    outbox = store.get_score_outbox(result["evaluation_id"])
    assert outbox is not None
    assert outbox.score_payload["metadata"]["criteria"] == "Require confirmation before refunding."
    assert outbox.score_payload["metadata"]["policy_hash"] == current_policy_hash
    assert current_policy_hash != old_policy_hash
    store = PostgresActivityStore(services.context.database_url, WORKSPACE_ID)
    run = store.get_evaluation(result["evaluation_id"])
    assert run is not None
    assert run.status == "succeeded"
    assert store.get_context_item_for_activity(run.activity_id, f"evaluation_inputs_{run.policy_id}") is not None


def test_tracked_retry_releases_lease_when_policy_is_removed_after_interrupt(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.sessions.traces, "load", lambda _: detail)
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

    policy_repository = services.policy_service.repository
    current = policy_repository.list_scope("workspace")
    expected = [
        {"name": row.name, "criteria": row.criteria, "approved": row.approved}
        for row in current
    ]
    policy_repository.seed_scope(
        "workspace",
        "",
        expected,
        [
            {"name": row["name"], "criteria": row["criteria"], "approved": False}
            for row in expected
        ],
    )
    with pytest.raises(ValueError, match="No approved policy"):
        services.evaluations._evaluate_session("agent", "Rule", session_id)
    run = store.get_evaluation(created[0].id)
    assert run is not None
    assert run.status != "running"
    events = [event.event_type for event in store.list_events(activity_id)]
    assert "retryable_error" in events
