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
from lisa.models.work_items import EvaluationWorkItem, ScoreOutboxItem, WorkItemClaim
from lisa.repositories.lisa_database import LisaDatabaseReader
from lisa.repositories.work_items import PostgresWorkItemStore
from lisa.views.evaluations import render_evaluation_context

WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"


def test_pending_score_accepts_stale_payload_policy_id():
    work_item_id = "00000000-0000-0000-0000-000000000001"
    pending = (
        EvaluationWorkItem(
            id=work_item_id,
            session_id="session",
            created=False,
            policy_id="P1.10",
        ),
        ScoreOutboxItem(
            id="00000000-0000-0000-0000-000000000002",
            work_item_id=work_item_id,
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
    assert result["work_item_id"]

    store = PostgresWorkItemStore(services.context.database_url, WORKSPACE_ID)
    run = store.get_evaluation(result["work_item_id"])
    assert run is not None
    assert run.verdict == "pass"
    assert run.policy_id == services.policy_service.approved_rules("agent")[0].id
    assert scores[0]["metadata"]["policy_id"] == run.policy_id
    outbox = store.get_score_outbox(run.id)
    assert outbox.status == "acknowledged"
    assert run.score_id == outbox.score_id == scores[0]["score_id"]
    events = store.list_events(run.id)
    assert [event.event_type for event in events] == [
        "accepted",
        "claimed",
        "context_saved",
        "judging_started",
        "score_prepared",
        "succeeded",
    ]
    context_item = store.get_context_item_for_work_item(run.id, "evaluation_inputs")
    assert context_item.content["session_id"] == session_id
    assert context_item.content["policy_id"] == run.policy_id
    assert "policy" not in context_item.content
    assert context_item.content["trajectory"]["events"]

    run_dir = services.context.home() / "work-items" / run.id
    assert (run_dir / "inputs.json").exists()
    assert (run_dir / "context.md").exists()
    assert run.context_path == f"work-items/{run.id}"

    reader = LisaDatabaseReader(services.context.database_read_url, WORKSPACE_ID)
    visible = reader.query(read_sql_case("readable_work_item").replace("WORK_ITEM_ID", run.id))
    assert visible["rows"] == [{"status": "succeeded", "verdict": "pass"}]
    with pytest.raises(ValueError, match="approved Lisa views or tables"):
        reader.query(read_sql_case("reject_base_table"))


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
    store = PostgresWorkItemStore(services.context.database_url, WORKSPACE_ID)
    pending = store.find_pending_evaluations_for_session("agent", session_id)
    assert pending == []
    run = store.get_evaluation(result["work_item_id"])
    assert run is not None
    assert run.status == "succeeded"
    assert store.find_pending_evaluations_for_session("agent", session_id) == []
    assert [event.event_type for event in store.list_events(run.id)] == [
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
    assert same["work_item_id"] == first["work_item_id"]
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
    assert first["work_item_id"] != second["work_item_id"]
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

    pending = PostgresWorkItemStore(
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
    work_item_id = str(uuid4())
    claim = WorkItemClaim(
        work_item_id=work_item_id,
        owner="lisa-evaluation",
        fencing_token=1,
        lease_expires_at=datetime.now(timezone.utc),
    )
    work_item = EvaluationWorkItem(
        id=work_item_id,
        session_id="session",
        created=False,
        context_path=f"work-items/{work_item_id}",
        policy_id="P1.1",
    )
    store = MagicMock()
    store.create_evaluation.return_value = work_item
    store.claim_work_item.return_value = claim
    store.get_context_item_for_work_item.return_value = SimpleNamespace(
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
        work_items=store,
        artifact_repository=EvaluationArtifactRepository(context),
        render_context=render_evaluation_context,
    )
    return service, store, work_item


def test_tracked_live_reload_releases_lease_when_policy_is_unapproved(tmp_path):
    service, store, work_item = _tracked_service_with_saved_inputs(tmp_path, approved_rules=[])

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
        work_item,
        None,
    )

    assert result["status"] == "policy_unavailable"
    assert result["stored"] is False
    store.release_work_item.assert_called_once()
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
    store = PostgresWorkItemStore(services.context.database_url, WORKSPACE_ID)
    run = store.get_evaluation(result["work_item_id"])
    assert run is not None
    assert run.status == "succeeded"
    assert store.get_context_item_for_work_item(run.id, "evaluation_inputs") is not None


def test_tracked_retry_releases_lease_when_policy_is_unapproved_after_interrupt(
    evaluation_services, monkeypatch
):
    services = evaluation_services
    session_id = f"session-{uuid4()}"
    detail = session_detail()
    detail["spans"][0]["trace_id"] = session_id
    monkeypatch.setattr(services.client, "get", lambda _: detail)
    created = []
    store = services.evaluations.work_items
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
    work_item_id = created[0].id

    run = store.get_evaluation(work_item_id)
    assert run is not None
    assert run.status != "running"

    save_rule(services, approved="no")
    with pytest.raises(ValueError, match="No approved policy"):
        services.evaluations._evaluate_session("agent", "Rule", session_id)
    run = store.get_evaluation(work_item_id)
    assert run is not None
    assert run.status != "running"
    events = [event.event_type for event in store.list_events(work_item_id)]
    assert "retryable_error" in events
