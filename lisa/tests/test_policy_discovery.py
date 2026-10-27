import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.models.policy_discovery import PolicyGapSignal, PolicySuggestionDraft
from lisa.services.policy_discovery import PolicyDiscoveryService


def test_gap_signal_requires_citations_for_a_concrete_concern():
    with pytest.raises(ValueError):
        PolicyGapSignal.model_validate({
            "detected": True,
            "summary": "The agent performed a consequential action without authorization.",
            "span_ids": [],
        })


def test_suggestion_must_cite_spans_from_the_selected_session():
    draft = PolicySuggestionDraft.model_validate({
        "name": "Confirm consequential actions",
        "criteria": "Ask for confirmation before a consequential action.",
        "scope": "workspace",
        "target_agent_name": "",
        "rationale": "The same uncovered behavior occurred in multiple sessions.",
        "signal_ids": ["signal-1"],
    })
    assert draft.scope == "workspace"
    with pytest.raises(ValueError):
        PolicyDiscoveryService.validate_references(
            ["missing-span"], {"span-1"}
        )
    PolicyDiscoveryService.validate_references(["span-1"], {"span-1"})


def test_suggestion_synthesis_serializes_persisted_postgres_values(monkeypatch):
    from lisa.evaluation import policy_discovery

    captured = {}

    def capture(model, *, contents, **_kwargs):
        captured["payload"] = json.loads(contents)
        return model.model_validate({"suggestions": []})

    monkeypatch.setattr(policy_discovery, "_generate", capture)
    signal_id = uuid4()
    created_at = datetime(2026, 10, 25, 10, 15, tzinfo=UTC)

    assert policy_discovery.synthesize_policy_suggestions(
        signals=[{
            "id": signal_id,
            "session_id": "benchmark-session",
            "created_at": created_at,
            "evidence": [],
        }],
        policies=[],
        reviewed=[],
    ) == []

    assert captured["payload"]["signals"][0]["id"] == str(signal_id)
    assert captured["payload"]["signals"][0]["created_at"] == created_at.isoformat()


def test_hourly_discovery_limit_is_bounded_and_configured_per_workspace():
    service = PolicyDiscoveryService(
        repository=None,
        policy_service=None,
        sessions=None,
        hourly_sample_limit=5,
        clock=lambda: datetime(2026, 10, 25, 10, 15, tzinfo=UTC),
    )
    assert service.hourly_sample_limit == 5
    with pytest.raises(ValueError):
        PolicyDiscoveryService(
            repository=None,
            policy_service=None,
            sessions=None,
            hourly_sample_limit=0,
        )


def test_policy_discovery_samples_are_capped_per_hour_and_idempotent(
    lisa_test_database, monkeypatch, tmp_path
):
    workspace = str(uuid4())
    database = lisa_test_database(workspace)
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace)
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    services = build_services(WorkspaceContext.from_environment())
    discovery = services.policy_discovery
    activity = services.activity_store.create_activity(
        title="Discovery sample", source="session_push", kind="session_evaluation"
    )
    now = datetime(2026, 10, 25, 10, 15, tzinfo=UTC)
    for index in range(5):
        assert discovery.repository.reserve_sample(
            session_id=f"session-{index}", agent_name="agent",
            activity_id=activity.id, hourly_limit=5, now=now,
        )
    assert discovery.repository.reserve_sample(
        session_id="session-0", agent_name="agent", activity_id=activity.id,
        hourly_limit=5, now=now,
    )
    assert not discovery.repository.reserve_sample(
        session_id="session-5", agent_name="agent", activity_id=activity.id,
        hourly_limit=5, now=now,
    )


def test_successful_empty_consolidation_marks_the_entire_batch_reviewed(
    lisa_test_database, monkeypatch, tmp_path
):
    workspace = str(uuid4())
    database = lisa_test_database(workspace)
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace)
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    services = build_services(WorkspaceContext.from_environment())
    activity = services.activity_store.create_activity(
        title="Discovery evidence", source="session_push", kind="session_evaluation"
    )
    for index in range(21):
        services.policy_discovery.repository.save_signal(
            session_id=f"batch-session-{index}", activity_id=activity.id,
            agent_name="agent", summary="Observed a possible uncovered behavior.",
            span_ids=[f"span-{index}"], evidence=[{"span_id": f"span-{index}"}],
        )
    services.policy_discovery.synthesizer = lambda *_: []

    assert services.policy_discovery.synthesize_daily() == []
    assert len(services.policy_discovery.repository.pending_signals(limit=100)) == 1


def test_failed_synthesis_keeps_signals_for_tomorrow_without_retrying_today(
    lisa_test_database, monkeypatch, tmp_path
):
    workspace = str(uuid4())
    database = lisa_test_database(workspace)
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace)
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    services = build_services(WorkspaceContext.from_environment())
    activity = services.activity_store.create_activity(
        title="Discovery evidence", source="session_push", kind="session_evaluation"
    )
    services.policy_discovery.repository.save_signal(
        session_id="retry-next-day", activity_id=activity.id, agent_name="agent",
        summary="A concrete uncovered behavior.", span_ids=["span-1"],
        evidence=[{"span_id": "span-1"}],
    )

    def fail_synthesis(*_args):
        raise RuntimeError("provider unavailable")

    services.policy_discovery.synthesizer = fail_synthesis
    with pytest.raises(RuntimeError, match="provider unavailable"):
        services.policy_discovery.synthesize_daily()

    assert len(services.policy_discovery.repository.pending_signals(limit=10)) == 1
    assert not services.policy_discovery.repository.claim_daily_synthesis()


def test_suggestion_approval_creates_active_policy_atomically_and_owner_gates(
    lisa_test_database, monkeypatch, tmp_path
):
    workspace = str(uuid4())
    database = lisa_test_database(workspace)
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace)
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_OWNER_USER_IDS", "owner-1")
    context = WorkspaceContext.from_environment()
    services = build_services(context)
    activity = services.activity_store.create_activity(
        title="Policy discovery evidence", source="session_push", kind="session_evaluation"
    )
    signal_id = services.policy_discovery.repository.save_signal(
        session_id="session-evidence", activity_id=activity.id, agent_name="agent",
        summary="The agent took an important action without adequate confirmation.",
        span_ids=["span-user", "span-tool"],
        evidence=[
            {"span_id": "span-user", "kind": "user_message", "content": "Do this"},
            {"span_id": "span-tool", "kind": "tool_call", "name": "change_state"},
        ],
    )
    draft = PolicySuggestionDraft.model_validate({
        "name": "Confirm consequential actions",
        "criteria": "Ask the user before taking a consequential action.",
        "scope": "agent", "target_agent_name": "agent",
        "rationale": "The same gap was observed repeatedly.", "signal_ids": [signal_id],
    })
    signal_rows = services.policy_discovery.repository.pending_signals()
    saved = services.policy_discovery.repository.create_suggestions(
        drafts=[draft], signal_rows=signal_rows,
        overlaps_by_draft=[[]],
    )[0]
    duplicate = services.policy_discovery.repository.create_suggestions(
        drafts=[draft], signal_rows=signal_rows,
        overlaps_by_draft=[[]],
    )[0]
    assert duplicate["id"] == saved["id"]
    assert saved["evidence"][0]["evidence"][1]["span_id"] == "span-tool"
    with pytest.raises(ValueError, match="authorized"):
        services.policy_discovery.approve(suggestion_id=saved["id"], actor_id="guest")
    result = services.policy_discovery.approve(suggestion_id=saved["id"], actor_id="owner-1")
    assert result["policy"]["approved"] is True
    assert result["suggestion"]["status"] == "approved"
    replay = services.policy_discovery.approve(suggestion_id=saved["id"], actor_id="owner-1")
    assert replay["policy"]["id"] == result["policy"]["id"]
    assert len(services.policy_service.approved_rules("agent")) == 1


def test_policy_gap_citations_must_resolve_inside_the_trajectory():
    class Repo:
        def save_signal(self, **kwargs):
            self.saved = kwargs
            return "signal-1"

    class Sessions:
        def get_trajectory(self, _session_id):
            return {"events": [{"span_id": "span-1", "kind": "user_message", "content": "Help"}]}

    repo = Repo()
    service = PolicyDiscoveryService(repo, None, Sessions())
    signal = PolicyGapSignal(detected=True, summary="Uncovered behavior", span_ids=["span-1"])
    service._save_signal("s1", "a1", "agent", signal)
    assert repo.saved["evidence"][0]["span_id"] == "span-1"
    assert len(json.dumps(repo.saved["evidence"])) < 5_000
