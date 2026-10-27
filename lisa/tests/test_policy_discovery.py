import json
from datetime import UTC, datetime
from types import SimpleNamespace
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


def test_gap_analysis_prompt_has_lisa_speak_to_the_owner(monkeypatch):
    from lisa.evaluation import policy_discovery

    captured = {}
    monkeypatch.setattr(
        policy_discovery,
        "format_trajectory_for_llm",
        lambda _events: SimpleNamespace(
            text="[event]", span_id_map={"[event]": "span-1"}
        ),
    )

    def generate(_model, **kwargs):
        captured.update(kwargs)
        return PolicyGapSignal.model_validate({
            "detected": True,
            "summary": "I noticed a concerning action in this run.",
            "span_ids": ["[event]"],
        })

    monkeypatch.setattr(policy_discovery, "_generate", generate)
    result = policy_discovery.analyze_policy_gap({"events": [{"span_id": "span-1"}]})

    assert result.summary == "I noticed a concerning action in this run."
    instruction = captured["system_instruction"].casefold()
    assert "speaking directly to the nontechnical owner" in instruction
    assert "do not use trace or span terminology" in instruction
    assert "only sampled run" in instruction


def test_suggestion_synthesis_prompt_has_lisa_speak_to_the_owner(monkeypatch):
    from lisa.evaluation import policy_discovery

    captured = {}

    def generate(_model, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(suggestions=[])

    monkeypatch.setattr(policy_discovery, "_generate", generate)
    signals = [
        {"id": f"signal-{index}", "session_id": f"run-{index}"}
        for index in range(3)
    ]

    assert policy_discovery.synthesize_policy_suggestions(signals, [], []) == []
    instruction = captured["system_instruction"].casefold()
    assert "write rationale as lisa speaking directly to the nontechnical owner" in instruction
    assert "do not use trace or span terminology" in instruction
    assert "three distinct sampled session_ids" in instruction


def test_suggestion_must_cite_spans_from_the_selected_session():
    draft = PolicySuggestionDraft.model_validate({
        "name": "Confirm consequential actions",
        "criteria": "Ask for confirmation before a consequential action.",
        "scope": "workspace",
        "target_agent_name": "",
        "rationale": "The same uncovered behavior occurred in multiple sessions.",
        "signal_ids": ["signal-1", "signal-2", "signal-3"],
    })
    assert draft.scope == "workspace"
    with pytest.raises(ValueError):
        PolicyDiscoveryService.validate_references(
            ["missing-span"], {"span-1"}
        )
    PolicyDiscoveryService.validate_references(["span-1"], {"span-1"})


def test_provider_synthesis_skips_fewer_than_three_distinct_error_samples(monkeypatch):
    from lisa.evaluation import policy_discovery

    calls = []
    monkeypatch.setattr(
        policy_discovery, "_generate",
        lambda *_args, **_kwargs: calls.append(True),
    )
    signals = [
        {"id": "signal-1", "session_id": "run-1"},
        {"id": "signal-2", "session_id": "run-2"},
    ]

    assert policy_discovery.synthesize_policy_suggestions(signals, [], []) == []
    assert calls == []


def test_policy_suggestion_requires_three_distinct_error_samples():
    payload = {
        "name": "Confirm destructive actions",
        "criteria": "Ask for confirmation before destructive actions.",
        "scope": "workspace",
        "target_agent_name": "",
        "rationale": "The same clear error appeared in three sampled runs.",
        "signal_ids": ["signal-1", "signal-2"],
    }
    with pytest.raises(ValueError, match="at least 3"):
        PolicySuggestionDraft.model_validate(payload)


def test_synthesis_does_not_start_before_three_error_samples():
    class Repository:
        def claim_daily_synthesis(self):
            return True

        def pending_signals(self, limit):
            return [
                {"id": "signal-1", "session_id": "run-1"},
                {"id": "signal-2", "session_id": "run-2"},
            ][:limit]

        def reviewed_decisions(self, limit):
            return []

        def finish_daily_synthesis(self, _signal_ids):
            pass

        def release_daily_synthesis(self):
            pass

        def create_suggestions(self, **_kwargs):
            return []

    synthesis_calls = []
    service = PolicyDiscoveryService(
        Repository(), policy_service=SimpleNamespace(list_policy_records=list), sessions=None,
        synthesizer=lambda *_: synthesis_calls.append(True) or [],
    )

    assert service.synthesize_daily() == []
    assert synthesis_calls == []


def test_three_error_signals_from_only_two_sessions_do_not_start_synthesis():
    class Repository:
        def claim_daily_synthesis(self):
            return True

        def pending_signals(self, limit):
            return [
                {"id": "signal-1", "session_id": "run-1"},
                {"id": "signal-2", "session_id": "run-1"},
                {"id": "signal-3", "session_id": "run-2"},
            ][:limit]

        def reviewed_decisions(self, limit):
            return []

        def finish_daily_synthesis(self, _signal_ids):
            pass

        def release_daily_synthesis(self):
            pass

        def create_suggestions(self, **_kwargs):
            return []

    synthesis_calls = []
    service = PolicyDiscoveryService(
        Repository(), policy_service=SimpleNamespace(list_policy_records=list), sessions=None,
        synthesizer=lambda *_: synthesis_calls.append(True) or [],
    )

    assert service.synthesize_daily() == []
    assert synthesis_calls == []


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
        signals=[
            {
                "id": signal_id if index == 0 else uuid4(),
                "session_id": f"benchmark-session-{index}",
                "created_at": created_at,
                "evidence": [],
            }
            for index in range(3)
        ],
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


def test_policy_discovery_signals_require_a_reserved_sample(
    lisa_test_database, monkeypatch, tmp_path
):
    workspace = str(uuid4())
    database = lisa_test_database(workspace)
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace)
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    services = build_services(WorkspaceContext.from_environment())
    activity = services.activity_store.create_activity(
        title="Unsampled discovery evidence", source="session_push", kind="session_evaluation"
    )

    with pytest.raises(ValueError, match="reserved sample"):
        services.policy_discovery.repository.save_signal(
            session_id="unsampled-run", activity_id=activity.id, agent_name="agent",
            summary="A concrete uncovered behavior.", span_ids=["span-1"],
            evidence=[{"span_id": "span-1"}],
        )


def test_first_sampled_error_reacts_in_chat_before_a_policy_is_recommended(
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
        title="Sampled account run", source="session_push", kind="session_evaluation"
    )
    trajectory = {
        "events": [
            {"span_id": "request", "kind": "user_message", "content": "Help me with my account."},
            {"span_id": "delete", "kind": "tool_call", "name": "delete_account", "content": "Delete account 42"},
            {"span_id": "result", "kind": "tool_result", "content": 'api_key="observation-secret" {"status":"deleted"}'},
        ]
    }
    monkeypatch.setattr(services.sessions, "get_trajectory", lambda _session_id: trajectory)
    sample_time = datetime(2026, 10, 25, 10, 15, tzinfo=UTC)
    discovery.clock = lambda: sample_time

    assert discovery.reserve_sample(
        session_id="sampled-run-1", agent_name="account-agent", activity_id=activity.id,
    )
    assert discovery.record_judge_signal(
        session_id="sampled-run-1", activity_id=activity.id, agent_name="account-agent",
        signal=PolicyGapSignal(
            detected=True,
            summary="I noticed the agent deleted the account without asking for confirmation.",
            span_ids=["request", "delete", "result"],
        ).model_dump(mode="json"),
    )
    assert discovery.record_judge_signal(
        session_id="sampled-run-1", activity_id=activity.id, agent_name="account-agent",
        signal=PolicyGapSignal(
            detected=True,
            summary="I noticed the agent deleted the account without asking for confirmation.",
            span_ids=["request", "delete", "result"],
        ).model_dump(mode="json"),
    )

    updates = services.proactive_updates.list(limit=10)
    assert len(updates) == 1
    assert updates[0]["source_type"] == "qa_observation"
    assert updates[0]["messages"] == [
        "I noticed the agent deleted the account without asking for confirmation."
    ]
    observation = discovery.get_observation(updates[0]["source_id"])
    assert observation["session_id"] == "sampled-run-1"
    assert observation["activity_id"] == str(activity.id)
    assert observation["span_ids"] == ["request", "delete", "result"]
    assert "observation-secret" not in observation["evidence"][2]["content"]
    assert "[REDACTED]" in observation["evidence"][2]["content"]
    assert len(discovery.repository.pending_signals(limit=10)) == 1
    assert discovery.synthesize_daily() == []
    assert discovery.repository.list_suggestions(status="pending", limit=10) == []


def test_successful_empty_consolidation_marks_the_entire_batch_reviewed(
    lisa_test_database, monkeypatch, tmp_path
):
    workspace = str(uuid4())
    database = lisa_test_database(workspace)
    monkeypatch.setenv("LISA_WORKSPACE_ID", workspace)
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_POLICY_DISCOVERY_SAMPLES_PER_HOUR", "100")
    services = build_services(WorkspaceContext.from_environment())
    activity = services.activity_store.create_activity(
        title="Discovery evidence", source="session_push", kind="session_evaluation"
    )
    discovery = services.policy_discovery
    for index in range(21):
        session_id = f"batch-session-{index}"
        assert discovery.reserve_sample(
            session_id=session_id, agent_name="agent", activity_id=activity.id,
        )
        discovery.repository.save_signal(
            session_id=session_id, activity_id=activity.id,
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
    discovery = services.policy_discovery
    for index in range(3):
        session_id = f"retry-next-day-{index}"
        assert discovery.reserve_sample(
            session_id=session_id, agent_name="agent", activity_id=activity.id,
        )
        discovery.repository.save_signal(
            session_id=session_id, activity_id=activity.id, agent_name="agent",
            summary="A concrete uncovered behavior.", span_ids=[f"span-{index}"],
            evidence=[{"span_id": f"span-{index}"}],
        )

    def fail_synthesis(*_args):
        raise RuntimeError("provider unavailable")

    services.policy_discovery.synthesizer = fail_synthesis
    with pytest.raises(RuntimeError, match="provider unavailable"):
        services.policy_discovery.synthesize_daily()

    assert len(services.policy_discovery.repository.pending_signals(limit=10)) == 3
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
    signal_ids = []
    for index in range(3):
        session_id = f"session-evidence-{index}"
        assert services.policy_discovery.reserve_sample(
            session_id=session_id, agent_name="agent", activity_id=activity.id,
        )
        signal_ids.append(services.policy_discovery.repository.save_signal(
            session_id=session_id, activity_id=activity.id, agent_name="agent",
            summary="The agent took an important action without adequate confirmation.",
            span_ids=[f"span-user-{index}", f"span-tool-{index}"],
            evidence=[
                {"span_id": f"span-user-{index}", "kind": "user_message", "content": "Do this"},
                {"span_id": f"span-tool-{index}", "kind": "tool_call", "name": "change_state"},
            ],
        ))
    draft = PolicySuggestionDraft.model_validate({
        "name": "Confirm consequential actions",
        "criteria": "Ask the user before taking a consequential action.",
        "scope": "agent", "target_agent_name": "agent",
        "rationale": "The same gap appeared in three sampled runs.", "signal_ids": signal_ids,
    })
    signal_rows = services.policy_discovery.repository.pending_signals()
    saved = services.policy_discovery.repository.create_suggestions(
        drafts=[draft], signal_rows=signal_rows,
        overlaps_by_draft=[[]],
    )[0]
    suggestion_update = next(
        update for update in services.proactive_updates.list(limit=10)
        if update["source_type"] == "policy_suggestion"
    )
    assert suggestion_update["messages"] == [draft.rationale]
    duplicate = services.policy_discovery.repository.create_suggestions(
        drafts=[draft], signal_rows=signal_rows,
        overlaps_by_draft=[[]],
    )[0]
    assert duplicate["id"] == saved["id"]
    assert saved["signal_count"] == 3
    assert saved["evidence"][0]["evidence"][1]["span_id"] == "span-tool-0"
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
