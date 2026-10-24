import json
import threading
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from policy_cases import save_rule, session_detail

from lisa.bootstrap import build_services
from lisa.evaluation.contracts import EvaluationRequest
from lisa.evaluation.scores import evaluation_run_identity
from lisa.evaluation.trajectory import build_trajectory_events, session_timestamp
from lisa.models.activities import ActivityClaim
from lisa.services import evaluations as evaluations_module


@pytest.fixture
def services(lisa_test_context):
    services = build_services(
        lisa_test_context(
            slack_channel="original-channel",
            slack_thread="original-thread",
        )
    )
    save_rule(services)
    return services


def test_evaluation_run_identity_tracks_evidence_and_evaluator_versions():
    trajectory = {
        "events": [{"kind": "user_message", "content": "Check refund eligibility"}],
        "complete": True,
        "limitations": [],
    }
    first = evaluation_run_identity(
        session_id="session",
        policy_hash="policy-v1",
        trajectory=trajectory,
        triage_model="triage-v1",
        judge_model="judge-v1",
    )
    same = evaluation_run_identity(
        session_id="session",
        policy_hash="policy-v1",
        trajectory=trajectory,
        triage_model="triage-v1",
        judge_model="judge-v1",
    )

    assert first == same
    assert evaluation_run_identity(
        session_id="session",
        policy_hash="policy-v1",
        trajectory={**trajectory, "complete": False},
        triage_model="triage-v1",
        judge_model="judge-v1",
    ) != first
    assert evaluation_run_identity(
        session_id="session",
        policy_hash="policy-v1",
        trajectory=trajectory,
        triage_model="triage-v1",
        judge_model="judge-v2",
    ) != first


def test_session_timestamp_uses_current_time_when_spans_have_no_timestamps():
    timestamp = session_timestamp([{}])

    assert timestamp.endswith("Z")
    assert datetime.fromisoformat(timestamp).tzinfo is not None


@pytest.mark.parametrize("raise_error", [False, True])
def test_lease_heartbeat_stops_when_lease_scope_exits(monkeypatch, raise_error):
    real_thread = threading.Thread
    threads = []

    def tracking_thread(*args, **kwargs):
        thread = real_thread(*args, **kwargs)
        threads.append(thread)
        return thread

    monkeypatch.setattr(evaluations_module.threading, "Thread", tracking_thread)
    service = evaluations_module.EvaluationService.__new__(evaluations_module.EvaluationService)
    service.activities = SimpleNamespace(renew_lease=lambda *args, **kwargs: None)
    claim = ActivityClaim(
        activity_id="activity-1",
        owner="lisa-evaluation",
        fencing_token=1,
        lease_expires_at=datetime.now(UTC),
    )

    if raise_error:
        with (
            pytest.raises(RuntimeError, match="operation failed"),
            service._lease_heartbeat("activity-1", "lisa-evaluation", claim),
        ):
            assert len(threads) == 1
            assert threads[0].is_alive()
            raise RuntimeError("operation failed")
    else:
        with service._lease_heartbeat("activity-1", "lisa-evaluation", claim):
            assert len(threads) == 1
            assert threads[0].is_alive()

    assert not threads[0].is_alive()


def test_missing_span_timestamps_build_a_deterministic_trajectory():
    spans = [
        {
            "span_id": "agent-a",
            "attributes": {"sp.input": "First message", "sp.output": "First response"},
        },
        {
            "span_id": "agent-b",
            "attributes": {"sp.input": "Second message", "sp.output": "Second response"},
        },
    ]

    first = build_trajectory_events(spans)
    second = build_trajectory_events(spans)

    assert first.events == second.events
    assert all(event["timestamp"] is None for event in first.events)
    EvaluationRequest.model_validate(
        {
            "run_id": "run-1",
            "policy": {
                "name": "Rule",
                "scope": "workspace",
                "approved": True,
                "criteria": "Check the observed behavior.",
            },
            "trajectory": {
                "trace_id": "trace-1",
                "events": first.events,
                "complete": True,
                "tool_events_captured": False,
                "tool_order_certain": True,
                "message_order_certain": first.message_order_certain,
                "limitations": [],
            },
        }
    )


def test_mixed_missing_timestamps_keep_the_same_order_across_builds():
    spans = [
        {
            "span_id": "timed",
            "start_time": "2026-10-12T03:30:00Z",
            "attributes": {"sp.input": "Observed first"},
        },
        {"span_id": "untimed", "attributes": {"sp.input": "Timing unavailable"}},
    ]

    first = build_trajectory_events(spans)
    second = build_trajectory_events(spans)

    assert first.events == second.events
    assert [event["timestamp"] for event in first.events] == [None, "2026-10-12T03:30:00Z"]
    assert first.message_order_certain is False


@pytest.mark.parametrize(
    "session,expected",
    [
        ({"agent_names": ["other-agent"]}, "skipped_agent_mismatch"),
        ({"spans": []}, "insufficient_evidence"),
    ],
)
def test_unusable_session_does_not_judge_or_store(services, monkeypatch, session, expected):
    monkeypatch.setattr(services.sessions.traces, "load", lambda path: {**session_detail(), **session})
    monkeypatch.setattr(services.evaluations, "judge", lambda r: pytest.fail("Must not judge"))
    result = services.evaluations._evaluate_session("agent", "Rule", "session")
    assert result["status"] == expected
    assert result["stored"] is False


def test_real_refund_trace_becomes_a_compact_judgment_trajectory(services, monkeypatch):
    spans = [
        {
            "span_id": "tool-span",
            "parent_span_id": "generation-span",
            "start_time": "2026-10-12T03:30:09.918549466Z",
            "end_time": "2026-10-12T03:30:09.918615466Z",
            "name": "issue_refund",
            "attributes": {
                "gen_ai.tool.name": "issue_refund",
                "gen_ai.tool.call.id": "call-1",
                "sp.input": '{"ticket_id":"DEMO-42"}',
                "sp.output": '{"ticket_id":"DEMO-42","status":"refunded"}',
                "sp.tool.status": "ok",
            },
        },
        {
            "span_id": "generation-span",
            "parent_span_id": "agent-span",
            "start_time": "2026-10-12T03:30:09.080647174Z",
            "end_time": "2026-10-12T03:30:09.918281508Z",
            "name": "gemini.refund_decision",
            "model_name": "gemini-3.8-flash",
            "attributes": {
                "sp.input": json.dumps(
                    {
                        "messages": [
                            {"role": "system", "content": "long repeated system prompt"},
                            {"role": "user", "content": "Please refund ticket DEMO-42."},
                        ],
                        "tools": [{"name": "issue_refund", "description": "large schema"}],
                    }
                ),
                "sp.output": json.dumps(
                    {
                        "tool_calls": [
                            {
                                "id": "call-1",
                                "name": "issue_refund",
                                "arguments": '{"ticket_id":"DEMO-42"}',
                            }
                        ]
                    }
                ),
                "sp.tool.call.count": 1,
                "gen_ai.usage.input_tokens": 92,
            },
        },
        {
            "span_id": "agent-span",
            "trace_id": "trace",
            "start_time": "2026-10-12T03:30:09.069094258Z",
            "end_time": "2026-10-12T03:30:09.918654341Z",
            "name": "dogfood-refund-agent",
            "attributes": {
                "sp.input": json.dumps(
                    [{"role": "user", "content": "Please refund ticket DEMO-42."}]
                ),
                "sp.output": '{"content":"Your refund for DEMO-42 has been issued."}',
            },
        },
    ]

    events = build_trajectory_events(spans)

    assert [
        (event["kind"], event.get("name"), event.get("content")) for event in events.events
    ] == [
        ("user_message", None, "Please refund ticket DEMO-42."),
        ("tool_call", "issue_refund", None),
        ("tool_result", "issue_refund", None),
        ("assistant_message", None, "Your refund for DEMO-42 has been issued."),
    ]
    assert events.events[1]["payload"] == {"ticket_id": "DEMO-42"}
    assert events.events[2]["payload"] == {"ticket_id": "DEMO-42", "status": "refunded"}
    assert events.events[2]["status"] == "ok"
    assert "long repeated system prompt" not in str(events.events)
    assert "large schema" not in str(events.events)

    request = {}

    def get(path):
        assert path == "session"
        return {
            "session_id": "session",
            "span_count": len(spans),
            "agent_names": ["agent"],
            "scores": [],
            "spans": spans,
        }

    monkeypatch.setattr(services.sessions.traces, "load", get)
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda value: (
            request.update(value) or {"status": "pass", "rationale": "Observed trajectory"}
        ),
    )
    result = services.evaluations._evaluate_session("agent", "Rule", "session")

    assert result["status"] == "pass"
    assert request["trajectory"]["events"] == events.events


def test_bounded_public_session_detail_marks_partial_data_and_keeps_score_history(
    services, monkeypatch
):
    detail = {**session_detail(), "span_count": 501, "scores": [{"score_id": str(i)} for i in range(25)]}
    requested = []
    monkeypatch.setattr(services.sessions.traces, "load", lambda path: requested.append(path) or detail)

    captured = services.evaluations._session_detail("session / one")
    trajectory = services.evaluations._trajectory(captured)

    assert requested == ["session / one"]
    assert len(captured["scores"]) == 25
    assert trajectory["complete"] is False
    assert "span limit was reached" in trajectory["limitations"][0]


def test_model_reported_tool_call_without_a_tool_span_marks_trajectory_incomplete():
    events = build_trajectory_events(
        [
            {
                "span_id": "generation",
                "start_time": "2026-10-12T03:30:09Z",
                "end_time": "2026-10-12T03:30:10Z",
                "attributes": {
                    "sp.output": json.dumps(
                        {
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "name": "check_eligibility",
                                    "arguments": '{"ticket_id":"DEMO-42"}',
                                }
                            ]
                        }
                    )
                },
            }
        ]
    )

    assert events.events[0]["kind"] == "tool_call"
    assert events.events[0]["payload"] == {"ticket_id": "DEMO-42"}
    assert events.complete is False
    assert "no matching tool span" in events.limitations[0]


def test_repeated_confirmations_in_distinct_turns_are_preserved():
    events = build_trajectory_events(
        [
            {
                "span_id": "agent",
                "start_time": "2026-10-12T03:30:09Z",
                "attributes": {
                    "sp.input": json.dumps(
                        [
                            {"role": "user", "content": "yes"},
                            {"role": "assistant", "content": "Proceeding."},
                            {"role": "user", "content": "yes"},
                        ]
                    )
                },
            }
        ]
    )

    assert [event["content"] for event in events.events if event["kind"] == "user_message"] == [
        "yes",
        "yes",
    ]


def test_repeated_confirmation_in_nested_later_turn_is_preserved():
    events = build_trajectory_events(
        [
            {
                "span_id": "agent",
                "start_time": "2026-10-12T03:30:00Z",
                "attributes": {"sp.input": '[{"role":"user","content":"yes"}]'},
            },
            {
                "span_id": "generation",
                "parent_span_id": "agent",
                "start_time": "2026-10-12T03:30:10Z",
                "attributes": {"sp.input": '[{"role":"user","content":"yes"}]'},
            },
        ]
    )

    assert [event["content"] for event in events.events if event["kind"] == "user_message"] == [
        "yes",
        "yes",
    ]


def test_explicitly_mismatched_tool_call_ids_remain_visible_and_incomplete():
    events = build_trajectory_events(
        [
            {
                "span_id": "tool",
                "name": "issue_refund",
                "attributes": {
                    "gen_ai.tool.name": "issue_refund",
                    "gen_ai.tool.call.id": "captured-id",
                    "sp.input": '{"ticket_id":"DEMO-42"}',
                },
            },
            {
                "span_id": "generation",
                "start_time": "2026-10-12T03:30:09Z",
                "end_time": "2026-10-12T03:30:10Z",
                "attributes": {
                    "sp.output": json.dumps(
                        {
                            "tool_calls": [
                                {"id": "reported-id", "name": "issue_refund", "arguments": "{}"}
                            ]
                        }
                    )
                },
            },
        ]
    )

    calls = [event for event in events.events if event["kind"] == "tool_call"]
    assert {event["call_id"] for event in calls} == {"captured-id", "reported-id"}
    assert events.complete is False
    assert "no matching tool span" in events.limitations[0]


def test_list_shaped_assistant_message_preserves_nested_tool_calls():
    events = build_trajectory_events(
        [
            {
                "span_id": "generation",
                "start_time": "2026-10-12T03:30:09Z",
                "end_time": "2026-10-12T03:30:10Z",
                "attributes": {
                    "sp.output": json.dumps(
                        [
                            {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call-1",
                                        "function": {
                                            "name": "issue_refund",
                                            "arguments": '{"ticket_id":"DEMO-42"}',
                                        },
                                    }
                                ],
                            }
                        ]
                    )
                },
            }
        ]
    )

    assert events.events[0]["kind"] == "tool_call"
    assert events.events[0]["name"] == "issue_refund"
    assert events.events[0]["payload"] == {"ticket_id": "DEMO-42"}
    assert events.complete is False


@pytest.mark.parametrize("status", ["pass", "fail", "uncertain", "insufficient_evidence"])
def test_categorical_outcomes_are_stored_with_policy(services, monkeypatch, status):
    monkeypatch.setattr(services.sessions.traces, "load", lambda path: session_detail())
    monkeypatch.setattr(
        services.evaluations, "judge", lambda r: {"status": status, "rationale": "Explanation"}
    )
    result = services.evaluations._evaluate_session("agent", "Rule", "session")
    assert result["status"] == status
    assert result["stored"] is True
    outbox = services.evaluations.activities.get_score_outbox(result["evaluation_id"])
    assert outbox is not None
    assert outbox.score_payload["numeric_value"] is None
    assert outbox.score_payload["config_id"] is None
    assert outbox.score_payload["metadata"]["policy_id"]
    assert outbox.score_payload["metadata"]["criteria"]
    assert bool(services.failures.repository.list_all()) == (status == "fail")


def test_transient_judge_errors_leave_no_score_or_failure(services, monkeypatch):
    monkeypatch.setattr(services.sessions.traces, "load", lambda path: session_detail())
    monkeypatch.setattr(
        services.evaluations, "judge", lambda r: {"status": "error", "rationale": "Unavailable"}
    )
    result = services.evaluations._evaluate_session("agent", "Rule", "session")
    assert result["stored"] is False
    assert services.failures.repository.list_all() == []


def test_notification_uses_stored_failure_without_fetching_remote_score(services, monkeypatch):
    policy = services.evaluations._policy("agent", "Rule")
    services.failures.record(policy, "session", "trace", "Failure", None)
    calls = []
    monkeypatch.setattr(
        services.notifier, "notify", lambda *args: calls.append(args) or {"status": "sent"}
    )
    result = services.evaluations.notify_evaluation_failure("agent", "Rule", "session")
    assert result["status"] == "sent"
    assert calls[0][0]["slack_channel_id"] == "original-channel"
    assert calls[0][0]["slack_thread_ts"] == "original-thread"


def test_notification_uses_failure_receipt_without_loading_trace_bundle(services, monkeypatch):
    policy = services.evaluations._policy("agent", "Rule")
    services.failures.record(policy, "session", "trace-2", "Failure", None)
    monkeypatch.setattr(
        services.sessions.traces,
        "load",
        lambda path: pytest.fail("notifications must use Lisa's durable failure receipt"),
    )
    monkeypatch.setattr(
        services.notifier, "notify", lambda *args: {"status": "sent"}
    )
    assert services.evaluations.notify_evaluation_failure("agent", "Rule", "session")["status"] == "sent"


def test_policy_criteria_edit_permits_a_fresh_evaluation(services, monkeypatch):
    save_rule(services)
    runs = []

    monkeypatch.setattr(services.sessions.traces, "load", lambda _: session_detail())
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda request: (
            runs.append(request)
            or {
                "status": "pass",
                "rationale": "Policy satisfied",
                "references": [],
                "limitations": [],
            }
        ),
    )
    first = services.evaluations._evaluate_session("agent", "Rule", "session")
    save_rule(services, text="Require a confirmation before refunding.")
    result = services.evaluations._evaluate_session("agent", "Rule", "session")

    assert result["status"] == "pass"
    assert len(runs) == 2
    first_outbox = services.evaluations.activities.get_score_outbox(first["evaluation_id"])
    second_outbox = services.evaluations.activities.get_score_outbox(result["evaluation_id"])
    assert first_outbox is not None and second_outbox is not None
    assert first_outbox.score_payload["metadata"]["policy_hash"] != second_outbox.score_payload["metadata"]["policy_hash"]


def test_remote_score_history_does_not_bypass_the_evaluation_ledger(services, monkeypatch):
    policy = services.evaluations._policy("agent", "Rule")
    score = {
        "trace_id": "trace",
        "source": "evaluator",
        "string_value": "fail",
        "metadata": {
            "policy_hash": policy["policy_hash"],
            "slack_channel_id": "historical-channel",
            "slack_thread_ts": "historical-thread",
        },
    }
    monkeypatch.setattr(services.sessions.traces, "load", lambda path: {**session_detail(), "scores": [score]})
    judged = []
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda request: judged.append(request) or {"status": "pass", "rationale": "Checked"},
    )
    result = services.evaluations._evaluate_session("agent", "Rule", "session")

    assert result["status"] == "pass"
    assert len(judged) == 1
