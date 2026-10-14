import json

import pytest
from policy_cases import evidence, save_rule

from thelake_lisa.context import WorkspaceContext
from thelake_lisa.evaluation.evidence import normalize_events
from thelake_lisa.services import build_services


@pytest.fixture
def services(tmp_path):
    services = build_services(
        WorkspaceContext(
            workspace="11111111-1111-4111-8111-111111111111",
            data_dir=tmp_path,
            confirmed_policy_title="Rule",
            slack_channel="original-channel",
            slack_thread="original-thread",
        )
    )
    save_rule(services)
    return services


@pytest.mark.parametrize(
    "session,expected",
    [
        ({"agent_names": ["other-agent"]}, "skipped_agent_mismatch"),
        ({"spans": []}, "insufficient_evidence"),
    ],
)
def test_unusable_session_does_not_judge_or_store(services, monkeypatch, session, expected):
    monkeypatch.setattr(services.client, "get", lambda path: {**evidence(), **session})
    monkeypatch.setattr(services.evaluations, "judge", lambda r: pytest.fail("Must not judge"))
    result = services.evaluations.evaluate_session("agent", "Rule", "session")
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
            "model_name": "gemini-2.5-flash",
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
            "name": "quickstart-refund-agent",
            "attributes": {
                "sp.input": json.dumps([{"role": "user", "content": "Please refund ticket DEMO-42."}]),
                "sp.output": '{"content":"Your refund for DEMO-42 has been issued."}',
            },
        },
    ]

    normalized = normalize_events(spans)

    assert [(event["kind"], event.get("name"), event.get("content")) for event in normalized.events] == [
        ("user_message", None, "Please refund ticket DEMO-42."),
        ("tool_call", "issue_refund", None),
        ("tool_result", "issue_refund", None),
        ("assistant_message", None, "Your refund for DEMO-42 has been issued."),
    ]
    assert normalized.events[1]["payload"] == {"ticket_id": "DEMO-42"}
    assert normalized.events[2]["payload"] == {"ticket_id": "DEMO-42", "status": "refunded"}
    assert normalized.events[2]["status"] == "ok"
    assert "long repeated system prompt" not in str(normalized.events)
    assert "large schema" not in str(normalized.events)

    request = {}

    def get(path):
        assert path == "/v1/sessions/session?limit=500"
        return {
            "session_id": "session",
            "span_count": len(spans),
            "agent_names": ["agent"],
            "scores": [],
            "spans": spans,
        }

    monkeypatch.setattr(services.client, "get", get)
    monkeypatch.setattr(services.client, "post", lambda *args, **kwargs: ({}, 201))
    monkeypatch.setattr(services.evaluations, "judge", lambda value: request.update(value) or {"status": "pass", "rationale": "Observed trajectory"})
    result = services.evaluations.evaluate_session("agent", "Rule", "session")

    assert result["status"] == "pass"
    assert request["evidence"]["events"] == normalized.events


def test_bounded_public_session_detail_marks_partial_data_and_keeps_score_history(
    services, monkeypatch
):
    detail = {**evidence(), "span_count": 501, "scores": [{"score_id": str(i)} for i in range(25)]}
    requested = []
    monkeypatch.setattr(services.client, "get", lambda path: requested.append(path) or detail)

    captured = services.evaluations._session_evidence("session / one")
    trajectory = services.evaluations._trajectory(captured)

    assert requested == ["/v1/sessions/session%20%2F%20one?limit=500"]
    assert len(captured["scores"]) == 25
    assert trajectory["complete"] is False
    assert "span limit was reached" in trajectory["limitations"][0]


def test_model_reported_tool_call_without_a_tool_span_marks_trajectory_incomplete():
    normalized = normalize_events(
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

    assert normalized.events[0]["kind"] == "tool_call"
    assert normalized.events[0]["payload"] == {"ticket_id": "DEMO-42"}
    assert normalized.complete is False
    assert "no matching tool span" in normalized.limitations[0]


def test_repeated_confirmations_in_distinct_turns_are_preserved():
    normalized = normalize_events(
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

    assert [event["content"] for event in normalized.events if event["kind"] == "user_message"] == [
        "yes",
        "yes",
    ]


def test_repeated_confirmation_in_nested_later_turn_is_preserved():
    normalized = normalize_events(
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

    assert [event["content"] for event in normalized.events if event["kind"] == "user_message"] == [
        "yes",
        "yes",
    ]


def test_explicitly_mismatched_tool_call_ids_remain_visible_and_incomplete():
    normalized = normalize_events(
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
                        {"tool_calls": [{"id": "reported-id", "name": "issue_refund", "arguments": "{}"}]}
                    )
                },
            },
        ]
    )

    calls = [event for event in normalized.events if event["kind"] == "tool_call"]
    assert {event["call_id"] for event in calls} == {"captured-id", "reported-id"}
    assert normalized.complete is False
    assert "no matching tool span" in normalized.limitations[0]


def test_list_shaped_assistant_message_preserves_nested_tool_calls():
    normalized = normalize_events(
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

    assert normalized.events[0]["kind"] == "tool_call"
    assert normalized.events[0]["name"] == "issue_refund"
    assert normalized.events[0]["payload"] == {"ticket_id": "DEMO-42"}
    assert normalized.complete is False


@pytest.mark.parametrize("status", ["pass", "fail", "uncertain", "insufficient_evidence"])
def test_categorical_outcomes_are_stored_with_policy_snapshot(services, monkeypatch, status):
    writes = []

    def post(path, body, *, include_status=False):
        writes.append(body)
        return ({}, 201)

    monkeypatch.setattr(services.client, "get", lambda path: evidence())
    monkeypatch.setattr(services.client, "post", post)
    monkeypatch.setattr(
        services.evaluations, "judge", lambda r: {"status": status, "rationale": "Explanation"}
    )
    result = services.evaluations.evaluate_session("agent", "Rule", "session")
    assert result["status"] == status
    assert result["stored"] is True
    assert writes[0]["numeric_value"] is None
    assert writes[0]["config_id"] is None
    assert "approved: yes" in writes[0]["metadata"]["policy_snapshot"]
    assert bool(services.failures._read_pending_failures()) == (status == "fail")


def test_transient_judge_errors_leave_no_score_or_failure(services, monkeypatch):
    monkeypatch.setattr(services.client, "get", lambda path: evidence())
    monkeypatch.setattr(services.client, "post", lambda *args, **kwargs: pytest.fail("No write"))
    monkeypatch.setattr(
        services.evaluations, "judge", lambda r: {"status": "error", "rationale": "Unavailable"}
    )
    result = services.evaluations.evaluate_session("agent", "Rule", "session")
    assert result["stored"] is False
    assert services.failures._read_pending_failures() == []


def test_notification_requires_stored_failure_and_keeps_captured_destination(services, monkeypatch):
    policy = services.evaluations._policy_snapshot("agent", "Rule")
    services.failures.record(policy, "session", "trace", "Failure", None)
    calls = []
    session = evidence()
    monkeypatch.setattr(services.client, "get", lambda path: session)
    monkeypatch.setattr(
        services.notifier, "notify", lambda *args: calls.append(args) or {"status": "sent"}
    )
    assert (
        services.evaluations.notify_evaluation_failure("agent", "Rule", "session")["status"]
        == "not_a_stored_failure"
    )
    assert calls == []
    session["scores"] = [
        {
            "trace_id": "trace",
            "source": "evaluator",
            "string_value": "fail",
            "metadata": {"policy_hash": policy["policy_hash"]},
        }
    ]
    services.evaluations.notify_evaluation_failure("agent", "Rule", "session")
    assert calls[0][0]["slack_channel_id"] == "original-channel"
    assert calls[0][0]["slack_thread_ts"] == "original-thread"


def test_queued_trace_is_verified_in_multi_trace_session(services, monkeypatch):
    policy = services.evaluations._policy_snapshot("agent", "Rule")
    services.failures.record(policy, "session", "trace-2", "Failure", None)
    monkeypatch.setattr(services.client, "get", lambda path: evidence())
    assert (
        services.evaluations.notify_evaluation_failure("agent", "Rule", "session")["status"]
        == "trace_not_found"
    )


def test_legacy_failures_recover_without_loading_a_registry(services, monkeypatch):
    old = {"evaluator_id": "old", "version": 2, "name": "Old rule", "target_agent_name": "agent"}
    score = {
        "score_id": "old-score",
        "config_id": "evaluator:old:v2",
        "source": "evaluator",
        "trace_id": "trace",
        "string_value": "fail",
    }
    services.failures.record(old, "session", "trace", "Legacy failure", None, score_payload=score)
    gets = []
    monkeypatch.setattr(
        services.client,
        "get",
        lambda path: gets.append(path) or {**evidence(), "scores": [score]},
    )
    assert services.failures.list_pending_evaluation_failures()[0]["name"] == "Old rule"
    assert gets == ["/v1/sessions/session?limit=500"]


def test_reused_failure_preserves_the_score_destination_after_a_later_turn(services, monkeypatch):
    policy = services.evaluations._policy_snapshot("agent", "Rule")
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
    monkeypatch.setattr(services.client, "get", lambda path: {**evidence(), "scores": [score]})
    services.evaluations.evaluate_session("agent", "Rule", "session")
    queued = services.failures._read_pending_failures()[0]
    assert queued["slack_channel_id"] == "historical-channel"
    assert queued["slack_thread_ts"] == "historical-thread"
