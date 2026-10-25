from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from lisa.services.thelake_sampling import (
    TheLakeApiError,
    TheLakeClient,
    TheLakeConfiguration,
    TheLakeSessionSampler,
    _stratified_sample,
    session_to_otlp,
)


def _summary(session_id: str, agent_name: str, *, errors: int = 0, age_minutes: int = 20):
    end = datetime.now(UTC) - timedelta(minutes=age_minutes)
    start = end - timedelta(minutes=2)
    return {
        "session_id": session_id,
        "agent_name": agent_name,
        "start_time": start.isoformat(),
        "end_time": end.isoformat(),
        "span_count": 1,
        "error_count": errors,
        "trace_count": 1,
    }


def _trace_detail(session_id: str, agent_name: str, *, span_id: str = "1" * 16):
    return {
        "trace": {"trace_id": "a" * 32, "session_id": session_id},
        "spans": [
            {
                "summary": {
                    "trace_id": "a" * 32,
                    "span_id": span_id,
                    "parent_span_id": None,
                    "session_id": session_id,
                    "name": "agent turn",
                    "span_type": "agent",
                    "start_time": "2026-10-25T10:00:00Z",
                    "end_time": "2026-10-25T10:00:01Z",
                    "status_code": "OK",
                    "agent_name": agent_name,
                },
                "attributes": {"sp.session.id": session_id},
                "events": [{"name": "message", "timestamp": 1792922400000000000}],
            }
        ],
        "next_span_cursor": None,
    }


class FakeLake:
    def __init__(self, sessions, details):
        self.sessions = sessions
        self.details = details
        self.searches = []
        self.fetches = []

    def search_sessions(self, *, from_time, to_time, cursor):
        self.searches.append((from_time, to_time, cursor))
        return {"items": self.sessions, "next_cursor": None}

    def list_session_spans(self, session_id, *, from_time, to_time):
        return [
            {"trace_id": "a" * 32, "span_id": "1" * 16, "start_time": from_time}
        ]

    def get_trace(self, trace_id, *, session_id, from_time, to_time):
        self.fetches.append((trace_id, session_id))
        return self.details[session_id]

    def get_complete_session(self, summary):
        session_id = summary["session_id"]
        spans = self.list_session_spans(
            session_id, from_time=summary["start_time"], to_time=summary["end_time"]
        )
        traces = [
            self.get_trace(
                trace_id,
                session_id=session_id,
                from_time=summary["start_time"],
                to_time=summary["end_time"],
            )
            for trace_id in {item["trace_id"] for item in spans}
        ]
        return {"traces": traces, "span_count": sum(len(item["spans"]) for item in traces)}


class FakeIntake:
    def __init__(self, existing=()):
        self.existing = set(existing)
        self.bundles = []

    def has_session(self, session_id):
        return session_id in self.existing

    def accept(self, bundle):
        self.bundles.append(bundle)
        attrs = bundle["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"]
        session_id = next(item["value"]["stringValue"] for item in attrs if item["key"] == "sp.session.id")
        self.existing.add(session_id)
        return {"session_id": session_id, "status": "queued"}


def test_thelake_configuration_is_optional_but_rejects_partial_settings():
    assert TheLakeConfiguration.from_values("", "") is None
    assert TheLakeConfiguration.from_values("https://lake.example/", "secret") == (
        TheLakeConfiguration("https://lake.example", "secret")
    )
    with pytest.raises(ValueError, match="configured together"):
        TheLakeConfiguration.from_values("https://lake.example", "")


def test_thelake_client_reads_every_page_and_checks_complete_session_counts():
    requests = []

    def respond(request):
        requests.append(request)
        if request.url.path == "/v1/spans/search":
            if json.loads(request.content).get("cursor"):
                return httpx.Response(200, json={"items": [{"trace_id": "b" * 32}], "next_cursor": None})
            return httpx.Response(200, json={"items": [{"trace_id": "a" * 32}], "next_cursor": "page-2"})
        trace_id = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(
            200,
            json={"trace": {"trace_id": trace_id}, "spans": [{"summary": {"span_id": trace_id[:16]}}], "next_span_cursor": None},
        )

    client = httpx.Client(
        base_url="https://lake.example",
        headers={"Authorization": "Bearer sensitive-token"},
        transport=httpx.MockTransport(respond),
    )
    lake = TheLakeClient(TheLakeConfiguration("https://lake.example", "sensitive-token"), client)
    summary = {
        "session_id": "session-1",
        "start_time": "2026-10-25T10:00:00Z",
        "end_time": "2026-10-25T10:01:00Z",
        "span_count": 2,
        "trace_count": 2,
    }

    complete = lake.get_complete_session(summary)

    assert complete["span_count"] == 2
    assert len([request for request in requests if request.url.path == "/v1/spans/search"]) == 2
    assert all(request.headers["Authorization"] == "Bearer sensitive-token" for request in requests)
    lake.close()


def test_thelake_client_hides_server_response_and_auth_token_on_failure():
    client = httpx.Client(
        base_url="https://lake.example",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(500, text="sensitive-token backend detail")
        ),
    )
    lake = TheLakeClient(TheLakeConfiguration("https://lake.example", "sensitive-token"), client)

    with pytest.raises(TheLakeApiError) as error:
        lake.search_sessions(from_time="2026-10-25T10:00:00Z", to_time="2026-10-25T11:00:00Z", cursor=None)

    assert "sensitive-token" not in str(error.value)
    assert "backend detail" not in str(error.value)
    lake.close()


def test_thelake_client_rejects_an_oversized_final_page():
    client = httpx.Client(
        base_url="https://lake.example",
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={"items": [{"trace_id": "a" * 32}] * 5001, "next_cursor": None},
            )
        ),
    )
    lake = TheLakeClient(TheLakeConfiguration("https://lake.example", "token"), client)

    with pytest.raises(ValueError, match="bounded sampling limit"):
        lake.get_complete_session(
            {
                "session_id": "session-1",
                "start_time": "2026-10-25T10:00:00Z",
                "end_time": "2026-10-25T10:01:00Z",
                "span_count": 5001,
                "trace_count": 1,
            }
        )
    lake.close()


def test_thelake_client_enforces_span_limit_across_all_traces():
    def respond(request):
        if request.url.path == "/v1/spans/search":
            return httpx.Response(
                200,
                json={"items": [{"trace_id": "a" * 32}, {"trace_id": "b" * 32}], "next_cursor": None},
            )
        spans = 3000 if request.url.path.endswith("a" * 32) else 2001
        return httpx.Response(
            200,
            json={"trace": {}, "spans": [{}] * spans, "next_span_cursor": None},
        )

    client = httpx.Client(
        base_url="https://lake.example", transport=httpx.MockTransport(respond)
    )
    lake = TheLakeClient(TheLakeConfiguration("https://lake.example", "token"), client)

    with pytest.raises(ValueError, match="bounded sampling limit"):
        lake.get_complete_session(
            {
                "session_id": "session-1",
                "start_time": "2026-10-25T10:00:00Z",
                "end_time": "2026-10-25T10:01:00Z",
                "span_count": 2,
                "trace_count": 2,
            }
        )
    lake.close()


def test_session_to_otlp_preserves_span_identity_attributes_events_and_agent():
    bundle = session_to_otlp("session-1", "agent-1", _trace_detail("session-1", "agent-1"))

    resource_span = bundle["resourceSpans"][0]
    assert resource_span["resource"]["attributes"] == [
        {"key": "sp.agent.name", "value": {"stringValue": "agent-1"}},
        {"key": "service.name", "value": {"stringValue": "agent-1"}},
    ]
    span = resource_span["scopeSpans"][0]["spans"][0]
    assert span["traceId"] == "a" * 32
    assert span["spanId"] == "1" * 16
    assert {item["key"] for item in span["attributes"]} == {
        "sp.session.id",
        "sp.agent.name",
    }
    assert span["events"][0]["name"] == "message"


def test_sampler_filters_quiet_sessions_stratifies_and_deduplicates():
    summaries = [
        _summary("clean-a", "agent-a"),
        _summary("error-a", "agent-a", errors=1),
        _summary("clean-b", "agent-b"),
        _summary("active", "agent-b", age_minutes=2),
    ]
    details = {
        summary["session_id"]: _trace_detail(summary["session_id"], summary["agent_name"])
        for summary in summaries
    }
    lake = FakeLake(summaries, details)
    intake = FakeIntake(existing={"clean-a"})
    sampler = TheLakeSessionSampler(lake, intake, now=lambda: datetime.now(UTC))

    result = sampler.sample_recent_sessions(lookback_hours=24, max_sessions=3)

    assert result["eligible_count"] == 3
    assert result["already_stored_count"] == 1
    assert result["queued_count"] == 2
    assert {bundle["resourceSpans"][0]["resource"]["attributes"][0]["value"]["stringValue"]
            for bundle in intake.bundles} == {"agent-a", "agent-b"}
    assert all(session_id != "active" for _, session_id in lake.fetches)


def test_sampler_rejects_incomplete_or_mismatched_trace_evidence():
    summary = _summary("session-1", "agent-1")
    detail = _trace_detail("session-1", "agent-1")
    detail["spans"] = []
    sampler = TheLakeSessionSampler(
        FakeLake([summary], {"session-1": detail}), FakeIntake()
    )

    result = sampler.sample_recent_sessions(lookback_hours=24, max_sessions=1)

    assert result["queued_count"] == 0
    assert result["incomplete_count"] == 1


def test_sampler_honors_per_run_cap_and_returns_only_safe_counts():
    summaries = [_summary(f"session-{i}", f"agent-{i % 2}") for i in range(8)]
    details = {
        summary["session_id"]: _trace_detail(summary["session_id"], summary["agent_name"])
        for summary in summaries
    }
    sampler = TheLakeSessionSampler(FakeLake(summaries, details), FakeIntake())

    result = sampler.sample_recent_sessions(lookback_hours=24, max_sessions=2)

    assert result["queued_count"] == 2
    assert result["selected_count"] == 2
    assert "trace" not in str(result).lower()


def test_sampler_does_not_backfill_already_sampled_sessions_in_same_hour():
    now = datetime.now(UTC)
    summaries = [_summary(f"session-{i}", f"agent-{i % 2}") for i in range(8)]
    details = {
        summary["session_id"]: _trace_detail(summary["session_id"], summary["agent_name"])
        for summary in summaries
    }
    intake = FakeIntake()
    sampler = TheLakeSessionSampler(
        FakeLake(summaries, details), intake, now=lambda: now
    )

    first = sampler.sample_recent_sessions(lookback_hours=24, max_sessions=2)
    second = sampler.sample_recent_sessions(lookback_hours=24, max_sessions=2)

    assert first["queued_count"] == 2
    assert first["selected_count"] == 2
    assert second["eligible_count"] == 8
    assert second["selected_count"] == 2
    assert second["already_stored_count"] == 2
    assert second["queued_count"] == 0
    assert len(intake.bundles) == 2


def test_stratified_cohort_rotates_group_priority_between_hours():
    summaries = [
        _summary(f"group-{index}", f"agent-{index}", errors=index % 2)
        for index in range(8)
    ]

    first = _stratified_sample(summaries, 5, sample_key="hour-1")
    second = _stratified_sample(summaries, 5, sample_key="hour-2")
    first_groups = {(item["agent_name"], int(item["error_count"]) > 0) for item in first}
    second_groups = {(item["agent_name"], int(item["error_count"]) > 0) for item in second}

    assert len(first_groups) == 5
    assert len(second_groups) == 5
    assert len(first_groups | second_groups) > 5
    assert {item["session_id"] for item in first} != {item["session_id"] for item in second}


def test_sampler_continues_when_one_local_manifest_check_fails():
    summaries = [_summary("candidate-1", "agent-a"), _summary("candidate-2", "agent-b")]
    details = {
        summary["session_id"]: _trace_detail(summary["session_id"], summary["agent_name"])
        for summary in summaries
    }

    class FailingIntake(FakeIntake):
        def __init__(self):
            super().__init__()
            self.first = True

        def has_session(self, session_id):
            if self.first:
                self.first = False
                raise ValueError("corrupt receipt")
            return super().has_session(session_id)

    intake = FailingIntake()
    sampler = TheLakeSessionSampler(FakeLake(summaries, details), intake)

    result = sampler.sample_recent_sessions(lookback_hours=24, max_sessions=2)

    assert result["failed_count"] == 1
    assert result["queued_count"] == 1
