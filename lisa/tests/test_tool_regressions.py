from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext


@pytest.fixture(autouse=True)
def workspace(monkeypatch, tmp_path):
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_TURN_SOURCE", "web")
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Rule")


def span(**values):
    return {
        "span_id": "span",
        "trace_id": "trace",
        "start_time": "2026-10-09T12:00:00Z",
        "input": "request",
        **values,
    }


@pytest.mark.parametrize(
    "spans",
    [
        [span(input="x" * 20_001)],
        [span(span_id=f"span-{i}") for i in range(1001)],
    ],
)
def test_locally_bounded_evidence_is_never_marked_complete(monkeypatch, spans):
    requests = []

    def get(path):
        return {
            "agent_names": ["agent"],
            "span_count": len(spans),
            "spans": spans,
            "scores": [],
        }

    def post(path, body, **kwargs):
        return ({}, 201)

    services = build_services(WorkspaceContext.from_environment())
    services.policy_service.update_policy("## Rule\napproved: yes\n\nFollow the rule.", "")
    monkeypatch.setattr(services.client, "get", get)
    monkeypatch.setattr(services.client, "post", post)
    monkeypatch.setattr(
        services.evaluations,
        "judge",
        lambda request: (
            requests.append(request)
            or {
                "status": "insufficient_evidence",
                "rationale": "Incomplete",
                "limitations": [],
            }
        ),
    )
    services.evaluations._evaluate_session("agent", "Rule", "session")
    assert requests[0]["trajectory"]["complete"] is False
    assert requests[0]["trajectory"]["limitations"]


def _write_policy(start, text, context):
    services = build_services(context)
    start.wait()
    try:
        services.policy_service.update_policy(f"## Rule\n\n{text}", "")
        return "saved"
    except ValueError:
        return "conflict"


def test_policy_compare_and_swap_serializes_concurrent_writers():
    context = WorkspaceContext.from_environment()
    start = Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as workers:
        outcomes = list(
            workers.map(
                lambda text: _write_policy(start, text, context),
                ["First owner change", "Second owner change"],
            )
        )
    assert sorted(outcomes) == ["conflict", "saved"]
