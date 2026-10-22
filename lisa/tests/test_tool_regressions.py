from benchmark.policy_seed import seed_policy_document

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from lisa.bootstrap import build_services


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
def test_locally_bounded_evidence_is_never_marked_complete(monkeypatch, spans, lisa_test_context):
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

    services = build_services(lisa_test_context())
    seed_policy_document(services.policy_service, "## Rule\napproved: yes\n\nFollow the rule.", "")
    monkeypatch.setattr(services.sessions.traces, "load", get)
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
        seed_policy_document(services.policy_service, f"## Rule\n\n{text}", "")
        return "saved"
    except ValueError:
        return "conflict"


def test_policy_compare_and_swap_serializes_concurrent_writers(lisa_test_context):
    context = lisa_test_context()
    start = Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as workers:
        outcomes = list(
            workers.map(
                lambda text: _write_policy(start, text, context),
                ["First owner change", "Second owner change"],
            )
        )
    assert sorted(outcomes) == ["conflict", "saved"]
