import multiprocessing
import time

import pytest

from thelake_lisa.context import WorkspaceContext
from thelake_lisa.services import build_services


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
    services.policies.update_policy("## Rule\napproved: yes\n\nFollow the rule.", "")
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
    services.evaluations.evaluate_session("agent", "Rule", "session")
    assert requests[0]["evidence"]["complete"] is False
    assert requests[0]["evidence"]["limitations"]


def _write_policy(start, ready, results, text, context):
    services = build_services(context)
    original = services.policies.check_changes

    def pause(*args):
        time.sleep(0.2)
        return original(*args)

    services.policies.check_changes = pause
    ready.put(True)
    start.wait()
    try:
        services.policies.update_policy(f"## Rule\n\n{text}", "")
        results.put("saved")
    except ValueError:
        results.put("conflict")


def test_policy_compare_and_swap_serializes_separate_processes():
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    ready = context.Queue()
    results = context.Queue()
    workers = [
        context.Process(
            target=_write_policy,
            args=(start, ready, results, text, WorkspaceContext.from_environment()),
        )
        for text in ["First owner change", "Second owner change"]
    ]
    for worker in workers:
        worker.start()
    try:
        for _ in workers:
            ready.get(timeout=10)
        start.set()
        outcomes = [results.get(timeout=10) for _ in workers]
        assert sorted(outcomes) == ["conflict", "saved"]
    finally:
        for worker in workers:
            worker.join(timeout=5)
            if worker.is_alive():
                worker.terminate()
                worker.join()
