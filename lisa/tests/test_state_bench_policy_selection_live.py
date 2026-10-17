"""Opt-in real-model gold calibration, reported independently for every case."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import replace

import pytest
from fastmcp import Client
from state_bench_cases import load_gold_cases, load_gold_rules

from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.evaluation.contracts import EvaluationRequest
from lisa.evaluation.judge import run_evaluation
from lisa.mcp_app import create_server

CASES = load_gold_cases()
LIVE_ENABLED = os.getenv("LISA_RUN_STATE_BENCH_LIVE") == "1"
HAS_OPENAI_KEY = bool(os.getenv("OPENAI_API_KEY"))
HAS_GEMINI_KEY = bool(os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY"))


def assert_gold_verdict(case, verdict):
    assert verdict["status"] == case["expected"]["verdict"], (
        f"{case['name']}: expected {case['expected']['verdict']}, "
        f"received {verdict['status']}: {verdict.get('rationale', '')}"
    )
    assert verdict["rationale"].strip()
    references = verdict.get("references", [])
    assert references, "A definitive verdict must cite the captured trajectory"
    captured = {event["span_id"] for event in case["input"]["trajectory"]["events"]}
    assert all(reference["span_id"] in captured for reference in references)
    # The gold supporting span is illustrative; other captured spans may support
    # the same conclusion. Do not turn valid alternate citations into failures.
    assert not any("prompt_event_body_truncated" in item for item in verdict["limitations"])


@pytest.mark.skipif(
    not (LIVE_ENABLED and HAS_GEMINI_KEY),
    reason="Live gold judge quality requires LISA_RUN_STATE_BENCH_LIVE=1 and Gemini credentials",
)
@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_state_bench_gold_judge_quality(case):
    """Measure actual Gemini verdict direction independently of policy routing."""
    request = EvaluationRequest.model_validate(case["input"])
    verdict = run_evaluation(request.model_dump(mode="json", exclude_unset=True))
    assert_gold_verdict(case, verdict)


@pytest.mark.skipif(
    not (LIVE_ENABLED and HAS_OPENAI_KEY and HAS_GEMINI_KEY),
    reason=(
        "Live gold selection+judge quality requires LISA_RUN_STATE_BENCH_LIVE=1, "
        "OPENAI_API_KEY and Gemini credentials"
    ),
)
@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_state_bench_gold_policy_selection_then_evaluation(case, monkeypatch, tmp_path):
    """Exercise Lisa's MCP evaluation path and judge every policy it selects."""
    rules = {
        name: replace(rule, id=f"STATE-BENCH-{index}")
        for index, (name, rule) in enumerate(load_gold_rules().items(), start=1)
    }
    context = WorkspaceContext(
        workspace="11111111-1111-4111-8111-111111111111",
        data_dir=tmp_path,
    )
    services = build_services(context)
    session_id = case["input"]["run_id"]
    trajectory = case["input"]["trajectory"]
    detail = {
        "session_id": session_id,
        "agent_names": ["state-bench-agent"],
        "spans": [{"trace_id": trajectory["trace_id"]}],
        "scores": [],
    }
    production_trajectory = {
        **trajectory,
        "session_id": session_id,
        "agent_names": detail["agent_names"],
        "trace_ids": [trajectory["trace_id"]],
    }
    monkeypatch.setattr(
        services.policy_service,
        "approved_rules",
        lambda _agent_name: list(rules.values()),
    )
    monkeypatch.setattr(services.client, "get", lambda _path: detail)
    monkeypatch.setattr(services.sessions, "trajectory", lambda _detail: production_trajectory)
    posted_scores = []

    def post_score(path, payload, *, include_status=False):
        assert path == "/v1/scores"
        posted_scores.append(payload)
        return ({}, 201) if include_status else {}

    monkeypatch.setattr(services.client, "post", post_score)
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    recording_root = context.home() / "llm-calls"
    before = set(recording_root.glob("*.jsonl")) if recording_root.exists() else set()

    async def call_production_tool():
        async with Client(create_server(services)) as client:
            return await client.call_tool(
                "evaluate_session",
                {"target_agent_name": "state-bench-agent", "session_id": session_id},
            )

    tool_result = asyncio.run(call_production_tool())
    if services.evaluations.policy_selector.client is not None:
        services.evaluations.policy_selector.client.close()
    assert not tool_result.is_error, str(tool_result)
    result = tool_result.data

    selected_names = [item["name"] for item in result["selected_policies"]]
    selected_ids = [item["id"] for item in result["selected_policies"]]
    evaluations = result["evaluations"]
    assert [item["evaluation_name"] for item in evaluations] == selected_names
    assert not result.get("prompt_truncations")
    assert len(posted_scores) == len(selected_names)

    new_records = set(recording_root.glob("*.jsonl")) - before
    call_records = [
        [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        for path in sorted(new_records)
    ]
    selector_requests = [
        rows[0] for rows in call_records
        if rows and rows[0].get("type") == "request" and rows[0].get("surface") == "selector"
    ]
    judge_records = [
        rows for rows in call_records
        if rows and rows[0].get("type") == "request" and rows[0].get("surface") == "judge"
    ]
    assert len(selector_requests) == 1
    assert len(judge_records) == len(selected_names), (
        f"Lisa selected {selected_names!r} but recorded {len(judge_records)} judge requests"
    )
    judge_policy_names = [
        rows[0]["request"]["contents"].splitlines()[0].removeprefix("Policy: ")
        for rows in judge_records
    ]
    assert sorted(judge_policy_names) == sorted(selected_names)
    judge_criteria = {
        rows[0]["request"]["contents"].splitlines()[0].removeprefix("Policy: "):
        rows[0]["request"]["contents"].splitlines()[1]
        for rows in judge_records
    }
    assert judge_criteria == {name: rules[name].criteria for name in selected_names}
    assert all(len(rows) == 2 and rows[1].get("type") == "response" for rows in judge_records)
    recorded_verdicts = {
        rows[0]["request"]["contents"].splitlines()[0].removeprefix("Policy: "):
        rows[1]["response"]["parsed"]["status"]
        for rows in judge_records
    }
    assert recorded_verdicts == {
        item["evaluation_name"]: item["status"] for item in evaluations
    }
    assert [payload["name"] for payload in posted_scores] == selected_names
    assert [payload["metadata"]["policy_id"] for payload in posted_scores] == selected_ids
    assert result["status"] == "evaluated"
    assert all(item["status"] in {"pass", "fail", "uncertain"} for item in evaluations)

    expected_name = case["expected"]["selected_policy"]
    expected_evaluation = next(
        item for item in evaluations if item["evaluation_name"] == expected_name
    )
    assert_gold_verdict(case, expected_evaluation)
    assert selected_names == [expected_name], (
        f"{case['name']}: expected policy {expected_name!r}; selected {selected_names!r}"
    )
