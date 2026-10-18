"""Opt-in real-model dataset calibration, reported independently for every case."""

from __future__ import annotations

import asyncio
import json
import os

import pytest
from fastmcp import Client

from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.evaluation.contracts import EvaluationRequest
from lisa.evaluation.dataset import load_dataset
from lisa.evaluation.judge import run_evaluation
from lisa.mcp_app import create_server
from lisa.models.policies import PolicyRule

DATA = load_dataset()
PAIRS = [p for p in DATA.pairs() if p.annotation.outcome != "not_applicable"]
BATCHES = DATA.manifest.batches
LIVE_ENABLED = os.getenv("LISA_RUN_EVALUATION_LIVE") == "1"
HAS_OPENAI_KEY = bool(os.getenv("OPENAI_API_KEY"))
HAS_GEMINI_KEY = bool(os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY"))


def assert_pair_verdict(pair, verdict):
    expected = {
        "violation": {"fail"},
        "compliant": {"pass"},
        "insufficient_evidence": {"uncertain", "insufficient_evidence"},
        "not_applicable": {"pass", "uncertain"},
    }[pair.annotation.outcome]
    assert verdict["status"] in expected, (
        f"{pair.trajectory.name}: expected {pair.annotation.outcome}, received {verdict}"
    )
    assert verdict["rationale"].strip()
    if verdict["status"] in {"pass", "fail"}:
        references = verdict.get("references", [])
        assert references
        captured = {e.span_id for e in pair.trajectory.trajectory.events}
        assert all(reference["span_id"] in captured for reference in references)
    assert not any("prompt_event_body_truncated" in item for item in verdict["limitations"])


@pytest.mark.skipif(
    not (LIVE_ENABLED and HAS_GEMINI_KEY),
    reason="Live dataset judge quality requires LISA_RUN_EVALUATION_LIVE=1 and Gemini credentials",
)
@pytest.mark.parametrize(
    "pair", PAIRS, ids=lambda pair: f"{pair.trajectory.provenance['source']}:{pair.trajectory.name}"
)
def test_pair_judge_calibration(pair):
    """Calibrate against explicit annotations; proposed labels are author expectations."""
    request = EvaluationRequest.model_validate(pair.request())
    verdict = run_evaluation(request.model_dump(mode="json", exclude_unset=True))
    assert_pair_verdict(pair, verdict)


@pytest.mark.skipif(
    not (LIVE_ENABLED and HAS_OPENAI_KEY and HAS_GEMINI_KEY),
    reason=(
        "Live dataset selection+judge quality requires LISA_RUN_EVALUATION_LIVE=1, "
        "OPENAI_API_KEY and Gemini credentials"
    ),
)
@pytest.mark.parametrize("batch", BATCHES, ids=lambda batch: batch.trajectory_ref)
def test_catalog_selection_through_mcp(batch, monkeypatch, tmp_path):
    """Exercise Lisa's MCP evaluation path and judge every policy it selects."""
    rules = {
        ref: PolicyRule(id=ref, **DATA.policies[ref].policy.model_dump())
        for ref in batch.policy_refs
    }
    trajectory_record = DATA.trajectories[batch.trajectory_ref]
    context = WorkspaceContext(
        workspace="11111111-1111-4111-8111-111111111111",
        data_dir=tmp_path,
    )
    services = build_services(context)
    session_id = trajectory_record.id
    trajectory = trajectory_record.trajectory.model_dump(mode="json", exclude_unset=True)
    detail = {
        "session_id": session_id,
        "agent_names": ["test-agent"],
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
                {"target_agent_name": "test-agent", "session_id": session_id},
            )

    tool_result = asyncio.run(call_production_tool())
    if services.evaluations.policy_selector.client is not None:
        services.evaluations.policy_selector.client.close()
    assert not tool_result.is_error, str(tool_result)
    result = tool_result.data

    if not trajectory["complete"]:
        assert result["status"] == "insufficient_evidence"
        assert result["evaluations"] == []
        assert posted_scores == []
        return

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
        rows[0]
        for rows in call_records
        if rows and rows[0].get("type") == "request" and rows[0].get("surface") == "selector"
    ]
    judge_records = [
        rows
        for rows in call_records
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
        rows[0]["request"]["contents"].splitlines()[0].removeprefix("Policy: "): rows[0]["request"][
            "contents"
        ].splitlines()[1]
        for rows in judge_records
    }
    assert judge_criteria == {
        rule.name: rule.criteria for rule in rules.values() if rule.name in selected_names
    }
    assert all(len(rows) == 2 and rows[1].get("type") == "response" for rows in judge_records)
    recorded_verdicts = {
        rows[0]["request"]["contents"].splitlines()[0].removeprefix("Policy: "): rows[1][
            "response"
        ]["parsed"]["status"]
        for rows in judge_records
    }
    assert recorded_verdicts == {item["evaluation_name"]: item["status"] for item in evaluations}
    assert [payload["name"] for payload in posted_scores] == selected_names
    assert [payload["metadata"]["policy_id"] for payload in posted_scores] == selected_ids
    if selected_names:
        assert result["status"] == "evaluated"
    assert all(item["status"] in {"pass", "fail", "uncertain"} for item in evaluations)

    for pair in DATA.pairs(batch, adjudicated_only=True):
        evaluation = next(
            (e for e in evaluations if e["evaluation_name"] == pair.policy.policy.name), None
        )
        if pair.annotation.outcome == "violation":
            assert evaluation is not None, f"Missed violation: {pair.annotation.ref}"
        if evaluation is not None:
            assert_pair_verdict(pair, evaluation)
