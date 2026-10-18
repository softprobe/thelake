"""Evaluation trajectory fidelity and provider wiring; these do not measure model quality."""

import json
from copy import deepcopy
from types import SimpleNamespace

import pytest
from test_policy_selection import FakeDecisionsClient

from lisa.evaluation import evaluator
from lisa.evaluation.contracts import EvaluationRequest
from lisa.evaluation.dataset import load_dataset
from lisa.evaluation.policy_selection import OpenAIPolicySelector
from lisa.evaluation.prompt import format_trajectory_for_llm
from lisa.models.policies import PolicyRule
from lisa.security import redact_evidence

DATA = load_dataset()
PAIRS = [p for p in DATA.pairs() if p.trajectory.provenance["source"] == "state_bench"]


@pytest.mark.parametrize("pair", PAIRS, ids=lambda pair: pair.trajectory.name)
def test_evaluation_transcript_preserves_each_message_and_tool_payload(pair):
    trajectory = pair.request()["trajectory"]
    before = deepcopy(trajectory)
    view = format_trajectory_for_llm(trajectory["events"])
    events = sorted(redact_evidence(trajectory["events"]), key=lambda event: event["sequence"])
    lines = view.text.splitlines()
    assert len(lines) <= len(events)
    assert not view.truncations, "The dataset cases should fit without losing evidence"
    assert not view.limitations
    aliases = {span: alias for alias, span in view.span_id_map.items()}
    for event in events:
        alias = aliases[event["span_id"]]
        assert f"[{alias}]" in view.text
        if event["kind"] in {"user_message", "assistant_message", "context_message"}:
            role = event["kind"].removesuffix("_message")
            line = next(line for line in lines if line.startswith(f"[{alias}] {role}:"))
            body = line.split(": ", 1)[1]
            # Decode message escapes independently of the production formatter.
            text = json.loads('"' + body + '"')
            assert text == event["content"]
        else:
            if event["kind"] == "tool_call":
                assert f"[{alias}] tool call: {event['name']}" in view.text
            else:
                assert f"[{alias}] result:" in view.text
            compact_payload = json.dumps(
                event["payload"], ensure_ascii=False, separators=(",", ":")
            )
            assert compact_payload in view.text
    assert f"[{aliases[pair.annotation.evidence[0]]}]" in view.text
    assert "statebench-fixture:" not in view.text
    assert trajectory == before
    assert len(view.text) < len(json.dumps(events, ensure_ascii=False))


@pytest.mark.parametrize("pair", PAIRS, ids=lambda pair: pair.trajectory.name)
def test_evaluation_transcript_reaches_both_provider_boundaries(pair, monkeypatch):
    monkeypatch.delenv("LISA_LLM_TRACE_ENABLED", raising=False)
    original = deepcopy(pair)
    batch = next(b for b in DATA.manifest.batches if b.trajectory_ref == pair.trajectory.ref)
    rules = {
        ref: PolicyRule(id=ref, **DATA.policies[ref].policy.model_dump())
        for ref in batch.policy_refs
    }
    expected_status = {
        "violation": "fail",
        "compliant": "pass",
        "insufficient_evidence": "uncertain",
    }[pair.annotation.outcome]
    client = FakeDecisionsClient(
        [
            "suspected_violation" if rule.id == pair.policy.ref else "no_violation_found"
            for rule in rules.values()
        ]
    )
    selection = OpenAIPolicySelector(client=client).select(
        pair.request()["trajectory"],
        list(rules.values()),
    )
    assert [item["name"] for item in selection["selected_policies"]] == [pair.policy.policy.name]
    judge_view = format_trajectory_for_llm(pair.request()["trajectory"]["events"])
    supporting_alias = next(
        alias
        for alias, span in judge_view.span_id_map.items()
        if span == pair.annotation.evidence[0]
    )
    captures = []
    closed = []

    def generate(**kwargs):
        captures.append(kwargs)
        return SimpleNamespace(
            parsed={
                "status": expected_status,
                "rationale": "Synthetic provider response",
                "span_ids": [supporting_alias],
            }
        )

    monkeypatch.setattr(
        evaluator,
        "create_judge_client",
        lambda: SimpleNamespace(
            models=SimpleNamespace(generate_content=generate),
            close=lambda: closed.append(True),
        ),
    )
    request = EvaluationRequest.model_validate(pair.request())
    result = evaluator.evaluate(request)
    selector_transcript = client.request["input"].split("\n\nTrajectory:\n", 1)[1]
    judge_transcript = captures[0]["contents"].split(
        "Trajectory (cite span ids in brackets):\n",
        1,
    )[1]
    events = pair.request()["trajectory"]["events"]
    assert selector_transcript == format_trajectory_for_llm(events, short_ids=False).text
    assert judge_transcript == judge_view.text
    assert "statebench-fixture:" not in judge_transcript
    for alias in judge_view.span_id_map:
        assert f"[{alias}]" in judge_transcript
    for rule in rules.values():
        assert rule.name in client.request["input"]
        # Criteria must be complete after decoding their one-line escape form.
        criteria_line = client.request["input"].split(rule.name + "\n", 1)[1].splitlines()[0]
        assert json.loads('"' + criteria_line + '"') == rule.criteria
    assert result.status == expected_status
    assert result.references[0].span_id == pair.annotation.evidence[0]
    assert not any("truncated" in limitation for limitation in result.limitations)
    assert closed == [True]
    assert pair == original
