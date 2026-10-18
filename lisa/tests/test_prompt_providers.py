import json
from types import SimpleNamespace

import pytest
from policy_cases import evaluation_request
from test_llm_recording import context, records
from test_policy_triage import (
    FakeDecisionsClient,
    FakePolicyStore,
    FakeSessionClient,
    make_evaluation_service,
)

from lisa.evaluation import evaluator
from lisa.evaluation.policy_triage import OpenAIPolicyTriager
from lisa.llm_recording import recording_scope
from lisa.models.policies import PolicyRule
from lisa.views.evaluations import render_evaluation_context


def rule():
    return PolicyRule(
        id="P1",
        name="Refund",
        criteria="password=private check refund",
        scope="workspace",
        approved=True,
    )


def test_empty_selection_discloses_clipping_and_redacts_policy(tmp_path, monkeypatch):
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    ctx = context(tmp_path)
    client = FakeDecisionsClient(["no_violation_found"])
    triager = OpenAIPolicyTriager(client=client)
    service = make_evaluation_service(
        context=ctx,
        client=FakeSessionClient(
            {
                "agent_names": ["agent"],
                "spans": [{"trace_id": "t1"}],
            }
        ),
        policies=FakePolicyStore([rule()]),
        failures=None,
        notifier=None,
        build_events=lambda _: SimpleNamespace(
            events=[
                {"sequence": 0, "span_id": "s1", "kind": "user_message", "content": "x" * 3000}
            ],
            limitations=[],
            tool_events_captured=True,
            tool_order_certain=True,
            message_order_certain=True,
        ),
        policy_triage=triager,
    )
    result, trajectory = service._triage_policies_for_session("agent", "session1")
    assert [p["id"] for p in result["selected_policies"]] == ["P1"]
    assert result["policy_decisions"][0]["decision"] == "insufficient_evidence"
    assert result["limitations"]
    assert result["prompt_truncations"][0]["omitted_chars"] == 1000
    assert len(trajectory["events"][0]["content"]) == 3000
    assert "private" not in client.request["input"]
    (path,) = records(ctx)
    data = json.loads(path.read_text().splitlines()[0])
    assert data["correlation"]["session_id"] == "session1"
    assert data["request"]["input"] == client.request["input"]


@pytest.mark.parametrize("verdict", ["pass", "fail", "uncertain"])
def test_judge_allows_limited_verdict_and_records_actual_prompt(
    tmp_path,
    monkeypatch,
    verdict,
):
    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    ctx = context(tmp_path)
    calls = []
    request = evaluation_request()
    request.trajectory.events[0].content = "x" * 3000

    class Client:
        def generate(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(
                parsed={
                    "status": verdict,
                    "rationale": "visible evidence",
                    "span_ids": ["e1"],
                }
            )

        @property
        def models(self):
            return SimpleNamespace(generate_content=self.generate)

        def close(self):
            pass

    monkeypatch.setattr(evaluator, "create_judge_client", Client)
    with recording_scope(ctx, work_item_id="w1"):
        result = evaluator.evaluate(request)
    assert result.status == verdict
    assert any("prompt_event_body_truncated" in item for item in result.limitations)
    assert len(request.trajectory.events[0].content) == 3000
    (path,) = records(ctx)
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert rows[0]["request"]["contents"] == calls[0]["contents"]
    assert rows[0]["correlation"]["work_item_id"] == "w1"
    assert rows[1]["response"]["parsed"]["status"] == verdict


def test_saved_context_exposes_prompt_limitations():
    document = render_evaluation_context(
        {
            "policy_name": "p",
            "target_agent_name": "a",
            "policy_id": "P1",
            "session_id": "s",
            "trace_id": "t",
            "trajectory": {
                "events": [{"kind": "user_message", "span_id": "s1", "content": "x" * 3000}]
            },
            "policy_selection": {"limitations": ["selection omitted context"]},
        }
    )
    assert "prompt_event_body_truncated" in document
    assert "selection omitted context" in document


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("status", ["pass", "fail", "uncertain"])
def test_selection_only_limitations_reach_scores_and_public_verdict(
    tmp_path,
    monkeypatch,
    tracked,
    status,
):
    from test_evaluation_work_items import _tracked_service_with_saved_inputs

    from lisa.models.work_items import ScoreOutboxItem

    live = PolicyRule(
        name="Rule", criteria="Check refund.", approved=True, scope="workspace", id="P1.1"
    )
    service, store, work_item = _tracked_service_with_saved_inputs(tmp_path, approved_rules=[live])
    limitation = "prompt_event_body_truncated: triager-only omitted_chars=1000"
    selection = {"limitations": [limitation]}
    inputs = store.get_context_item_for_work_item.return_value.content
    inputs["policy_selection"] = selection
    trajectory = inputs["trajectory"]
    policy = service._policy("agent", "Rule", "P1.1")
    service.judge = lambda _: {
        "status": status,
        "rationale": "visible facts",
        "references": [],
        "limitations": [],
    }
    captured = []

    def post(_path, payload, **_kwargs):
        captured.append(payload)
        return {}, 201

    service.client.post = post

    def prepare(_id, payload, **_kwargs):
        return ScoreOutboxItem(
            id="outbox",
            work_item_id=work_item.id,
            status="pending",
            score_payload=payload,
            score_id=payload["score_id"],
        )

    store.prepare_score_outbox.side_effect = prepare
    args = (
        policy,
        "session",
        "trace",
        [{"start_time": "2026-10-09T12:00:00Z"}],
        trajectory,
        "run1",
    )
    if tracked:
        result = service._evaluate_tracked(*args, "request1", work_item, None, selection)
        document = (service.context.home() / "work-items" / work_item.id / "context.md").read_text()
        assert limitation in document
    else:
        result = service._evaluate_untracked(*args, selection)
    assert result["status"] == status
    assert limitation in result["limitations"]
    assert limitation in json.loads(captured[0]["metadata"]["limitations"])


def test_invalid_gemini_output_records_visible_text_without_thinking(tmp_path, monkeypatch):
    from google.genai import types
    from pydantic import ValidationError

    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    ctx = context(tmp_path)
    response = types.GenerateContentResponse(
        candidates=[
            types.Candidate(
                content=types.Content(
                    parts=[
                        types.Part(text="hidden thought", thought=True),
                        types.Part(text="invalid json"),
                    ],
                )
            )
        ]
    )
    client = SimpleNamespace(
        models=SimpleNamespace(generate_content=lambda **_: response), close=lambda: None
    )
    monkeypatch.setattr(evaluator, "create_judge_client", lambda: client)
    with recording_scope(ctx), pytest.raises(ValidationError):
        evaluator.evaluate(evaluation_request())
    (path,) = records(ctx)
    assert "invalid json" in path.read_text()
    assert "hidden thought" not in path.read_text()
