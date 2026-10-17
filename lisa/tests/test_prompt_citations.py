from types import SimpleNamespace

import pytest
from policy_cases import evaluation_request

from lisa.evaluation import evaluator
from lisa.evaluation.prompt import format_trajectory_for_llm
from lisa.llm_recording import safe_data


def test_prompt_ids_are_short_and_mapping_keeps_tool_linkage():
    prefix = "statebench-fixture:long-session-prefix:"
    events = [
        {
            "sequence": 0,
            "span_id": prefix + "call",
            "kind": "tool_call",
            "name": "lookup",
            "call_id": prefix + "tool0",
            "payload": {},
        },
        {
            "sequence": 1,
            "span_id": prefix + "result",
            "kind": "tool_result",
            "name": "lookup",
            "call_id": prefix + "tool0",
            "payload": False,
        },
        {
            "sequence": 2,
            "span_id": prefix + "result",
            "kind": "assistant_message",
            "content": "done",
        },
    ]
    view = format_trajectory_for_llm(events)
    assert prefix not in view.text
    assert view.text.splitlines() == [
        "[e1] tool call: lookup {}; [e2] result: false",
        "[e2] assistant: done",
    ]
    assert view.span_id_map == {"e1": prefix + "call", "e2": prefix + "result"}
    assert view.call_id_map == {}


def test_judge_resolves_provider_aliases_to_real_span_ids(monkeypatch):
    monkeypatch.setattr(
        evaluator,
        "create_judge_client",
        lambda: SimpleNamespace(
            models=SimpleNamespace(
                generate_content=lambda **_: SimpleNamespace(
                    parsed={"status": "pass", "rationale": "supported", "span_ids": ["e1"]}
                )
            ),
            close=lambda: None,
        ),
    )
    result = evaluator.evaluate(evaluation_request())
    assert [ref.span_id for ref in result.references] == ["span"]


@pytest.mark.parametrize("span_id", ["invented", "span", "e2"])
def test_judge_rejects_citations_absent_from_prompt(monkeypatch, span_id):
    monkeypatch.setattr(
        evaluator,
        "create_judge_client",
        lambda: SimpleNamespace(
            models=SimpleNamespace(
                generate_content=lambda **_: SimpleNamespace(
                    parsed={"status": "pass", "rationale": "unsupported", "span_ids": [span_id]}
                )
            ),
            close=lambda: None,
        ),
    )
    with pytest.raises(ValueError, match="outside"):
        evaluator.evaluate(evaluation_request())


def test_recording_preserves_usage_counts_but_redacts_credentials():
    data = safe_data(
        {
            "usage": {
                "prompt_token_count": 100,
                "candidates_token_count": 20,
                "total_token_count": 120,
                "thoughts_token_count": 10,
                "cached_content_token_count": 0,
                "prompt_tokens_details": {"cached_tokens": 4},
                "api_key": "private",
                "access_token": 123,
            }
        }
    )
    assert data["usage"]["prompt_token_count"] == 100
    assert data["usage"]["candidates_token_count"] == 20
    assert data["usage"]["total_token_count"] == 120
    assert data["usage"]["thoughts_token_count"] == 10
    assert data["usage"]["cached_content_token_count"] == 0
    assert data["usage"]["prompt_tokens_details"]["cached_tokens"] == 4
    assert data["usage"]["api_key"] == "[REDACTED]"
    assert data["usage"]["access_token"] == "[REDACTED]"
    assert safe_data({"prompt_token_count": "secret"})["prompt_token_count"] == "[REDACTED]"


def test_identity_mapping_survives_redaction_and_alias_name_collisions():
    ids = ["sk-abcdefghijklmnop1234", "sk-abcdefghijklmnop5678", "e2", "e1"]
    view = format_trajectory_for_llm(
        [
            {"sequence": index, "span_id": span, "kind": "user_message", "content": "hello"}
            for index, span in enumerate(ids)
        ]
    )
    assert list(view.span_id_map.values()) == ids
    assert len(view.text.splitlines()) == 4
    assert "sk-" not in view.text
    assert view.span_id_map["e3"] == "e2"
    assert view.span_id_map["e4"] == "e1"


def test_recorded_judge_response_has_single_output_and_numeric_usage(tmp_path, monkeypatch):
    import json

    from test_llm_recording import context, records

    from lisa.llm_recording import recording_scope

    monkeypatch.setenv("LISA_LLM_TRACE_ENABLED", "1")
    response = SimpleNamespace(
        parsed={"status": "pass", "rationale": "supported", "span_ids": ["e1"]},
        text='{"status":"pass","rationale":"supported","span_ids":["e1"]}',
        usage_metadata=SimpleNamespace(
            model_dump=lambda **_: {
                "prompt_token_count": 100,
                "candidates_token_count": 20,
                "total_token_count": 120,
            }
        ),
    )
    monkeypatch.setattr(
        evaluator,
        "create_judge_client",
        lambda: SimpleNamespace(
            models=SimpleNamespace(generate_content=lambda **_: response),
            close=lambda: None,
        ),
    )
    ctx = context(tmp_path)
    with recording_scope(ctx):
        result = evaluator.evaluate(evaluation_request())
    (path,) = records(ctx)
    request, reply = [json.loads(line) for line in path.read_text().splitlines()]
    assert request["request"]["span_id_map"] == {"e1": "span"}
    assert request["request"]["tool_format"] == "lisa-judge-tools/v4"
    assert result.references[0].span_id == "span"
    assert "parsed" in reply["response"] and "text" not in reply["response"]
    assert reply["response"]["usage"]["total_token_count"] == 120
    assert reply["response"]["usage"]["prompt_token_count"] == 100


def test_judge_keeps_original_identity_before_body_redaction(monkeypatch):
    request = evaluation_request()
    original_id = "sk-abcdefghijklmnop1234"
    request.trajectory.events[0].span_id = original_id
    monkeypatch.setattr(
        evaluator,
        "create_judge_client",
        lambda: SimpleNamespace(
            models=SimpleNamespace(
                generate_content=lambda **_: SimpleNamespace(
                    parsed={"status": "pass", "rationale": "supported", "span_ids": ["e1"]}
                )
            ),
            close=lambda: None,
        ),
    )
    result = evaluator.evaluate(request)
    assert result.references[0].span_id == original_id


def test_verbose_routing_labels_still_redact_sensitive_ids():
    secret = "sk-abcdefghijklmnop1234"
    view = format_trajectory_for_llm(
        [
            {
                "sequence": 0,
                "span_id": secret,
                "kind": "tool_call",
                "name": "lookup",
                "call_id": secret,
                "payload": {},
            }
        ],
        short_ids=False,
    )
    assert secret not in view.text
    assert "[REDACTED]" in view.text


@pytest.mark.parametrize("calls_before", [0, 2])
def test_result_name_is_retained_even_when_call_id_links_it(calls_before):
    calls = [
        {
            "sequence": i,
            "span_id": f"call{i}",
            "kind": "tool_call",
            "name": "lookup",
            "call_id": "shared",
            "payload": {},
        }
        for i in range(calls_before)
    ]
    result = {
        "sequence": calls_before,
        "span_id": "result",
        "kind": "tool_result",
        "name": "lookup",
        "call_id": "shared",
        "status": "ok",
        "payload": {},
    }
    events = calls + [result]
    if calls_before == 0:
        events.append(
            {
                "sequence": 1,
                "span_id": "late",
                "kind": "tool_call",
                "name": "lookup",
                "call_id": "shared",
                "payload": {},
            }
        )
    view = format_trajectory_for_llm(events)
    assert "status=" not in view.text
    if calls_before:
        assert "result: lookup call_id=c1 {}" in view.text
    else:
        assert "result: call_id=c1 {}" in view.text


def test_reserved_or_delimiter_tool_names_are_explicit_calls():
    for name in ("result", "call_id", "status", "args", "output", "content", "lookup@c2", "tool with spaces"):
        view = format_trajectory_for_llm(
            [
                {
                    "sequence": 0,
                    "span_id": "s",
                    "kind": "tool_call",
                    "name": name,
                    "call_id": "x",
                    "payload": {},
                }
            ]
        )
        label = name if name in {"result", "call_id", "status", "args", "output", "content"} else f'"{name}"'
        assert f"tool call: {label} call_id=c1 {{}}" in view.text
