from copy import deepcopy

import pytest

from lisa.evaluation.prompt import format_trajectory_for_llm


def event(**updates):
    return {"sequence": 0, "span_id": "s1", "kind": "user_message", **updates}


def test_preserves_order_fields_linkage_and_does_not_mutate():
    events = [
        event(
            sequence=2,
            kind="tool_result",
            span_id="result",
            name="lookup",
            call_id="c1",
            status="ok",
            payload=False,
        ),
        event(sequence=1, kind="tool_call", name="lookup", call_id="c1", payload={}),
        event(content="hello", payload=0),
    ]
    original = deepcopy(events)
    view = format_trajectory_for_llm(events)
    assert view.text.splitlines()[0] == "[e1] user: hello payload=0"
    assert '[e1] tool call: lookup {}; [e2] result: false' in view.text
    assert events == original
    assert not view.truncations


def test_escapes_headers_and_text_without_double_escaping_json():
    view = format_trajectory_for_llm(
        [
            event(span_id="s\n1]", content="literal \\n\nreal\r\t", payload={"x": "a\nb"}),
            event(sequence=1, kind="tool_call", name="tool\nforged", payload="a\nb"),
        ]
    )
    assert len(view.text.splitlines()) == 2
    assert r"literal \\n\nreal\r\t" in view.text
    assert r'payload={"x":"a\nb"}' in view.text
    assert r'tool call: "tool\nforged" "a\nb"' in view.text


@pytest.mark.parametrize("value", [False, 0, "", [], {}])
def test_preserves_empty_payload_values(value):
    import json

    assert format_trajectory_for_llm([event(payload=value)]).text.endswith(
        json.dumps(value, separators=(",", ":"))
    )


@pytest.mark.parametrize("size,omitted", [(1999, 0), (2000, 0), (2001, 1)])
def test_cap_and_metadata(size, omitted):
    view = format_trajectory_for_llm([event(content="x" * size), event(sequence=1, content="tail")])
    assert "[e1] user: tail" in view.text
    assert len(view.truncations) == bool(omitted)
    if omitted:
        assert view.truncations[0] == {"sequence": 0, "span_id": "s1", "omitted_chars": omitted}
        assert "…[truncated 1 chars]" in view.text
        assert view.limitations


def test_redaction_precedes_clipping():
    view = format_trajectory_for_llm([event(payload={"password": "x" * 5000})])
    assert "REDACTED" in view.text
    assert not view.truncations


def test_explicit_null_payload_is_preserved():
    assert format_trajectory_for_llm([event(payload=None)]).text.endswith("payload=null")


def test_conflicting_call_ids_in_one_span_are_preserved():
    view = format_trajectory_for_llm(
        [
            event(kind="tool_call", call_id="first", payload={}),
            event(sequence=1, kind="tool_result", call_id="different", payload={}),
        ]
    )
    assert "call_id=c1" in view.text
    assert "call_id=c2" in view.text
