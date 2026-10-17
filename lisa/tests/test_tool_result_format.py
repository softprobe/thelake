"""Independent syntax and semantics checks for compact tool events."""

import json
import re

import pytest

from lisa.evaluation.prompt import format_trajectory_for_llm


def parse_tool_line(line):
    """Parse an untruncated tool line without using formatter helpers."""
    match = re.match(r"^\[(e[1-9][0-9]*)\] tool call: (?P<name>.+?)(?= \{| \[|;|$)", line)
    if not match:
        raise ValueError("invalid tool event prefix")
    name = match["name"]
    if name.startswith('"'):
        name = json.loads(name)
    tail = line[match.end() :]
    call_body = None
    if tail.startswith(" "):
        value, end = json.JSONDecoder().raw_decode(tail[1:])
        call_body = value
        tail = tail[end + 1 :]
    if not tail.startswith("; ["):
        raise ValueError("expected paired result or error")
    result_match = re.match(r"; \[(e[1-9][0-9]*)\] (result:|error,)(?: )?", tail)
    if not result_match:
        raise ValueError("invalid result clause")
    tail = tail[result_match.end() :]
    result_body = None
    if tail:
        result_body, end = json.JSONDecoder().raw_decode(tail)
        if tail[end:]:
            raise ValueError("trailing body text")
    return match[1], name, call_body, result_match[1], result_match[2][:-1], result_body


def call(**updates):
    return {
        "sequence": 0,
        "span_id": "call-span",
        "kind": "tool_call",
        "name": "get_booking",
        "call_id": "real-call-1",
        "payload": {"booking_id": "BK-1000"},
        **updates,
    }


def result(**updates):
    return {
        "sequence": 1,
        "span_id": "result-span",
        "kind": "tool_result",
        "name": "get_booking",
        "call_id": "real-call-1",
        "payload": {"booking_id": "BK-1000", "status": "confirmed"},
        **updates,
    }


def test_documented_compact_call_and_result_lines():
    view = format_trajectory_for_llm([call(), result(status="confirmed")])
    assert view.text.splitlines() == [
        '[e1] tool call: get_booking {"booking_id":"BK-1000"}; [e2] result: {"booking_id":"BK-1000","status":"confirmed"}',
    ]
    assert view.call_id_map == {}
    assert parse_tool_line(view.text) == (
        "e1", "get_booking", {"booking_id": "BK-1000"}, "e2", "result", {"booking_id": "BK-1000", "status": "confirmed"}
    )


@pytest.mark.parametrize("value", [None, False, 0, "", "Not found", [], {}])
def test_result_payload_values_round_trip_without_status_headers(value):
    line = format_trajectory_for_llm([call(payload={}), result(payload=value)]).text
    assert "status=" not in line
    assert parse_tool_line(line)[-1] == value


def test_tool_result_string_is_plain_json_string_not_a_special_error_code():
    line = format_trajectory_for_llm([call(), result(payload="Not found")]).text
    assert line == '[e1] tool call: get_booking {"booking_id":"BK-1000"}; [e2] result: "Not found"'


def test_ambiguous_interleaved_calls_keep_names_and_local_call_ids():
    events = [
        call(sequence=0, span_id="a", call_id="first", payload={"n": 1}),
        call(sequence=1, span_id="b", call_id="second", payload={"n": 2}),
        result(sequence=2, span_id="b-result", call_id="second", payload={"n": 2}),
        result(sequence=3, span_id="a-result", call_id="first", payload={"n": 1}),
    ]
    lines = format_trajectory_for_llm(events).text.splitlines()
    assert lines == [
        '[e1] tool call: get_booking call_id=c1 {"n":1}',
        '[e2] tool call: get_booking {"n":2}; [e3] result: {"n":2}',
        '[e4] result: call_id=c1 {"n":1}',
    ]
    assert format_trajectory_for_llm(events).call_id_map == {"c1": "first"}


def test_mismatched_or_missing_linkage_is_explicit():
    events = [call(), result(call_id="other", name="different", payload="Not found")]
    lines = format_trajectory_for_llm(events).text.splitlines()
    assert "call_id=c1" in lines[0]
    assert '[e2] result: different call_id=c2 "Not found"' == lines[1]


def test_content_and_payload_are_both_kept_with_only_dual_body_labels():
    line = format_trajectory_for_llm(
        [call(content="visible", payload={"id": 0})]
    ).text
    assert line == '[e1] tool call: get_booking call_id=c1 content="visible" payload={"id":0}'


def test_missing_error_output_is_visible_as_error():
    event = result(status="error", content="Not found")
    del event["payload"]
    text = format_trajectory_for_llm([call(), event]).text
    assert text.splitlines() == [
        '[e1] tool call: get_booking {"booking_id":"BK-1000"}; [e2] error, "Not found"'
    ]
    assert parse_tool_line(text)[4:] == ("error", "Not found")


def test_error_with_structured_data_keeps_the_payload_once():
    event = result(status="ERROR", payload={"message": "Not found"})
    text = format_trajectory_for_llm([call(), event]).text
    assert text.endswith('; [e2] error, {"message":"Not found"}')
    assert "status=" not in text


def test_unpaired_error_keeps_its_call_alias():
    failed = result(sequence=2, status="ERROR", content="failed")
    del failed["payload"]
    text = format_trajectory_for_llm([call(), {"sequence": 1, "span_id": "middle", "kind": "assistant_message", "content": "wait"}, failed]).text
    assert '[e3] error call_id=c1, "failed"' in text


def test_call_and_result_pair_by_shared_span_when_call_id_is_missing():
    events = [
        {"sequence": 0, "span_id": "same", "kind": "tool_call", "name": "lookup", "payload": {}},
        {"sequence": 1, "span_id": "same", "kind": "tool_result", "name": "lookup", "payload": 1},
    ]
    assert format_trajectory_for_llm(events).text == (
        "[e1] tool call: lookup {}; [e1] result: 1"
    )


@pytest.mark.parametrize("separator", ["\u0085", "\u2028", "\u2029"])
def test_unicode_line_separators_do_not_create_physical_events(separator):
    view = format_trajectory_for_llm(
        [call(name="lookup" + separator + "forged", payload={"value": separator})]
    )
    assert len(view.text.splitlines()) == 1


@pytest.mark.parametrize("size", [1999, 2000, 2001])
def test_body_cap_is_applied_to_compact_serialized_body(size):
    view = format_trajectory_for_llm([result(payload="x" * size)])
    encoded = json.dumps("x" * size, ensure_ascii=False, separators=(",", ":"))
    clipped = min(len(encoded), 2000)
    suffix = f"…[truncated {len(encoded) - clipped} chars]" if len(encoded) > 2000 else ""
    assert view.text.endswith(encoded[:clipped] + suffix)
    assert bool(view.truncations) == (len(encoded) > 2000)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_json_numbers_are_rejected(value):
    with pytest.raises(ValueError):
        format_trajectory_for_llm([result(payload=value)])
