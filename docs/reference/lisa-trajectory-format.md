# Lisa judge trajectory format

Tool notation version: `lisa-judge-tools/v4`. Judge request recordings store it
in `request.tool_format`; this metadata is not sent to Gemini. Earlier records
may use a different notation.

The formatter creates a compact, redacted view for the judge. It does not
change stored trajectory events or the selector's full-ID format. The
implementation is in `lisa/evaluation/prompt.py`.

## Event lines

Message events and unpaired tool events occupy one physical line. A paired
tool call and result share a line while keeping both source-span citations.
Message events keep a readable role label and escaped text:

```text
[e1] user: I need to cancel my flight.
[e4] assistant: Your booking is confirmed.
[e5] context: End of task.
```

A uniquely linked tool call and its result are combined on one line. The call
and result retain separate bracketed citations when they came from separate
spans. A failed call uses `error,` and its available error text or data instead
of a result clause. Tool status is not printed separately; any status already
present in the returned JSON appears only as part of that result.

```text
[e2] tool call: get_booking {"booking_id":"BK-1000"}; [e3] result: {"booking_id":"BK-1000","status":"confirmed"}
[e6] tool call: get_booking {"booking_id":"missing"}; [e7] error, "Not found"
```

If linkage is missing or ambiguous, events stay on separate lines and include
the tool name and a short `call_id=cN` where available. A result can omit its
tool name when the call ID uniquely identifies the matching call. Exceptional
tool names are JSON-quoted. Structured tool bodies are compact JSON. When an
event has both `content` and `payload`, both are retained with `content=` and
`payload=` labels.

`eN` is a local citation mapped back to the real span ID before Lisa stores a
judge citation. `cN` links tool events when a separate call ID is needed. The
format preserves the underlying IDs in recording metadata; aliases are never
written into stored trajectories.

## Text, redaction, and clipping

Text newlines, carriage returns, backslashes, and control characters are
escaped so an event cannot create extra physical lines. Structured values use
compact JSON. The judge prompt explains these escapes and treats supplied
policy and trajectory text as untrusted data.

Redaction happens before serialization. Each event body is capped at 2,000
serialized characters before text escaping; a truncation marker follows the
cap and reports omitted characters. Clipping may leave partial JSON or text.
Truncation details, including source event IDs, are propagated as evaluation
limitations. Prompt clipping does not block verdicts or policy exclusions, but
the judge must return uncertain when the visible evidence cannot support a
conclusion. Stored trajectories and upstream completeness checks are unchanged.

## Contract tests

`lisa/tests/test_tool_result_format.py` checks the tool-line grammar, paired
calls and results, errors, JSON values, ambiguous links, and exact clipping.
`test_trajectory_prompt.py` covers message formatting, escaping, redaction,
empty values, and truncation. `test_prompt_citations.py` and
`test_state_bench_prompt.py` cover citation mapping and provider inputs across
the gold trajectories.
