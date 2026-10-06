#!/usr/bin/env python3
"""Validate Softprobe SDK contract schemas and fixtures."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "contracts" / "schemas"
FIXTURES = ROOT / "contracts" / "fixtures"


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def assert_type(value: Any, expected: str | list[str], path: str) -> None:
    mapping = {
        "string": str,
        "number": (int, float),
        "integer": int,
        "boolean": bool,
        "object": dict,
        "array": list,
        "null": type(None),
    }
    expected_list = [expected] if isinstance(expected, str) else expected
    ok = False
    for item in expected_list:
        py = mapping[item]
        if item == "number" and isinstance(value, bool):
            continue
        if item == "integer" and isinstance(value, bool):
            continue
        if isinstance(value, py):
            ok = True
            break
    if not ok:
        fail(f"{path}: expected {expected_list}, got {type(value).__name__}")


def validate_against_schema(instance: Any, schema: dict[str, Any], path: str = "$") -> None:
    if "$ref" in schema:
        ref = schema["$ref"]
        ref_path = SCHEMAS / Path(ref).name
        validate_against_schema(instance, load_json(ref_path), path)
        return

    schema_type = schema.get("type")
    if schema_type is not None:
        assert_type(instance, schema_type, path)

    if "const" in schema and instance != schema["const"]:
        fail(f"{path}: expected const {schema['const']!r}")

    if "enum" in schema and instance not in schema["enum"]:
        fail(f"{path}: value {instance!r} not in enum {schema['enum']}")

    if "minLength" in schema and isinstance(instance, str) and len(instance) < schema["minLength"]:
        fail(f"{path}: string shorter than minLength {schema['minLength']}")

    if schema.get("type") == "array":
        if "minItems" in schema and len(instance) < schema["minItems"]:
            fail(f"{path}: array shorter than minItems")
        if "maxItems" in schema and len(instance) > schema["maxItems"]:
            fail(f"{path}: array longer than maxItems")
        if schema.get("uniqueItems") and len(instance) != len(set(json.dumps(x, sort_keys=True) for x in instance)):
            fail(f"{path}: array items are not unique")
        item_schema = schema.get("items")
        if item_schema:
            for idx, item in enumerate(instance):
                validate_against_schema(item, item_schema, f"{path}[{idx}]")

    if schema.get("type") == "object":
        required = schema.get("required", [])
        for key in required:
            if key not in instance:
                fail(f"{path}: missing required property {key!r}")
        properties = schema.get("properties", {})
        additional = schema.get("additionalProperties", True)
        for key, value in instance.items():
            if key in properties:
                validate_against_schema(value, properties[key], f"{path}.{key}")
            elif additional is False:
                fail(f"{path}: unexpected property {key!r}")
            elif isinstance(additional, dict):
                validate_against_schema(value, additional, f"{path}.{key}")


def main() -> None:
    required_schemas = [
        "observation-types.json",
        "resource-attributes.json",
        "observation-attributes.json",
        "content-events.json",
        "score-request.json",
        "normalized-span.json",
    ]
    for name in required_schemas:
        path = SCHEMAS / name
        if not path.exists():
            fail(f"missing schema {path}")
        load_json(path)

    catalog = load_json(FIXTURES / "canonical-catalog.json")
    observation_types_schema = load_json(SCHEMAS / "observation-types.json")
    for idx, value in enumerate(catalog["observationTypes"]):
        validate_against_schema(value, observation_types_schema, f"catalog.observationTypes[{idx}]")
    if catalog["observationTypes"] != observation_types_schema["enum"]:
        fail("catalog.observationTypes must match schema enum exactly")

    validate_against_schema(
        catalog["resourceAttributes"],
        load_json(SCHEMAS / "resource-attributes.json"),
        "catalog.resourceAttributes",
    )
    validate_against_schema(
        catalog["contentEvents"],
        load_json(SCHEMAS / "content-events.json"),
        "catalog.contentEvents",
    )

    spans = load_json(FIXTURES / "expected-nested-spans.json")
    span_schema = load_json(SCHEMAS / "normalized-span.json")
    if not isinstance(spans, list) or not spans:
        fail("expected-nested-spans.json must be a non-empty array")
    names = set()
    for idx, span in enumerate(spans):
        validate_against_schema(span, span_schema, f"spans[{idx}]")
        names.add(span["name"])
        obs_type = span["attributes"].get("sp.observation.type")
        if obs_type != span["observation_type"]:
            fail(f"spans[{idx}]: observation_type mismatch with attributes")
    expected_types = set(observation_types_schema["enum"]) - {"event"}
    present_types = {span["observation_type"] for span in spans}
    missing = expected_types - present_types
    if missing:
        fail(f"expected-nested-spans.json missing observation types: {sorted(missing)}")

    for idx, span in enumerate(spans):
        parent = span.get("parent_name")
        if parent is not None and parent not in names:
            fail(f"spans[{idx}]: unknown parent_name {parent!r}")

    scores = load_json(FIXTURES / "expected-scores.json")
    score_schema = load_json(SCHEMAS / "score-request.json")
    if not isinstance(scores, list) or len(scores) != 3:
        fail("expected-scores.json must contain exactly 3 scores")
    for idx, score in enumerate(scores):
        validate_against_schema(score, score_schema, f"scores[{idx}]")
        has_target = any(score.get(key) for key in ("trace_id", "span_id", "session_id"))
        if not has_target:
            fail(f"scores[{idx}]: must target span, trace, or session")

    privacy = load_json(FIXTURES / "privacy-redaction.json")
    for key in ("redactKeys", "redactedPlaceholder", "exampleInput"):
        if key not in privacy:
            fail(f"privacy-redaction.json missing {key}")

    validate_tool_calls_fixture(catalog)

    print("Contract schemas and fixtures are valid.")


def validate_tool_calls_fixture(catalog: dict[str, Any]) -> None:
    """Validate Issue #7 Part A tool-call fixture (parallel siblings, correlation)."""
    attribute_schema = load_json(SCHEMAS / "observation-attributes.json")
    attribute_properties = attribute_schema["properties"]

    tool_attribute_catalog = catalog.get("toolAttributes")
    if not tool_attribute_catalog:
        fail("canonical-catalog.json missing toolAttributes")
    for role, keys in tool_attribute_catalog.items():
        for key in keys:
            if key not in attribute_properties:
                fail(f"catalog.toolAttributes.{role}: {key!r} not in observation-attributes.json")

    fixture = load_json(FIXTURES / "expected-tool-calls.json")
    calls = fixture.get("parallelCalls")
    if not isinstance(calls, list) or len(calls) < 2:
        fail("expected-tool-calls.json parallelCalls must contain at least 2 sibling tool spans")

    summary = fixture.get("generationSummary")
    if not isinstance(summary, dict):
        fail("expected-tool-calls.json missing generationSummary")
    validate_against_schema(summary, attribute_schema, "toolCalls.generationSummary")
    summary_ids = summary.get("sp.tool.call_ids", [])
    summary_names = summary.get("sp.tool.call_names", [])
    if summary.get("sp.tool.call_count") != len(summary_names):
        fail("generationSummary: sp.tool.call_count must match sp.tool.call_names length")
    if summary.get("sp.tool.available_count") != len(summary.get("sp.tool.available_names", [])):
        fail("generationSummary: sp.tool.available_count must match sp.tool.available_names length")

    parents = set()
    statuses = set()
    for idx, call in enumerate(calls):
        path = f"toolCalls.parallelCalls[{idx}]"
        attrs = call.get("attributes")
        if not isinstance(attrs, dict):
            fail(f"{path}: missing attributes")
        validate_against_schema(attrs, attribute_schema, path)
        if attrs.get("sp.observation.type") != "tool":
            fail(f"{path}: sp.observation.type must be 'tool'")
        parents.add(call.get("parent_name"))
        statuses.add(attrs.get("sp.tool.status"))
        call_id = attrs.get("gen_ai.tool.call.id")
        if call_id and call_id not in summary_ids:
            fail(f"{path}: gen_ai.tool.call.id {call_id!r} not in generation sp.tool.call_ids")
        name = attrs.get("gen_ai.tool.name")
        if name and name not in summary_names:
            fail(f"{path}: gen_ai.tool.name {name!r} not in generation sp.tool.call_names")
        if attrs.get("sp.tool.status") == "error" and call.get("status_code") != "ERROR":
            fail(f"{path}: sp.tool.status=error requires status_code ERROR")
    if len(parents) != 1 or None in parents:
        fail("parallelCalls must be siblings under a single named parent generation")
    if "error" not in statuses:
        fail("expected-tool-calls.json must exercise an error-status tool span")

    message = fixture.get("toolMessageShape")
    if not isinstance(message, dict):
        fail("expected-tool-calls.json missing toolMessageShape")
    for key in ("role", "name", "tool_call_id", "content"):
        if key not in message:
            fail(f"toolMessageShape missing {key!r}")
    if message.get("tool_call_id") not in summary_ids:
        fail("toolMessageShape.tool_call_id must correlate with generation sp.tool.call_ids")


if __name__ == "__main__":
    main()
