"""Run the labeled activity task benchmark through Lisa's real agent runtime.

Offline harness only — see ``benchmark`` package docs. Do not relocate into
``lisa.evaluation``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import subprocess
from contextlib import ExitStack
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg
from conftest import (
    LisaTestDatabase,
    create_lisa_test_database,
    start_lisa_postgres,
    stop_lisa_postgres,
)
from datasets.activity_dataset import (
    DEFAULT_MANIFEST,
    ActivityBenchmarkCase,
    ActivityTurn,
    load_activity_dataset,
)

from benchmark.policy_seed import seed_policy_document

ROOT = Path(__file__).resolve().parents[2]
SKILLS = ROOT / "lisa" / "skills"
ARTIFACTS = ROOT / "target" / "test-artifacts" / "lisa-activity-benchmark"
MEMORY_DATASET = ROOT / "lisa" / "datasets" / "activity_memory"
MEMORY_MANIFEST = MEMORY_DATASET / "manifests" / "benchmark.json"
MEMORY_ARTIFACTS = ROOT / "target" / "test-artifacts" / "lisa-activity-memory-benchmark"
OWNER = "activity-benchmark-owner"
OUTSIDER = "activity-benchmark-outsider"
_ACTIVITY_MEMORY_RESULT_CAPTURE_LIMIT_BYTES = 32 * 1024
# OpenHands JSON can escape each byte of a memory result by up to six bytes.
# Allow that expansion plus bounded event metadata, while hard-limiting reads.
_ACTIVITY_MEMORY_EVENT_ENVELOPE_LIMIT_BYTES = 16 * 1024
_ACTIVITY_MEMORY_EVENT_FILE_LIMIT_BYTES = (
    6 * _ACTIVITY_MEMORY_RESULT_CAPTURE_LIMIT_BYTES
    + _ACTIVITY_MEMORY_EVENT_ENVELOPE_LIMIT_BYTES
)
# OpenHands exposes domain tools after these setup calls; tool errors are still
# scored, but setup does not alter the activity-domain order being verified.
_TOOL_SETUP_CALLS = frozenset(
    {"discover_tool_namespaces", "load_tool_namespace", "invoke_skill"}
)


def _interpolate(text: str, values: dict[str, str]) -> str:
    """Replace benchmark placeholders without interpreting braces in case text."""
    result = text
    for key, value in values.items():
        result = result.replace(f"${key}", value).replace(f"{{{key}}}", value)
    return result


def _update_substitutions(services, substitutions: dict[str, str]) -> None:
    pending = [row for row in services.policy_service.list_policy_records() if not row.approved]
    if pending:
        substitutions["policy_id"] = str(pending[0].id)


def _score_state(
    services, expected: dict, substitutions: dict[str, str] | None = None
) -> list[dict]:
    feed = services.activity_store.list_activity_feed(limit=100)
    checks: list[dict] = [_score_activity_integrity(feed)]
    wanted_count = expected.get("activity_count")
    if wanted_count is not None:
        checks.append(
            {
                "check": "activity_count",
                "passed": len(feed) == wanted_count,
                "expected": wanted_count,
                "actual": len(feed),
            }
        )
    feed_by_id = {str(item.get("id")): item for item in feed}
    for label in expected.get("activities", []):
        activity_ids, title_errors = _activity_ids_for_titles(
            services.activity_store, [label["title_contains"]]
        )
        candidates = [feed_by_id[item_id] for item_id in activity_ids if item_id in feed_by_id]
        found = None
        timeline_summaries: list[str] = []
        for candidate in candidates:
            if label.get("status") and candidate["status"] != label["status"]:
                continue
            if label.get("next_action_contains") and label[
                "next_action_contains"
            ].casefold() not in (candidate.get("next_action") or "").casefold():
                continue
            parent_fragment = label.get("parent_title_contains")
            if parent_fragment:
                parent = feed_by_id.get(str(candidate.get("parent_id")))
                parent_ids, parent_errors = _activity_ids_for_titles(
                    services.activity_store, [parent_fragment]
                )
                if parent is None or parent_errors or str(parent["id"]) not in parent_ids:
                    continue
            if (
                label.get("min_timeline_actions", 0)
                or label.get("timeline_text_contains")
            ):
                detail = services.activity_store.get_activity_thread(str(candidate["id"]))
                summaries = [str(action.get("summary") or "") for action in detail["actions"]]
                if len(detail["actions"]) < label.get("min_timeline_actions", 0):
                    continue
                timeline = "\n".join(summaries).casefold()
                if not all(
                    phrase.casefold() in timeline
                    for phrase in label.get("timeline_text_contains", [])
                ):
                    continue
                timeline_summaries = summaries
            found = candidate
            break
        ok = found is not None and not title_errors
        if found is None and candidates:
            found = candidates[0]
        checks.append(
            {
                "check": f"activity:{label['title_contains']}",
                "passed": bool(ok),
                "expected": label,
                "title_resolution_errors": title_errors,
                "actual": None
                if found is None
                else {
                    "id": str(found["id"]),
                    "title": found["title"],
                    "status": found["status"],
                    "next_action": found.get("next_action"),
                    "parent_id": found.get("parent_id"),
                    "parent_title": (
                        feed_by_id.get(str(found.get("parent_id")), {}).get("title")
                        if found.get("parent_id")
                        else None
                    ),
                    "timeline_summaries": timeline_summaries,
                },
            }
        )
    records = services.policy_service.list_policy_records()
    if "pending_policy_count" in expected:
        checks.append(
            {
                "check": "pending_policy_count",
                "passed": sum(not item.approved for item in records) == expected["pending_policy_count"],
                "expected": expected["pending_policy_count"],
                "actual": sum(not item.approved for item in records),
            }
        )
    for label in expected.get("policies", []):
        ok = any(
            label["name_contains"].casefold() in item.name.casefold()
            and item.approved is label["approved"]
            for item in records
        )
        checks.append(
            {
                "check": f"policy:{label['name_contains']}:{label['approved']}",
                "passed": ok,
                "actual": [
                    {"id": item.id, "approved": item.approved}
                    for item in records
                    if label["name_contains"].casefold() in item.name.casefold()
                ],
            }
        )
    if "policy_count" in expected:
        checks.append(
            {
                "check": "policy_count",
                "passed": len(records) == expected["policy_count"],
                "expected": expected["policy_count"],
                "actual": len(records),
            }
        )
    policy_text = "\n".join(f"{item.name} {item.criteria}" for item in records).casefold()
    for phrase in expected.get("policy_text_contains", []):
        checks.append(
            {
                "check": f"policy_text:{phrase}",
                "passed": phrase.casefold() in policy_text,
            }
        )
    for template in expected.get("deleted_policy_ids", []):
        policy_id = _interpolate(template, substitutions or {})
        query = (Path(__file__).parent / "sql" / "policy_exists.sql").read_text(encoding="utf-8")
        with psycopg.connect(services.context.database_url) as connection:
            row = connection.execute(query, {"policy_id": policy_id}).fetchone()
        exists = bool(row[0])
        checks.append(
            {
                "check": f"policy_absent_from_database:{policy_id}",
                "passed": not exists,
                "expected": "no lisa.policies row",
                "actual": "present" if exists else "absent",
            }
        )
    return checks


def _activity_snapshot(
    activity_store,
    title_fragments: list[str],
    *,
    existing_snapshot: dict[str, list[dict]] | None = None,
) -> dict[str, list[dict]]:
    """Capture user-visible lifecycle state and timeline for named activities."""
    feed = activity_store.list_activity_feed(limit=100)
    snapshots: dict[str, list[dict]] = {}
    for fragment in title_fragments:
        if existing_snapshot is None:
            matches = [
                item for item in feed
                if fragment.casefold() in str(item.get("title", "")).casefold()
            ]
        else:
            candidate_ids = {
                item["id"] for item in existing_snapshot.get(fragment, [])
            }
            matches = [item for item in feed if str(item.get("id")) in candidate_ids]
        snapshots[fragment] = []
        for item in sorted(matches, key=lambda row: str(row.get("id", ""))):
            detail = activity_store.get_activity_thread(str(item["id"]))
            snapshots[fragment].append(
                {
                    "id": str(item.get("id")),
                    "title": item.get("title"),
                    "status": item.get("status"),
                    "summary": item.get("summary"),
                    "next_action": item.get("next_action"),
                    "details_json": item.get("details_json"),
                    "actions": [
                        {
                            key: action.get(key)
                            for key in (
                                "seq", "event_type", "title", "summary", "actor",
                                "reference_type", "reference_id", "status",
                            )
                        }
                        for action in detail.get("actions", [])
                    ],
                }
            )
    return snapshots


def _compare_activity_snapshots(before: dict[str, list[dict]], after: dict[str, list[dict]]) -> dict:
    missing = [title for title, items in before.items() if not items]
    passed = not missing and before == after
    return {
        "check": "unchanged_activities:" + ",".join(before),
        "passed": passed,
        "missing_before": missing,
        "before": before,
        "after": after,
    }


def _score_trajectory_links(activity_store, result: dict, previous_message_ids: set[str]) -> dict:
    """Check shared message links and per-request runtime-event links."""
    from lisa.runtime import activity_events_for_target

    activity_ids = result.get("chat_activity_ids", [])
    activity_ids = [item for item in activity_ids if isinstance(item, str)]
    message_ids = {
        str(message.get("id")) for message in result["messages"]
        if str(message.get("id")) not in previous_message_ids
        and message.get("role") in {"user", "assistant"}
    }
    missing: list[tuple[str, str, str]] = []
    unexpected: list[tuple[str, str, str]] = []
    malformed_details: list[dict[str, str]] = []
    turn_event_ids = {
        str(item["id"]) for item in result["activities"] if item.get("id")
    }
    for activity_id in activity_ids:
        detail = activity_store.get_activity_thread(activity_id)
        actions = detail.get("actions")
        if not isinstance(actions, list) or not actions:
            malformed_details.append({"activity_id": activity_id, "field": "actions"})
        else:
            sequences = [item.get("seq") for item in actions]
            if (
                any(not isinstance(sequence, int) for sequence in sequences)
                or sequences != sorted(set(sequences))
            ):
                malformed_details.append({"activity_id": activity_id, "field": "action_order"})
            for action in actions:
                for field in ("event_type", "title", "created_at"):
                    if not action.get(field):
                        malformed_details.append({
                            "activity_id": activity_id,
                            "field": f"action.{field}",
                        })
            action_references = {
                (item.get("reference_type"), item.get("reference_id"))
                for item in actions
                if item.get("reference_type") and item.get("reference_id")
            }
            if len(activity_ids) == 1:
                # A shared multi-activity turn intentionally omits duplicated
                # transcript actions from each thread projection. The message
                # references are still validated below; only their appearance
                # as task-scoped thread actions is suppressed.
                for message_id in message_ids:
                    if ("message", message_id) not in action_references:
                        missing.append((activity_id, "action_message", message_id))
        references = {
            (item["reference_type"], item["reference_id"])
            for item in detail.get("references", [])
        }
        missing.extend(
            (activity_id, "message", item_id)
            for item_id in message_ids
            if ("message", item_id) not in references
        )
        scoped_events = activity_events_for_target(
            result["activities"], activity_ids, activity_id,
        )
        scoped_event_ids = {
            str(item["id"]) for item in scoped_events if item.get("id")
        }
        missing.extend(
            (activity_id, "runtime_event", str(item["id"]))
            for item in scoped_events
            if item.get("id") and ("runtime_event", str(item["id"])) not in references
        )
        action_references = {
            (item.get("reference_type"), item.get("reference_id"))
            for item in actions or []
            if item.get("reference_type") and item.get("reference_id")
        }
        missing.extend(
            (activity_id, "action_runtime_event", str(item["id"]))
            for item in scoped_events
            if item.get("id")
            and ("runtime_event", str(item["id"])) not in action_references
        )
        unexpected.extend(
            (activity_id, "runtime_event", reference_id)
            for reference_type, reference_id in references
            if reference_type == "runtime_event"
            and reference_id in turn_event_ids
            and reference_id not in scoped_event_ids
        )
        unexpected.extend(
            (activity_id, "action_runtime_event", str(reference_id))
            for reference_type, reference_id in action_references
            if reference_type == "runtime_event"
            and reference_id in turn_event_ids
            and reference_id not in scoped_event_ids
        )
    return {
        "check": "activity_trajectory_links_and_scopes",
        "passed": bool(activity_ids) and not missing and not unexpected and not malformed_details,
        "activity_ids": activity_ids,
        "missing_references": missing,
        "unexpected_references": unexpected,
        "malformed_details": malformed_details,
    }


def _score_activity_integrity(feed: list[dict]) -> dict:
    """Reject feed rows that cannot render a stable, useful activity card."""
    allowed_statuses = {"pending", "blocked", "success", "error", "stopped"}
    required = ("id", "kind", "title", "status", "created_at", "updated_at")
    issues: list[dict[str, str]] = []
    for index, activity in enumerate(feed):
        for field in required:
            if not activity.get(field):
                issues.append({"row": str(index), "field": field})
        try:
            UUID(str(activity.get("id", "")))
        except (ValueError, TypeError, AttributeError):
            issues.append({"row": str(index), "field": "id"})
        if activity.get("status") and activity["status"] not in allowed_statuses:
            issues.append({"row": str(index), "field": "status"})
        if not isinstance(activity.get("details_json"), dict):
            issues.append({"row": str(index), "field": "details_json"})
        if activity.get("status") in {"success", "error", "stopped"} and not activity.get("finished_at"):
            issues.append({"row": str(index), "field": "finished_at"})
        if activity.get("status") == "blocked" and not activity.get("next_action"):
            issues.append({"row": str(index), "field": "next_action"})
    return {
        "check": "activity_feed_well_formed",
        "passed": not issues,
        "activity_count": len(feed),
        "issues": issues,
    }


def _score_turn(
    turn: ActivityTurn, result: dict, substitutions: dict[str, str], index: int
) -> dict:
    calls = [item for item in result["activities"] if item.get("kind") == "tool"]
    tool_names = [item.get("tool_name", "") for item in calls]
    assistant = [
        item.get("text", "") for item in result["messages"] if item.get("role") == "assistant"
    ]
    reply = assistant[-1] if assistant else ""
    required = turn.require_tools
    forbidden = turn.forbid_tools
    action_checks = []
    for name in required:
        action_checks.append({"check": f"required_tool:{name}", "passed": name in tool_names})
    for name in forbidden:
        action_checks.append({"check": f"forbidden_tool:{name}", "passed": name not in tool_names})
    if turn.tool_order:
        ordered_activity_tools = [name for name in tool_names if name not in _TOOL_SETUP_CALLS]
        ordered_names = iter(ordered_activity_tools)
        passed_order = all(
            any(actual == wanted for actual in ordered_names) for wanted in turn.tool_order
        )
        action_checks.append(
            {
                "check": "tool_order:" + "->".join(turn.tool_order),
                "passed": passed_order,
                "actual": ordered_activity_tools,
                "ignored_setup_calls": [name for name in tool_names if name in _TOOL_SETUP_CALLS],
            }
        )
    for name, expected_count in turn.tool_call_counts.items():
        actual_count = tool_names.count(name)
        action_checks.append(
            {
                "check": f"tool_call_count:{name}",
                "passed": actual_count == expected_count,
                "expected": expected_count,
                "actual": actual_count,
            }
        )
    arguments = json.dumps([item.get("arguments", {}) for item in calls], ensure_ascii=False)
    for template in turn.tool_arguments_must_include:
        phrase = _interpolate(template, substitutions)
        action_checks.append(
            {
                "check": f"tool_arguments_contain:{phrase}",
                "passed": phrase.casefold() in arguments.casefold(),
            }
        )
    for template in turn.tool_arguments_must_not_include:
        phrase = _interpolate(template, substitutions)
        action_checks.append(
            {
                "check": f"tool_arguments_exclude:{phrase}",
                "passed": phrase.casefold() not in arguments.casefold(),
            }
        )
    for title_fragment, templates in turn.resolver_summary_must_include_by_title.items():
        matching_resolver_summaries = []
        for call in calls:
            if call.get("tool_name") != "resolve_chat_activity":
                continue
            resolver_arguments = _tool_arguments(call)
            title = str(resolver_arguments.get("title", ""))
            if title_fragment.casefold() in title.casefold():
                matching_resolver_summaries.append(
                    str(resolver_arguments.get("summary", ""))
                )
        required_phrases = [
            _interpolate(template, substitutions) for template in templates
        ]
        action_checks.append(
            {
                "check": f"resolver_summary_content:{title_fragment}",
                "passed": len(matching_resolver_summaries) == 1
                and all(
                    phrase.casefold() in matching_resolver_summaries[0].casefold()
                    for phrase in required_phrases
                ),
                "matched_resolver_calls": len(matching_resolver_summaries),
                "required_phrases": required_phrases,
            }
        )
    for title_fragment, templates in turn.resolver_summary_must_not_include_by_title.items():
        matching_resolver_summaries = []
        for call in calls:
            if call.get("tool_name") != "resolve_chat_activity":
                continue
            resolver_arguments = _tool_arguments(call)
            title = str(resolver_arguments.get("title", ""))
            if title_fragment.casefold() in title.casefold():
                matching_resolver_summaries.append(
                    str(resolver_arguments.get("summary", ""))
                )
        forbidden_phrases = [
            _interpolate(template, substitutions) for template in templates
        ]
        action_checks.append(
            {
                "check": f"resolver_summary_excludes:{title_fragment}",
                "passed": len(matching_resolver_summaries) == 1
                and all(
                    phrase.casefold() not in matching_resolver_summaries[0].casefold()
                    for phrase in forbidden_phrases
                ),
                "matched_resolver_calls": len(matching_resolver_summaries),
                "forbidden_phrases": forbidden_phrases,
            }
        )
    for title_fragment, templates in turn.resolver_title_must_include_by_title.items():
        matching_resolver_titles = []
        for call in calls:
            if call.get("tool_name") != "resolve_chat_activity":
                continue
            resolver_arguments = _tool_arguments(call)
            title = str(resolver_arguments.get("title", ""))
            if title_fragment.casefold() in title.casefold():
                matching_resolver_titles.append(title)
        required_phrases = [
            _interpolate(template, substitutions) for template in templates
        ]
        action_checks.append(
            {
                "check": f"resolver_title_content:{title_fragment}",
                "passed": len(matching_resolver_titles) == 1
                and all(
                    phrase.casefold() in matching_resolver_titles[0].casefold()
                    for phrase in required_phrases
                ),
                "matched_resolver_calls": len(matching_resolver_titles),
                "required_phrases": required_phrases,
            }
        )
    database_query_arguments = json.dumps(
        [
            item.get("arguments", {})
            for item in calls
            if item.get("tool_name") == "lisa_db"
        ],
        ensure_ascii=False,
    )
    for template in turn.tool_query_arguments_must_not_include:
        phrase = _interpolate(template, substitutions)
        action_checks.append(
            {
                "check": f"tool_query_arguments_exclude:{phrase}",
                "passed": phrase.casefold() not in database_query_arguments.casefold(),
            }
        )
    actual_tool_errors = [
        str(item.get("tool_name"))
        for item in result["activities"]
        if item.get("kind") == "evidence" and item.get("is_error")
    ]
    action_checks.append(
        {
            "check": "tool_errors",
            "passed": sorted(actual_tool_errors) == sorted(turn.expected_tool_errors),
            "expected": turn.expected_tool_errors,
            "actual": actual_tool_errors,
        }
    )
    database_queries = []
    for call in calls:
        if call.get("tool_name") != "lisa_db":
            continue
        arguments = call.get("arguments", {})
        if isinstance(arguments, str):
            database_queries.append(arguments)
        elif isinstance(arguments, dict):
            database_queries.extend(value for value in arguments.values() if isinstance(value, str))
    for session_id in turn.tool_queries_must_filter_sessions:
        filter_pattern = re.compile(
            r"\bwhere\b.*?\bsession_id\s*=\s*(['\"]?)"
            + re.escape(session_id)
            + r"\1(?=\W|$)",
            re.IGNORECASE | re.DOTALL,
        )
        action_checks.append(
            {
                "check": f"session_filter:{session_id}",
                "passed": any(filter_pattern.search(query) for query in database_queries),
            }
        )
    reply_checks = []
    for phrase in turn.reply_must_include:
        resolved = _interpolate(phrase, substitutions)
        reply_checks.append(
            {
                "check": f"reply_contains:{resolved}",
                "passed": resolved.casefold() in reply.casefold(),
            }
        )
    if turn.reply_must_match_exactly_one_of:
        alternatives = [
            {
                "must_include": [
                    _interpolate(phrase, substitutions) for phrase in option.must_include
                ],
                "must_not_include": [
                    _interpolate(phrase, substitutions)
                    for phrase in option.must_not_include
                ],
            }
            for option in turn.reply_must_match_exactly_one_of
        ]
        matched_options = [
            option for option in alternatives
                if all(
                    phrase.casefold() in reply.casefold()
                    for phrase in option["must_include"]
                )
                and all(
                    phrase.casefold() not in reply.casefold()
                    for phrase in option["must_not_include"]
                )
        ]
        contradiction_matches = [
            phrase
            for option in alternatives
            for phrase in option["must_not_include"]
            if phrase.casefold() in reply.casefold()
        ]
        reply_checks.append(
            {
                "check": "reply_matches_exactly_one_of",
                "passed": len(matched_options) == 1 and not contradiction_matches,
                "alternatives": alternatives,
                "matched_alternatives": matched_options,
                "contradictions_found": contradiction_matches,
            }
        )
    for quote in turn.reply_must_quote:
        reply_checks.append(
            {
                "check": f"reply_quotes_exactly:{quote}",
                "passed": _verbatim_quote_matches(reply, quote),
                "required_for_answer_success": _turn_requires_verbatim_answer(turn.text),
            }
        )
    for phrase in turn.reply_must_not_include:
        reply_checks.append(
            {
                "check": f"reply_excludes:{phrase}",
                "passed": phrase.casefold() not in reply.casefold(),
            }
        )
    if turn.expects_abstention:
        reply_checks.append(
            {
                "check": "expected_abstention",
                "passed": _reply_indicates_abstention(
                    reply, forbidden_values=turn.reply_must_not_include,
                ),
            }
        )
    return {
        "turn": index,
        "chat": turn.chat,
        "tools": tool_names,
        "tool_calls": [
            {"name": item.get("tool_name"), "arguments": item.get("arguments")} for item in calls
        ],
        "tool_checks": action_checks,
        "reply": reply,
        "communication_checks": reply_checks,
    }


def _reply_indicates_abstention(
    reply: str, *, forbidden_values: list[str] | None = None,
) -> bool:
    """Recognize abstention only when the reply withholds labeled forbidden facts."""
    normalized = " ".join(reply.casefold().split())
    if any(
        value.strip().casefold() in normalized
        for value in (forbidden_values or [])
        if value.strip()
    ):
        return False
    abstention_markers = (
        "not enough information",
        "insufficient information",
        "cannot determine",
        "can't determine",
        "cannot tell",
        "can't tell",
        "not in the recorded history",
        "the activity does not record",
        "did not record",
        "didn't record",
        "does not record",
        "doesn't record",
        "not recorded",
        "was not recorded",
        "wasn't recorded",
        "never recorded",
        "not in this activity",
        "not present",
        "no record",
        "i don't know",
        "i will not guess",
        "i won't guess",
        "which activity do you mean",
        "which one do you mean",
    )
    return (
        any(marker in normalized for marker in abstention_markers)
        or bool(re.search(r"\binformation\b.{0,50}\bnot enough\b", normalized))
        or bool(re.search(r"\b(?:no|none)\b.{0,80}\b(?:recorded|available|present)\b", normalized))
    )


def _decode_json_value(value: Any) -> Any:
    """Decode structured MCP observations whether returned directly or as text."""
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        decoder = json.JSONDecoder()
        for index, character in enumerate(value):
            if character not in "{[":
                continue
            try:
                decoded, _ = decoder.raw_decode(value[index:])
                return decoded
            except json.JSONDecodeError:
                continue
    return value


def _find_activity_memory_payload(value: Any) -> dict | None:
    decoded = _decode_json_value(value)
    if isinstance(decoded, dict):
        if {
            "activity_id", "status", "freshness_watermark"
        } <= set(decoded):
            return decoded
        for child in decoded.values():
            result = _find_activity_memory_payload(child)
            if result is not None:
                return result
    elif isinstance(decoded, list):
        for child in decoded:
            result = _find_activity_memory_payload(child)
            if result is not None:
                return result
    return None


def _tool_arguments(call: dict) -> dict:
    value = _decode_json_value(call.get("arguments"))
    return value if isinstance(value, dict) else {}


def _activity_ids_for_titles(activity_store, titles: list[str]) -> tuple[set[str], list[str]]:
    ids: set[str] = set()
    errors: list[str] = []
    for fragment in titles:
        # Search may match the private resolver title, but feed results expose
        # only the neutral title assigned to a shared-turn activity.
        matches = activity_store.list_activity_feed(limit=100, query=fragment)
        if not matches:
            errors.append(f"{fragment}: no matching activity")
        else:
            ids.update(str(item["id"]) for item in matches)
    return ids, errors


def _mark_activity_memory_capture_truncated(activity: dict[str, Any]) -> None:
    activity["detail"] = json.dumps({"capture_truncated": True})
    activity["memory_result_capture_truncated"] = True


def _restore_complete_activity_memory_results(
    result: dict[str, Any], conversation_directory: Path, conversation_id: str,
) -> None:
    """Read full bounded memory observations from OpenHands events for evidence scoring.

    ``conversation_content`` intentionally caps ordinary tool evidence at 4,000
    characters for activity projections. That display cap can cut a valid JSON
    memory result before its source references. The benchmark reads the original
    OpenHands observation by event ID, with a 32 KiB bound, so evidence checks
    inspect what the model actually received rather than the shortened projection.
    """
    try:
        event_directory = Path(conversation_directory) / UUID(conversation_id).hex / "events"
    except (TypeError, ValueError, AttributeError):
        return
    if not event_directory.is_dir():
        return
    for activity in result.get("activities", []):
        if (
            activity.get("kind") != "evidence"
            or activity.get("tool_name") != "load_activity_memory"
        ):
            continue
        try:
            event_id = str(UUID(str(activity.get("id", ""))))
        except (TypeError, ValueError, AttributeError):
            continue
        matches = list(event_directory.glob(f"event-*-{event_id}.json"))
        if len(matches) != 1:
            continue
        event_path = matches[0]
        try:
            # Reject oversized envelopes before parsing, and bound the actual
            # read too so a concurrent file replacement cannot bypass the cap.
            if event_path.stat().st_size > _ACTIVITY_MEMORY_EVENT_FILE_LIMIT_BYTES:
                _mark_activity_memory_capture_truncated(activity)
                continue
            with event_path.open("rb") as event_file:
                raw_event = event_file.read(_ACTIVITY_MEMORY_EVENT_FILE_LIMIT_BYTES + 1)
            if len(raw_event) > _ACTIVITY_MEMORY_EVENT_FILE_LIMIT_BYTES:
                _mark_activity_memory_capture_truncated(activity)
                continue
            event = json.loads(raw_event.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if (
            not isinstance(event, dict)
            or event.get("id") != event_id
            or event.get("tool_name") != "load_activity_memory"
        ):
            continue
        observation = event.get("observation")
        content = observation.get("content") if isinstance(observation, dict) else None
        if not isinstance(content, list):
            continue
        detail = "\n".join(
            item["text"]
            for item in content
            if isinstance(item, dict) and isinstance(item.get("text"), str)
        )
        if len(detail.encode("utf-8")) > _ACTIVITY_MEMORY_RESULT_CAPTURE_LIMIT_BYTES:
            _mark_activity_memory_capture_truncated(activity)
            continue
        activity["detail"] = detail


def _score_activity_memory_turn(turn: ActivityTurn, result: dict, activity_store) -> list[dict]:
    thread_calls = [
        item for item in result.get("activities", [])
        if item.get("kind") == "tool" and item.get("tool_name") == "get_activity_thread"
    ]
    thread_results = [
        item for item in result.get("activities", [])
        if item.get("kind") == "evidence" and item.get("tool_name") == "get_activity_thread"
    ]
    if turn.memory_text_must_not_include:
        thread_failures = []
        for call in thread_calls:
            observation = next(
                (
                    item for item in thread_results
                    if str(item.get("tool_call_id")) == str(call.get("tool_call_id"))
                ),
                None,
            )
            if observation is None:
                thread_failures.append({"failure": "thread result is missing"})
                continue
            detail = str(observation.get("detail", "")).casefold()
            for forbidden in turn.memory_text_must_not_include:
                if forbidden.casefold() in detail:
                    thread_failures.append({
                        "failure": "cross-activity text leak",
                        "text": forbidden,
                        "activity_id": _tool_arguments(call).get("activity_id"),
                    })
        thread_check = {
            "check": "activity_thread_cross_task_isolation",
            "passed": not thread_failures,
            "thread_reads": len(thread_calls),
            "failures": thread_failures,
        }
    else:
        thread_check = None
    calls = [
        item for item in result.get("activities", [])
        if item.get("kind") == "tool" and item.get("tool_name") == "load_activity_memory"
    ]
    checks: list[dict] = [thread_check] if thread_check is not None else []
    if turn.forbid_activity_memory_load:
        checks.append(
            {
                "check": "activity_memory_forbidden",
                "passed": not calls,
                "actual_calls": len(calls),
            }
        )
    requested_ids = {
        str(_tool_arguments(call).get("activity_id", "")) for call in calls
    }
    if turn.memory_text_must_not_include and activity_store is not None:
        feed = activity_store.list_activity_feed(limit=100)
        selected_feed = [item for item in feed if str(item.get("id")) in requested_ids]
        feed_text = str(selected_feed).casefold()
        feed_failures = [
            phrase
            for phrase in turn.memory_text_must_not_include
            if phrase.casefold() in feed_text
        ]
        checks.append(
            {
                "check": "activity_feed_cross_task_isolation",
                "passed": not feed_failures,
                "activity_ids": sorted(requested_ids),
                "failures": feed_failures,
            }
        )
    if turn.memory_activity_title_must_not_contain:
        forbidden_ids, forbidden_title_errors = _activity_ids_for_titles(
            activity_store, turn.memory_activity_title_must_not_contain
        )
        checks.append(
            {
                "check": "activity_memory_excludes_other_context",
                "passed": not forbidden_title_errors
                and not requested_ids.intersection(forbidden_ids),
                "forbidden_activity_ids": sorted(forbidden_ids),
                "requested_activity_ids": sorted(requested_ids),
                "title_resolution_errors": forbidden_title_errors,
            }
        )
    if not turn.memory_activity_title_contains:
        return checks

    expected_ids, title_errors = _activity_ids_for_titles(
        activity_store, turn.memory_activity_title_contains
    )
    checks.append(
        {
            "check": "activity_memory_selection",
            "passed": not title_errors and requested_ids == expected_ids,
            "expected_activity_ids": sorted(expected_ids),
            "requested_activity_ids": sorted(requested_ids),
            "title_resolution_errors": title_errors,
        }
    )

    result_events = [
        item for item in result.get("activities", [])
        if item.get("kind") == "evidence" and item.get("tool_name") == "load_activity_memory"
    ]
    valid = bool(calls)
    failures: list[dict] = []
    memory_results: list[dict] = []
    if not calls:
        failures.append({"failure": "expected activity memory load did not occur"})
    for call in calls:
        arguments = _tool_arguments(call)
        activity_id = str(arguments.get("activity_id", ""))
        observation = next(
            (
                item for item in result_events
                if str(item.get("tool_call_id")) == str(call.get("tool_call_id"))
            ),
            None,
        )
        if observation is not None and observation.get("memory_result_capture_truncated"):
            valid = False
            failures.append(
                {
                    "activity_id": activity_id,
                    "failure": "tool result exceeds benchmark evidence capture limit",
                }
            )
            continue
        memory = _find_activity_memory_payload(observation.get("detail")) if observation else None
        if memory is None or memory.get("activity_id") != activity_id:
            valid = False
            failures.append({"activity_id": activity_id, "failure": "missing or mismatched tool result"})
            continue
        memory_results.append(
            {
                "activity_id": activity_id,
                "status": memory.get("status"),
                "event_count": len(memory.get("events") or []),
                "snapshot_watermark": (memory.get("freshness_watermark") or {}).get(
                    "snapshot_event_seq"
                ),
                "current_watermark": (memory.get("freshness_watermark") or {}).get(
                    "current_event_seq"
                ),
            }
        )
        freshness = memory.get("freshness_watermark") or {}
        current_watermark = freshness.get("current_event_seq")
        snapshot_watermark = freshness.get("snapshot_event_seq")
        if (
            not isinstance(current_watermark, int)
            or current_watermark < 0
            or snapshot_watermark is not None
            and (
                not isinstance(snapshot_watermark, int)
                or snapshot_watermark < 0
                or snapshot_watermark > current_watermark
            )
        ):
            valid = False
            failures.append({"activity_id": activity_id, "failure": "invalid freshness watermark"})
            continue
        thread = activity_store.get_activity_thread(activity_id) or {}
        actions = thread.get("actions") or []
        available = {
            (action.get("seq"), action.get("reference_type"), action.get("reference_id"))
            for action in actions
        }
        available.update((action.get("seq"), "activity_event", str(action.get("seq"))) for action in actions)
        refs = [
            *(memory.get("source_event_refs") or []),
            *((memory.get("snapshot") or {}).get("source_event_refs") or []),
        ]
        allowed_snapshot_watermark = (
            snapshot_watermark if isinstance(snapshot_watermark, int) else -1
        )
        for reference in refs:
            key = (
                reference.get("sequence"),
                reference.get("reference_type"),
                reference.get("reference_id"),
            )
            if (
                not isinstance(key[0], int)
                or key[0] > allowed_snapshot_watermark
                or key not in available
            ):
                valid = False
                failures.append({"activity_id": activity_id, "failure": "unresolved evidence reference", "reference": reference})
        for event in memory.get("events") or []:
            if (
                event.get("activity_id") != activity_id
                or not isinstance(event.get("seq"), int)
                or event.get("seq") > current_watermark
                or event.get("seq") <= (snapshot_watermark or 0)
                or not any(
                    action.get("seq") == event.get("seq") for action in actions
                )
            ):
                valid = False
                failures.append({"activity_id": activity_id, "failure": "event evidence is outside the requested activity"})
            if event.get("reference_type") and event.get("reference_id") and (
                event.get("seq"), event.get("reference_type"), event.get("reference_id")
            ) not in available:
                valid = False
                failures.append({"activity_id": activity_id, "failure": "event reference does not resolve", "event_seq": event.get("seq")})
        memory_text = "\n".join(
            [str((memory.get("snapshot") or {}).get("summary") or "")]
            + [str(event.get("summary") or "") for event in memory.get("events") or []]
        ).casefold()
        for forbidden in turn.memory_text_must_not_include:
            if forbidden.casefold() in memory_text:
                valid = False
                failures.append({"activity_id": activity_id, "failure": "cross-activity text leak", "text": forbidden})
    if len(result_events) != len(calls):
        valid = False
        failures.append({"failure": "load result count does not match tool call count"})
    checks.append(
        {
            "check": "activity_memory_evidence_refs",
            "passed": valid,
            "failures": failures,
            "memory_results": memory_results,
        }
    )
    return checks


def _score_resolved_activity_scope(turn: ActivityTurn, result: dict, activity_store) -> dict | None:
    actual_ids = {
        str(item) for item in result.get("chat_activity_ids", []) if isinstance(item, str)
    }
    write_tools = {
        "update_activity",
        "set_activity_progress",
        "complete_activity",
        "set_activity_section",
        "link_activity_reference",
    }
    write_targets = [
        str(_tool_arguments(call).get("activity_id"))
        for call in result.get("activities", [])
        if call.get("kind") == "tool"
        and call.get("tool_name") in write_tools
        and _tool_arguments(call).get("activity_id")
    ]
    if turn.resolved_activity_title_contains:
        expected_ids, errors = _activity_ids_for_titles(
            activity_store, turn.resolved_activity_title_contains
        )
        allowed_write_ids = expected_ids
    else:
        # Recall turns do not always have a title rubric. In those cases the
        # activity resolved for this turn is the authorization boundary.
        expected_ids = actual_ids
        errors = []
        allowed_write_ids = actual_ids
        if not actual_ids and not write_targets:
            return None
    wrong_write_targets = sorted(set(write_targets) - allowed_write_ids)
    return {
        "check": "resolved_activity_scope",
        "passed": (
            not errors
            and expected_ids == actual_ids
            and (not write_targets or bool(allowed_write_ids))
            and not wrong_write_targets
        ),
        "expected_activity_ids": sorted(expected_ids),
        "resolved_activity_ids": sorted(actual_ids),
        "activity_write_targets": write_targets,
        "wrong_write_targets": wrong_write_targets,
        "title_resolution_errors": errors,
    }


def _transcript_excludes_checkpoint_summaries(
    messages: list[dict], summaries: list[str]
) -> bool:
    """Check that private checkpoint text never appears in Lisa's visible transcript."""
    if not messages or any(item.get("role") not in {"user", "assistant"} for item in messages):
        return False
    visible_text = [str(item.get("text", "")) for item in messages]
    return all(
        not summary.strip() or all(summary not in text for text in visible_text)
        for summary in summaries
    )


def _activity_namespace_reloaded_before_use(turn: dict) -> bool:
    """Require the activity namespace to be loaded before an activity tool call."""
    calls = turn.get("tool_calls", [])
    activity_tools = {
        "find_activity",
        "get_activity_thread",
        "load_activity_memory",
        "resolve_chat_activity",
        "skip_chat_activity_for_turn",
        "update_activity",
        "set_activity_progress",
        "complete_activity",
        "list_recent_activities",
        "set_activity_section",
        "link_activity_reference",
    }
    first_activity = next(
        (index for index, call in enumerate(calls) if call.get("name") in activity_tools), None
    )
    if first_activity is None:
        return False

    def arguments(call: dict) -> dict:
        value = call.get("arguments")
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                return {}
        return value if isinstance(value, dict) else {}

    return any(
        call.get("name") == "load_tool_namespace"
        and arguments(call).get("namespace") == "activities"
        for call in calls[:first_activity]
    )


def _checkpoint_namespaces_cleared(adoptions: list[dict]) -> bool:
    return bool(adoptions) and all(not item.get("loaded_namespaces") for item in adoptions)


def _checkpoint_cut_is_safe(view, prefix_event_ids: list[str]) -> bool:
    """Require an exact event prefix and a cut allowed by the SDK's view invariants.

    The prefix comparison rules out reordered or stale IDs. The manipulation-index
    check keeps action/result pairs, parallel-call batches, and protected tool loops
    intact according to the OpenHands SDK.
    """
    event_ids = [str(event.id) for event in view.events]
    prefix_ids = [str(event_id) for event_id in prefix_event_ids]
    cut = len(prefix_ids)
    return prefix_ids == event_ids[:cut] and cut in view.manipulation_indices


def _slack_threads_have_distinct_conversations(chat_ids: dict[str, str]) -> bool:
    thread_ids = [
        conversation_id
        for key, conversation_id in chat_ids.items()
        if key.startswith("slack:")
    ]
    return len(thread_ids) >= 2 and len(thread_ids) == len(set(thread_ids))


def _seed_historical_evaluations(services, case: ActivityBenchmarkCase) -> None:
    """Seed labeled history via Lisa repositories and a benchmark-only SQL fixture."""
    if not case.historical_evaluations:
        return
    policy_names = sorted({item.policy_name for item in case.historical_evaluations})
    markdown = "\n\n".join(
        f"## {name}\napproved: yes\n\nBenchmark fixture policy for historical lookup."
        for name in policy_names
    )
    seed_policy_document(services.policy_service,
        markdown,
        "No policy exists yet.",
    )
    policies = {
        rule.name: rule for rule in services.policy_service.approved_rules("historical-agent")
    }
    for item in case.historical_evaluations:
        policy = policies[item.policy_name]
        if item.triage_decision:
            services.activity_store.record_policy_triage_decisions(
                item.session_id,
                [{"id": policy.id, "decision": item.triage_decision}],
            )
        if item.verdict:
            activity = services.activity_store.create_activity(
                title=f"Historical evaluation {item.session_id}",
                source="benchmark",
                kind="policy_evaluation",
            )
            score_id = f"history-{uuid4()}"
            run = services.activity_store.create_evaluation(
                title=f"Historical evaluation {item.session_id}",
                source="benchmark",
                target_agent_name="historical-agent",
                session_id=item.session_id,
                policy_name=policy.name,
                policy_id=policy.id,
                policy_hash=sha256(policy.criteria.encode()).hexdigest(),
                judge_config={"judge_model": "benchmark-fixture"},
                request_key=str(uuid4()),
                activity_id=activity.id,
            )
            fixture_sql = (Path(__file__).parent / "sql" / "complete_historical_evaluation.sql").read_text()
            score_payload = {
                "score_id": score_id,
                "string_value": item.verdict,
                "comment": f"Historical benchmark verdict for {item.session_id}.",
                "timestamp": datetime.now(UTC).isoformat(),
                "source": "activity-benchmark",
            }
            encoded_payload = json.dumps(score_payload, ensure_ascii=False, sort_keys=True)
            with psycopg.connect(services.context.database_url) as connection:
                result = connection.execute(
                    fixture_sql,
                    {
                        "evaluation_id": run.id,
                        "verdict": item.verdict,
                        "score_id": score_id,
                        "outbox_id": str(uuid4()),
                        "score_payload": encoded_payload,
                        "payload_hash": sha256(encoded_payload.encode("utf-8")).hexdigest(),
                    },
                )
                if result.rowcount != 1:
                    raise RuntimeError("Could not seed historical judge verdict")
            if not services.activity_store.finish_activity_without_claim(
                activity.id,
                f"Historical evaluation for {item.session_id} completed with its saved verdict.",
                status="succeeded",
            ):
                raise RuntimeError("Could not finish seeded historical evaluation activity")


def _seed_archived_activity_memories(services, case: ActivityBenchmarkCase) -> None:
    """Create immutable parent fixtures for the archived-parent benchmark cases."""
    if not case.seed_archived_activities:
        return
    from lisa.activity_memory import activity_memory_configuration_fingerprint

    model_name = os.getenv("LISA_MODEL", "gemini/gemini-3.8-flash")
    configuration = activity_memory_configuration_fingerprint(model_name)
    for seed in case.seed_archived_activities:
        activity = services.activity_store.create_activity(
            title=seed.title,
            subtitle=seed.summary[:500],
            details={"request_summary": seed.summary},
            source="activity-memory-benchmark-fixture",
            kind="chat_request",
            activity_type="chat_request",
        )
        services.activity_store.update_activity_presentation(
            activity.id,
            event_type="chat.request",
            event_title=f"Request for {seed.title}",
            event_summary=seed.summary,
        )
        if not services.activity_store.finish_activity_without_claim(
            activity.id,
            "The benchmark fixture is a completed parent activity.",
            status="succeeded",
        ):
            raise RuntimeError("Could not finish the archived activity fixture")
        state = services.activity_store.capture_activity_memory(
            activity.id,
            services.activity_store.get_activity_thread(activity.id)["actions"][-1]["seq"],
            configuration,
        )
        services.activity_store.publish_activity_memory(
            state, seed.summary, terminal=True
        )


def _process_benchmark_checkpoint_job(
    message: dict,
    services,
    conversations,
    workspace: str,
    published_checkpoints: list[str],
    published_activity_memory: list[dict],
) -> None:
    queued = message.get("message", {})
    if queued.get("kind") == "activity_memory":
        from lisa.activity_memory import activity_memory_configuration_fingerprint

        activity_id = str(queued.get("activity_id", ""))
        watermark = queued.get("event_watermark")
        model_name = os.getenv("LISA_MODEL", "gemini/gemini-3.8-flash")
        configuration = activity_memory_configuration_fingerprint(model_name)
        services.conversation_checkpoint_worker._process(message)
        artifact = services.activity_store.get_activity_memory_artifact(
            activity_id, watermark, configuration
        )
        if artifact is not None:
            published_activity_memory.append(
                {
                    "activity_id": activity_id,
                    "event_watermark": watermark,
                    "artifact_id": artifact["id"],
                    "is_terminal": artifact["is_terminal"],
                }
            )
        return

    request_id = str(queued.get("request_id", ""))
    conversation_id = str(queued.get("conversation_id", ""))
    services.conversation_checkpoint_worker._process(message)
    directory = conversations.repository.conversation_directory(workspace, conversation_id)
    checkpoint = conversations.repository.context_repository(directory).read_checkpoint(request_id)
    if checkpoint is not None:
        published_checkpoints.append(request_id)


def _run_case(
    case: ActivityBenchmarkCase,
    database: LisaTestDatabase,
    workspace: str,
    artifact_dir: Path,
    *,
    process_context_checkpoints: bool = False,
) -> dict:
    from lisa.backends.openhands import OpenHandsBackend
    from lisa.bootstrap import build_services
    from lisa.context import WorkspaceContext
    writer_url, reader_url = database.writer_url, database.reader_url
    data_dir = artifact_dir / "workspaces" / case.case_id
    process_context_checkpoints = process_context_checkpoints or case.force_compaction
    force_compaction = process_context_checkpoints and (
        case.force_compaction
        or case.case_id == "main_chat_interleaves_tasks_after_compaction"
    )
    if force_compaction:
        # Test-only limits force real condenser adoption in labeled memory cases.
        os.environ["LISA_CONTEXT_WINDOW_TOKENS"] = "512"
        os.environ["LISA_CONTEXT_CHECKPOINT_TRIGGER_RATIO"] = "0.5"
        os.environ["LISA_CONTEXT_RETAINED_TAIL_TOKENS"] = "128"
    else:
        os.environ.pop("LISA_CONTEXT_WINDOW_TOKENS", None)
        os.environ.pop("LISA_CONTEXT_CHECKPOINT_TRIGGER_RATIO", None)
        os.environ.pop("LISA_CONTEXT_RETAINED_TAIL_TOKENS", None)
    os.environ.update(
        {
            "LISA_WORKSPACE_ID": workspace,
            "LISA_DATA_DIR": str(data_dir),
            "LISA_DATABASE_URL": writer_url,
            "LISA_DATABASE_READ_URL": reader_url,
            "LISA_OWNER_USER_IDS": OWNER,
            "LISA_AGENT_BACKEND": "openhands",
            "LISA_SKILL_DIR": str(SKILLS),
            "LISA_LLM_TRACE_ENABLED": "1",
        }
    )
    context = WorkspaceContext(
        workspace=workspace,
        data_dir=data_dir,
        database_url=writer_url,
        database_read_url=reader_url,
        owner_user_ids=frozenset({OWNER}),
        actor_id=OWNER,
    )
    services = build_services(context)
    _seed_archived_activity_memories(services, case)
    _seed_historical_evaluations(services, case)
    conversations = services.conversations
    main = asyncio.run(conversations.main_conversation(workspace))
    chat_ids = {"main": main["conversation_id"]}
    substitutions: dict[str, str] = {}
    turns = []
    checkpoint_adoptions: list[dict] = []
    checkpoint_cuts: list[dict] = []
    deferred_activity_jobs: list[dict] = []
    published_activity_memory: list[dict] = []
    current_turn_index = -1
    if process_context_checkpoints:
        from unittest.mock import patch

        from lisa.backends import openhands as openhands_backend
        from lisa.conversation_context import ConversationCheckpointCondenser

        original_prepare = ConversationCheckpointCondenser.prepare_for_turn
        original_checkpoint_request = openhands_backend.prepare_checkpoint_request

        def record_checkpoint_adoption(condenser, events):
            adopted = original_prepare(condenser, events)
            if adopted:
                context_repository = services.conversation_repository.context_repository(
                    Path(condenser.directory)
                )
                checkpoint = context_repository.current_checkpoint(
                    condenser.configuration_fingerprint
                )
                checkpoint_adoptions.append(
                    {
                        "request_id": checkpoint.get("request_id") if checkpoint else None,
                        "event_count": len(events),
                        "turn_index": current_turn_index,
                        "summary": checkpoint.get("summary") if checkpoint else None,
                        "loaded_namespaces": sorted(context_repository.loaded_namespaces()),
                    }
                )
            return adopted

        def record_checkpoint_cut(view, llm, repository, **kwargs):
            request = original_checkpoint_request(view, llm, repository, **kwargs)
            if request is not None:
                request_id, payload = request
                prefix_ids = payload.get("prefix_event_ids", [])
                event_ids = [str(event.id) for event in view.events]
                prefix_ids = [str(event_id) for event_id in prefix_ids]
                # Check against the exact view used to capture this immutable prefix;
                # later events can change the SDK's valid boundaries, especially for
                # thought-enabled tool loops.
                checkpoint_cuts.append(
                    {
                        "request_id": request_id,
                        "prefix_event_count": len(prefix_ids),
                        "event_count": len(event_ids),
                        "matches_event_prefix": prefix_ids == event_ids[: len(prefix_ids)],
                        "valid_sdk_boundary": len(prefix_ids) in view.manipulation_indices,
                        "passed": _checkpoint_cut_is_safe(view, prefix_ids),
                    }
                )
            return request

        prepare_patch = patch.object(
            ConversationCheckpointCondenser, "prepare_for_turn", record_checkpoint_adoption
        )
        checkpoint_patch = patch.object(
            openhands_backend, "prepare_checkpoint_request", record_checkpoint_cut
        )
    else:
        prepare_patch = None
        checkpoint_patch = None
    published_checkpoints: list[str] = []
    context_checks: list[dict] = []
    with ExitStack() as patches:
        if prepare_patch is not None:
            patches.enter_context(prepare_patch)
            patches.enter_context(checkpoint_patch)
        for index, turn in enumerate(case.turns):
            current_turn_index = index
            if turn.chat == "main":
                conversation_id = chat_ids["main"]
                source, channel, thread = "web", None, None
            else:
                key = f"slack:{turn.channel}:{turn.thread}"
                if key not in chat_ids:
                    conversation_id = str(uuid4())
                    chat_ids[key] = conversation_id
                    conversations.create_conversation(workspace, conversation_id)
                conversation_id = chat_ids[key]
                source, channel, thread = "slack", turn.channel, turn.thread
            text = _interpolate(turn.text, substitutions)
            previous_message_ids = {
                str(message.get("id"))
                for message in conversations.get_conversation(
                    workspace, conversation_id
                ).get("messages", [])
            }
            unchanged_snapshot = _activity_snapshot(
                services.activity_store, turn.unchanged_activities
            )
            result = asyncio.run(
                conversations.run_user_turn(
                    conversation_id,
                    workspace,
                    text,
                    run_turn=OpenHandsBackend().run_turn,
                    turn_source=source,
                    actor_id=OWNER if turn.actor == "owner" else OUTSIDER,
                    slack_channel_id=channel,
                    slack_thread_ts=thread,
                )
            )
            _restore_complete_activity_memory_results(
                result,
                conversations.repository.conversation_directory(workspace, conversation_id),
                conversation_id,
            )
            if turn.track_activity and turn.actor == "owner":
                turns_check = _score_trajectory_links(
                    services.activity_store, result, previous_message_ids,
                )
            else:
                turns_check = None
            _update_substitutions(services, substitutions)
            scored_turn = _score_turn(turn, result, substitutions, index)
            scoped_activity_check = _score_resolved_activity_scope(
                turn, result, services.activity_store
            )
            if scoped_activity_check is not None:
                scored_turn["tool_checks"].append(scoped_activity_check)
            scored_turn["tool_checks"].extend(
                _score_activity_memory_turn(turn, result, services.activity_store)
            )
            if turn.unchanged_activities:
                scored_turn["tool_checks"].append(
                    _compare_activity_snapshots(
                        unchanged_snapshot,
                        _activity_snapshot(
                            services.activity_store,
                            turn.unchanged_activities,
                            existing_snapshot=unchanged_snapshot,
                        ),
                    )
                )
            if turns_check is not None:
                scored_turn["tool_checks"].append(turns_check)
            turns.append(scored_turn)
            if process_context_checkpoints and services.conversation_checkpoint_worker is not None:
                while True:
                    message = services.conversation_checkpoint_queue.read_one()
                    if message is None:
                        break
                    queued = message.get("message", {})
                    if (
                        queued.get("kind") == "activity_memory"
                        and any(deferred_index > index for deferred_index in case.defer_activity_memory_before_turns)
                    ):
                        deferred_activity_jobs.append(message)
                        continue
                    _process_benchmark_checkpoint_job(
                        message,
                        services,
                        conversations,
                        workspace,
                        published_checkpoints,
                        published_activity_memory,
                    )
                if index in case.defer_activity_memory_before_turns:
                    for message in deferred_activity_jobs:
                        _process_benchmark_checkpoint_job(
                            message,
                            services,
                            conversations,
                            workspace,
                            published_checkpoints,
                            published_activity_memory,
                        )
                    deferred_activity_jobs.clear()
    if deferred_activity_jobs and services.conversation_checkpoint_worker is not None:
        for message in deferred_activity_jobs:
            _process_benchmark_checkpoint_job(
                message,
                services,
                conversations,
                workspace,
                published_checkpoints,
                published_activity_memory,
            )
    state_checks = _score_state(
        services, case.expected.model_dump(exclude_unset=True), substitutions
    )
    if process_context_checkpoints:
        visible_messages = [
            message
            for conversation_id in set(chat_ids.values())
            for message in conversations.get_conversation(workspace, conversation_id).get(
                "messages", []
            )
        ]
        if case.case_id == "main_chat_interleaves_tasks_after_compaction":
            context_checks.extend(
                [
                    {
                        "check": "checkpoint_published",
                        "passed": bool(published_checkpoints),
                        "published_request_ids": published_checkpoints,
                    },
                    {
                        "check": "checkpoint_adopted",
                        "passed": bool(checkpoint_adoptions),
                        "adoptions": checkpoint_adoptions,
                    },
                    {
                        "check": "checkpoint_prefix_ends_at_safe_sdk_boundary",
                        # Require at least one captured checkpoint and require every
                        # captured prefix to match its view and end at a valid SDK cut.
                        "passed": bool(checkpoint_cuts)
                        and all(item["passed"] for item in checkpoint_cuts),
                        "captures": checkpoint_cuts,
                    },
                    {
                        "check": "checkpoint_clears_tool_namespaces",
                        "passed": _checkpoint_namespaces_cleared(checkpoint_adoptions),
                        "adoption_namespaces": [
                            item.get("loaded_namespaces") for item in checkpoint_adoptions
                        ],
                    },
                    {
                        "check": "transcript_projection_has_no_checkpoint_content",
                        "passed": _transcript_excludes_checkpoint_summaries(
                            visible_messages,
                            [
                                item["summary"]
                                for item in checkpoint_adoptions
                                if isinstance(item.get("summary"), str)
                            ],
                        ),
                        "message_count": len(visible_messages),
                        "roles": sorted({str(item.get("role")) for item in visible_messages}),
                    },
                ]
            )
            adoption_turn_indexes = sorted(
                {item["turn_index"] for item in checkpoint_adoptions}
            )
            context_checks.append(
                {
                    "check": "tool_namespace_reloaded_after_checkpoint",
                    "passed": any(
                        _activity_namespace_reloaded_before_use(turn_result)
                        for turn_result in turns
                        if turn_result["turn"] in adoption_turn_indexes
                    ),
                    "turns_with_load": [
                        turn_result["turn"]
                        for turn_result in turns
                        if _activity_namespace_reloaded_before_use(turn_result)
                    ],
                    "adoption_turn_indexes": adoption_turn_indexes,
                }
            )
        elif case.force_compaction:
            memory_turn_indexes = [
                turn_result["turn"]
                for turn_result, source_turn in zip(turns, case.turns)
                if source_turn.memory_activity_title_contains
            ]
            adoption_turn_indexes = sorted(
                {item["turn_index"] for item in checkpoint_adoptions}
            )
            context_checks.extend(
                [
                    {
                        "check": "global_checkpoint_adopted_before_each_task_memory_read",
                        "passed": bool(adoption_turn_indexes)
                        and (
                            not memory_turn_indexes
                            or any(
                                adopted_at <= min(memory_turn_indexes)
                                for adopted_at in adoption_turn_indexes
                            )
                        ),
                        "memory_read_turns": memory_turn_indexes,
                        "adoption_turn_indexes": adoption_turn_indexes,
                    },
                    {
                        "check": "global_checkpoint_prefix_ends_at_safe_sdk_boundary",
                        "passed": bool(checkpoint_cuts)
                        and all(item["passed"] for item in checkpoint_cuts),
                        "captures": checkpoint_cuts,
                    },
                    {
                        "check": "global_checkpoint_clears_tool_namespaces",
                        "passed": _checkpoint_namespaces_cleared(checkpoint_adoptions),
                        "adoption_namespaces": [
                            item.get("loaded_namespaces") for item in checkpoint_adoptions
                        ],
                    },
                    {
                        "check": "activity_namespace_reloaded_after_checkpoint",
                        "passed": bool(adoption_turn_indexes)
                        and all(
                            _activity_namespace_reloaded_before_use(turn_result)
                            for turn_result in turns
                            if turn_result["turn"] in adoption_turn_indexes
                            and turn_result.get("tool_calls")
                        ),
                        "adoption_turn_indexes": adoption_turn_indexes,
                        "turns_with_activity_namespace_load": [
                            turn_result["turn"]
                            for turn_result in turns
                            if _activity_namespace_reloaded_before_use(turn_result)
                        ],
                    },
                    {
                        "check": "transcript_projection_has_no_checkpoint_content",
                        "passed": _transcript_excludes_checkpoint_summaries(
                            visible_messages,
                            [
                                item["summary"]
                                for item in checkpoint_adoptions
                                if isinstance(item.get("summary"), str)
                            ],
                        ),
                        "message_count": len(visible_messages),
                        "roles": sorted({str(item.get("role")) for item in visible_messages}),
                    },
                ]
            )
            from lisa.activity_memory import activity_memory_configuration_fingerprint

            model_name = os.getenv("LISA_MODEL", "gemini/gemini-3.8-flash")
            configuration = activity_memory_configuration_fingerprint(model_name)
            memory_titles = sorted(
                {
                    item.title_contains for item in case.expected.activities
                }
                | {
                    item.title for item in case.seed_archived_activities
                }
            )
            memory_ids, memory_title_errors = _activity_ids_for_titles(
                services.activity_store, memory_titles
            )
            snapshots = []
            for activity_id in sorted(memory_ids):
                memory = services.activity_store.load_activity_memory_for_turn(
                    activity_id,
                    activity_id,
                    configuration_fingerprint=configuration,
                )
                snapshots.append(
                    {
                        "activity_id": activity_id,
                        "status": memory["status"],
                        "snapshot_watermark": memory["freshness_watermark"]["snapshot_event_seq"],
                        "current_watermark": memory["freshness_watermark"]["current_event_seq"],
                        "reference_count": len(memory["source_event_refs"]),
                    }
                )
            context_checks.append(
                {
                    "check": "final_activity_memory_snapshots_are_fresh_and_evidence_linked",
                    "passed": not memory_title_errors
                    and bool(memory_ids)
                    and len(snapshots) == len(memory_ids)
                    and all(
                        item["status"] == "ready"
                        and item["snapshot_watermark"] == item["current_watermark"]
                        and item["reference_count"] > 0
                        for item in snapshots
                    ),
                    "memory_title_errors": memory_title_errors,
                    "snapshots": snapshots,
                }
            )
            if case.defer_activity_memory_before_turns:
                deferred_turns = [
                    turns[index]
                    for index in case.defer_activity_memory_before_turns
                    if index < len(turns)
                ]
                context_checks.append(
                    {
                        "check": "pending_memory_uses_bounded_event_fallback",
                        "passed": all(
                            any(
                                call.get("name") == "load_activity_memory"
                                for call in turn_result.get("tool_calls", [])
                            )
                            for turn_result in deferred_turns
                        )
                        and all(
                            any(
                                check.get("check") == "activity_memory_evidence_refs"
                                and check.get("passed")
                                and any(
                                    result.get("status") == "pending"
                                    and result.get("event_count", 0) > 0
                                    for result in check.get("memory_results", [])
                                )
                                for check in turn_result.get("tool_checks", [])
                            )
                            for turn_result in deferred_turns
                        ),
                        "turns": case.defer_activity_memory_before_turns,
                    }
                )
        else:
            context_checks.append(
                {
                    "check": "slack_threads_have_distinct_conversations",
                    "passed": _slack_threads_have_distinct_conversations(chat_ids),
                    "thread_conversation_ids": {
                        key: conversation_id
                        for key, conversation_id in chat_ids.items()
                        if key.startswith("slack:")
                    },
                }
            )
            context_checks.append(
                {
                    "check": "separate_transcripts_contain_only_chat_roles",
                    "passed": bool(visible_messages)
                    and all(item.get("role") in {"user", "assistant"} for item in visible_messages),
                    "message_count": len(visible_messages),
                    "roles": sorted({str(item.get("role")) for item in visible_messages}),
                }
            )
    checks = [*state_checks, *(item for turn in turns for item in turn["tool_checks"])]
    communication = [item for turn in turns for item in turn["communication_checks"]]
    checks.extend([*communication, *context_checks])
    passed = _checks_pass_for_case(checks)
    case_result = {
        "case_id": case.case_id,
        "category": case.category,
        "split": case.split,
        "passed": passed,
        "state_checks": state_checks,
        "tool_checks": [item for turn in turns for item in turn["tool_checks"]],
        "communication_checks": communication,
        "context_checks": context_checks,
        "turns": turns,
        "scores": {
            "state": _rate(state_checks),
            "tool_use": _rate([item for turn in turns for item in turn["tool_checks"]]),
            "communication": _rate(communication),
        },
    }
    if case.category.startswith("memory_"):
        case_result["memory_ability"] = case.memory_ability or case.category
        case_result["upstream_benchmark"] = case.upstream_benchmark
        case_result["upstream_source_id"] = case.upstream_source_id
        case_result["adversarial_type"] = case.adversarial_type
        case_result["memory_scores"] = _memory_case_scores(case_result)
    return case_result


def _rate(checks: list[dict]) -> float | None:
    return None if not checks else round(sum(item["passed"] for item in checks) / len(checks), 4)


def _checks_pass_for_case(checks: list[dict]) -> bool:
    """Keep nonrequired exact-quote comparisons diagnostic instead of gating."""
    return all(
        item.get("passed") is True
        for item in checks
        if not (
            str(item.get("check", "")).startswith("reply_quotes_exactly:")
            and item.get("required_for_answer_success") is not True
        )
    )


def _turn_requires_verbatim_answer(text: str) -> bool:
    normalized = " ".join(text.casefold().split())
    return bool(
        re.search(
            r"\b(?:quote|repeat|copy|reproduce|transcribe)\b.{0,80}"
            r"\b(?:verbatim|word[- ]for[- ]word|exactly)\b",
            normalized,
        )
        or re.search(r"\bexact\s+(?:wording|text|answer|result)\b", normalized)
    )


def _normalize_verbatim_text(text: str) -> str:
    """Ignore Markdown presentation while preserving quoted words and punctuation."""
    normalized = re.sub(r"\[([^]]+)\]\([^)]+\)", r"\1", text)
    normalized = re.sub(r"(?<!\\)(?:\*\*?|__?|~~|`+)", "", normalized)
    normalized = re.sub(r"(?m)^\s*>\s?", "", normalized)
    normalized = re.sub(r"(?m)^\s*[-+*]\s+", "", normalized)
    return " ".join(normalized.split()).strip()


def _verbatim_quote_matches(reply: str, quote: str) -> bool:
    return _normalize_verbatim_text(quote) in _normalize_verbatim_text(reply)


def _memory_answer_checks(communication_checks: list[dict]) -> list[dict]:
    return [
        item for item in communication_checks
        if item.get("check", "").startswith("reply_contains:")
        or item.get("check") == "reply_matches_exactly_one_of"
        or item.get("check", "").startswith("reply_excludes:")
        or item.get("check") == "expected_abstention"
        or (
            item.get("check", "").startswith("reply_quotes_exactly:")
            and item.get("required_for_answer_success") is True
        )
    ]


def _is_memory_procedure_check(check: dict) -> bool:
    name = str(check.get("check", ""))
    return name.startswith((
        "required_tool:",
        "forbidden_tool:",
        "tool_order:",
        "tool_count:",
        "tool_errors",
    ))


def _memory_case_scores(case_result: dict) -> dict[str, float | None]:
    """Score task correctness and keep tool-procedure diagnostics independent."""
    tool_checks = case_result.get("tool_checks", [])
    communication_checks = case_result.get("communication_checks", [])
    answer_checks = _memory_answer_checks(communication_checks)
    selection_checks = [
        item for item in tool_checks
        if item.get("check") == "activity_memory_selection"
    ]
    evidence_checks = [
        item for item in tool_checks if item.get("check") == "activity_memory_evidence_refs"
    ]
    abstention_checks = [
        item for item in communication_checks if item.get("check") == "expected_abstention"
    ]
    quote_checks = [
        item for item in communication_checks
        if item.get("check", "").startswith("reply_quotes_exactly:")
    ]
    procedure_checks = [item for item in tool_checks if _is_memory_procedure_check(item)]
    safety_checks = [
        *case_result.get("state_checks", []),
        *[item for item in tool_checks if not _is_memory_procedure_check(item)],
        *[
            item for item in communication_checks
            if not item.get("check", "").startswith("reply_quotes_exactly:")
            or item.get("required_for_answer_success") is True
        ],
        *case_result.get("context_checks", []),
    ]
    core_correct = (
        bool(answer_checks)
        and all(item.get("passed") is True for item in answer_checks)
        and bool(selection_checks)
        and all(item.get("passed") is True for item in selection_checks)
        and bool(evidence_checks)
        and all(item.get("passed") is True for item in evidence_checks)
        and all(item.get("passed") is True for item in safety_checks)
        and not any(
            check.get("wrong_write_targets")
            for check in tool_checks
            if check.get("check") == "resolved_activity_scope"
        )
    )
    tool_error_checks = [item for item in procedure_checks if item.get("check") == "tool_errors"]
    return {
        "answer_success": _rate(answer_checks),
        "activity_selection": _rate(selection_checks),
        "evidence_reference_integrity": _rate(evidence_checks),
        "abstention": _rate(abstention_checks),
        "verbatim_repeat": _rate(quote_checks),
        "tool_sequence_adherence": _rate([
            item for item in procedure_checks
            if item.get("check", "").startswith((
                "required_tool:", "forbidden_tool:", "tool_order:", "tool_count:",
            ))
        ]),
        "tool_error_free": _rate(tool_error_checks),
        "core_correct": core_correct,
    }


def _memory_acceptance(case_results: list[dict], *, expected_cases: int = 35) -> dict:
    """Gate core task outcomes while reporting procedure misses independently."""
    memory_cases = [item for item in case_results if item.get("memory_ability")]
    evidence_checks = [
        check
        for case in memory_cases
        for check in case.get("tool_checks", [])
        if check.get("check") == "activity_memory_evidence_refs"
    ]
    scope_checks = [
        check
        for case in memory_cases
        for check in case.get("tool_checks", [])
        if check.get("check") == "resolved_activity_scope"
    ]
    leak_count = sum(
        1
        for check in evidence_checks
        for failure in check.get("failures", [])
        if failure.get("failure") in {
            "cross-activity text leak",
            "event evidence is outside the requested activity",
            "event reference does not resolve",
        }
    )
    leak_count += sum(
        check.get("passed") is not True
        for case in memory_cases
        for check in case.get("tool_checks", [])
        if check.get("check") == "activity_memory_excludes_other_context"
    )
    leak_count += sum(
        check.get("passed") is not True
        for case in memory_cases
        for check in case.get("tool_checks", [])
        if check.get("check") == "activity_thread_cross_task_isolation"
    )
    leak_count += sum(
        check.get("passed") is not True
        for case in memory_cases
        for check in case.get("communication_checks", [])
        if str(check.get("check", "")).startswith("reply_excludes:")
    )
    memory_scores = [_memory_case_scores(item) for item in memory_cases]
    correct_cases = sum(score["core_correct"] is True for score in memory_scores)
    evidence_refs_valid = bool(evidence_checks) and all(
        check.get("passed") is True for check in evidence_checks
    )
    wrong_activity_writes = sum(
        len(check.get("wrong_write_targets", [])) for check in scope_checks
    )
    procedure_checks = [
        check
        for case in memory_cases
        for check in case.get("tool_checks", [])
        if _is_memory_procedure_check(check)
    ]
    tool_sequence_checks = [
        check for check in procedure_checks
        if check.get("check", "").startswith((
            "required_tool:", "forbidden_tool:", "tool_order:", "tool_count:",
        ))
    ]
    tool_error_checks = [
        check for check in procedure_checks if check.get("check") == "tool_errors"
    ]
    accepted = (
        len(memory_cases) == expected_cases
        and correct_cases >= 32
        and evidence_refs_valid
        and leak_count == 0
        and wrong_activity_writes == 0
    )
    return {
        "case_count": len(memory_cases),
        "expected_case_count": expected_cases,
        "correct_cases": correct_cases,
        "core_correct_cases": correct_cases,
        "minimum_correct_cases": 32,
        "strict_case_passes": sum(bool(item.get("passed")) for item in memory_cases),
        "tool_sequence_failures": sum(check.get("passed") is not True for check in tool_sequence_checks),
        "unexpected_tool_error_cases": sum(check.get("passed") is not True for check in tool_error_checks),
        "evidence_refs_valid": evidence_refs_valid,
        "cross_activity_leaks": leak_count,
        "wrong_activity_writes": wrong_activity_writes,
        "accepted": accepted,
    }


def _benchmark_status(
    case_results: list[dict],
    runtime_failures: list[dict],
    *,
    memory_acceptance: dict | None = None,
) -> str:
    """Treat any failed labeled check as a failed live benchmark run."""
    return (
        "failed"
        if runtime_failures
        or any(
            not case.get("passed", False)
            for case in case_results
            if memory_acceptance is None or not case.get("memory_ability")
        )
        or memory_acceptance is not None and not memory_acceptance.get("accepted")
        else "completed"
    )


def _select_cases(cases: list[ActivityBenchmarkCase], split: str, case_prefix: str | None) -> list[ActivityBenchmarkCase]:
    return [
        case for case in cases
        if (split == "all" or case.split == split)
        and (case_prefix is None or case.case_id.startswith(case_prefix))
    ]


def run_benchmark(
    manifest_path: Path,
    split: str,
    output: Path,
    case_prefix: str | None = None,
    *,
    process_context_checkpoints: bool = False,
) -> dict:
    manifest, cases, manifest_digest = load_activity_dataset(manifest_path)
    selected = _select_cases(cases, split, case_prefix)
    if not selected:
        raise ValueError(f"No activity benchmark cases for split {split!r} and prefix {case_prefix!r}")
    artifact_dir = output.parent
    artifact_dir.mkdir(parents=True, exist_ok=True)
    stack = start_lisa_postgres(suite="lisa-activity-benchmark", artifact_dir=artifact_dir)
    case_results = []
    runtime_failures = []
    try:
        for case in selected:
            workspace = str(uuid4())
            database = create_lisa_test_database(stack.server_url, workspace)
            try:
                case_results.append(
                    _run_case(
                        case,
                        database,
                        workspace,
                        artifact_dir,
                        process_context_checkpoints=(
                            process_context_checkpoints or case.force_compaction
                        ),
                    )
                )
            except Exception as error:  # noqa: BLE001 - Save provider/runtime failures to the report.
                runtime_failures.append(
                    {"case_id": case.case_id, "error_type": type(error).__name__}
                )
                case_results.append(
                    {
                        "case_id": case.case_id,
                        "category": case.category,
                        "split": case.split,
                        "memory_ability": (
                            (case.memory_ability or case.category)
                            if case.category.startswith("memory_")
                            else None
                        ),
                        "adversarial_type": case.adversarial_type,
                        "passed": False,
                        "runtime_error": type(error).__name__,
                        "state_checks": [],
                        "tool_checks": [],
                        "communication_checks": [],
                        "context_checks": [],
                        "turns": [],
                        "scores": {"state": None, "tool_use": None, "communication": None},
                    }
                )
    finally:
        stop_lisa_postgres(stack)
    checks = [
        item
        for case in case_results
        for group in ("state_checks", "tool_checks", "communication_checks")
        for item in case[group]
    ]
    checks.extend(
        item for case in case_results for item in case.get("context_checks", [])
    )

    def macro_score(key: str, category: str | None = None) -> float | None:
        values = [
            case["scores"][key]
            for case in case_results
            if case["scores"][key] is not None
            and (category is None or case["category"] == category)
        ]
        return None if not values else round(sum(values) / len(values), 4)

    def macro_memory_score(key: str, category: str | None = None) -> float | None:
        values = [
            case["memory_scores"][key]
            for case in case_results
            if case.get("memory_scores")
            and case["memory_scores"][key] is not None
            and (category is None or case["category"] == category)
        ]
        return None if not values else round(sum(values) / len(values), 4)

    is_memory_suite = any(case.category.startswith("memory_") for case in selected)
    memory_acceptance = (
        _memory_acceptance(case_results)
        if is_memory_suite and split == "benchmark"
        else None
    )

    model = os.getenv("LISA_MODEL")
    if not model:
        model = "gemini/gemini-3.8-flash" if not os.getenv("LISA_BASE_URL") else None
    git_commit = (
        subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, check=False, capture_output=True, text=True
        ).stdout.strip()
        or None
    )
    report = {
        "benchmark": manifest.name,
        "dataset_digest": manifest_digest,
        "cases_digest": manifest.cases_sha256,
        "model": model,
        "provider": "gemini" if model and model.startswith("gemini/") else None,
        "backend": "openhands",
        "git_commit": git_commit,
        "split": split,
        "created_at": datetime.now(UTC).isoformat(),
        "case_count": len(selected),
        "completed_case_count": len(case_results) - len(runtime_failures),
        "passed_cases": sum(item["passed"] for item in case_results),
        "scores": {
            "state": macro_score("state"),
            "tool_use": macro_score("tool_use"),
            "communication": macro_score("communication"),
        },
        "memory_evaluation": (
            {
                "official_upstream_score": False,
                "scores": {
                    key: macro_memory_score(key)
                    for key in (
                        "answer_success",
                        "activity_selection",
                        "evidence_reference_integrity",
                        "abstention",
                        "verbatim_repeat",
                        "tool_sequence_adherence",
                        "tool_error_free",
                        "core_correct",
                    )
                },
                "heldout_acceptance": memory_acceptance,
            }
            if is_memory_suite
            else None
        ),
        "categories": {
            category: {
                "case_count": sum(case["category"] == category for case in case_results),
                "passed_cases": sum(
                    case["category"] == category and case["passed"] for case in case_results
                ),
                "scores": {
                    "state": macro_score("state", category),
                    "tool_use": macro_score("tool_use", category),
                    "communication": macro_score("communication", category),
                },
                "memory_scores": {
                    key: macro_memory_score(key, category)
                    for key in (
                        "answer_success",
                        "activity_selection",
                        "evidence_reference_integrity",
                        "abstention",
                        "verbatim_repeat",
                        "tool_sequence_adherence",
                        "tool_error_free",
                        "core_correct",
                    )
                },
            }
            for category in sorted({case["category"] for case in case_results})
        },
        "cases": case_results,
        "check_count": len(checks),
        "context_checks": [
            item for case in case_results for item in case.get("context_checks", [])
        ],
        "status": _benchmark_status(
            case_results, runtime_failures, memory_acceptance=memory_acceptance
        ),
        "runtime_failures": runtime_failures,
    }
    output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return report


def main(default_manifest: Path = DEFAULT_MANIFEST) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=None)
    parser.add_argument("--case-prefix", default=None)
    parser.add_argument(
        "--split", choices=("development", "benchmark", "all"), default="benchmark"
    )
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument(
        "--live", action="store_true", help="Confirm real model calls and provider charges"
    )
    parser.set_defaults(manifest=default_manifest)
    args = parser.parse_args()
    if not args.live:
        parser.error("--live is required because the benchmark invokes Lisa's configured model")
    if os.getenv("LISA_BASE_URL") or os.getenv("LISA_API_KEY"):
        parser.error("The live activity benchmark requires the direct Gemini production path")
    model = os.getenv("LISA_MODEL", "gemini/gemini-3.8-flash")
    if not model.startswith("gemini/"):
        parser.error("The live activity benchmark requires a Gemini model")
    os.environ["LISA_MODEL"] = model
    if not (os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")):
        parser.error("Set GOOGLE_API_KEY or GEMINI_API_KEY for the live Gemini benchmark")
    run_id = uuid4().hex[:12]
    artifact_root = (
        MEMORY_ARTIFACTS
        if Path(args.manifest).resolve() == MEMORY_MANIFEST.resolve()
        else ARTIFACTS
    )
    artifact_dir = artifact_root / run_id
    output = args.output or artifact_dir / "report.json"
    output = output.resolve()
    if not output.is_relative_to(artifact_root.resolve()):
        parser.error(f"Benchmark reports must be saved under {artifact_root}")
    if output.exists() or output.parent.exists() and any(output.parent.iterdir()):
        parser.error(
            f"Refusing to overwrite an existing benchmark artifact directory: {output.parent}"
        )
    report = run_benchmark(
        args.manifest, args.split, output, args.case_prefix,
    )
    print(
        json.dumps(
            {
                "report": str(output),
                "status": report["status"],
                "passed_cases": report["passed_cases"],
                "case_count": report["case_count"],
            }
        )
    )
    if report["status"] == "failed":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
