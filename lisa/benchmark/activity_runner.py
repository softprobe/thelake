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
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import UUID, uuid4

import psycopg

from benchmark.policy_seed import seed_policy_document
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

ROOT = Path(__file__).resolve().parents[2]
SKILLS = ROOT / "lisa" / "skills"
ARTIFACTS = ROOT / "target" / "test-artifacts" / "lisa-activity-benchmark"
OWNER = "activity-benchmark-owner"
OUTSIDER = "activity-benchmark-outsider"


def _interpolate(text: str, values: dict[str, str]) -> str:
    template = re.sub(r"\$policy_id", r"{policy_id}", text)
    return template.format_map({key: value for key, value in values.items()})


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
    lowered_feed = [(item, item["title"].casefold()) for item in feed]
    for label in expected.get("activities", []):
        candidates = [
            item
            for item, title in lowered_feed
            if label["title_contains"].casefold() in title
        ]
        found = None
        timeline_summaries: list[str] = []
        for candidate in candidates:
            if label.get("status") and candidate["status"] != label["status"]:
                continue
            if label.get("next_action_contains") and label[
                "next_action_contains"
            ].casefold() not in (candidate.get("next_action") or "").casefold():
                continue
            if label.get("min_timeline_actions", 0) or label.get("timeline_text_contains"):
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
        ok = found is not None
        if found is None and candidates:
            found = candidates[0]
        checks.append(
            {
                "check": f"activity:{label['title_contains']}",
                "passed": bool(ok),
                "expected": label,
                "actual": None
                if found is None
                else {
                    "id": str(found["id"]),
                    "title": found["title"],
                    "status": found["status"],
                    "next_action": found.get("next_action"),
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
        ordered_names = iter(tool_names)
        passed_order = all(
            any(actual == wanted for actual in ordered_names) for wanted in turn.tool_order
        )
        action_checks.append(
            {
                "check": "tool_order:" + "->".join(turn.tool_order),
                "passed": passed_order,
                "actual": tool_names,
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
    for phrase in turn.reply_must_not_include:
        reply_checks.append(
            {
                "check": f"reply_excludes:{phrase}",
                "passed": phrase.casefold() not in reply.casefold(),
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


def _run_case(
    case: ActivityBenchmarkCase,
    database: LisaTestDatabase,
    workspace: str,
    artifact_dir: Path,
) -> dict:
    from lisa.backends.openhands import OpenHandsBackend
    from lisa.bootstrap import build_services
    from lisa.context import WorkspaceContext
    writer_url, reader_url = database.writer_url, database.reader_url
    data_dir = artifact_dir / "workspaces" / case.case_id
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
    _seed_historical_evaluations(services, case)
    conversations = services.conversations
    main = asyncio.run(conversations.main_conversation(workspace))
    chat_ids = {"main": main["conversation_id"]}
    substitutions: dict[str, str] = {}
    turns = []
    for index, turn in enumerate(case.turns):
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
            for message in conversations.get_conversation(workspace, conversation_id).get("messages", [])
        }
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
        if turn.track_activity and turn.actor == "owner":
            turns_check = _score_trajectory_links(
                services.activity_store, result, previous_message_ids,
            )
        else:
            turns_check = None
        _update_substitutions(services, substitutions)
        scored_turn = _score_turn(turn, result, substitutions, index)
        if turns_check is not None:
            scored_turn["tool_checks"].append(turns_check)
        turns.append(scored_turn)
    state_checks = _score_state(
        services, case.expected.model_dump(exclude_unset=True), substitutions
    )
    checks = [*state_checks, *(item for turn in turns for item in turn["tool_checks"])]
    communication = [item for turn in turns for item in turn["communication_checks"]]
    checks.extend(communication)
    passed = all(item["passed"] for item in checks)
    return {
        "case_id": case.case_id,
        "category": case.category,
        "split": case.split,
        "passed": passed,
        "state_checks": state_checks,
        "tool_checks": [item for turn in turns for item in turn["tool_checks"]],
        "communication_checks": communication,
        "turns": turns,
        "scores": {
            "state": _rate(state_checks),
            "tool_use": _rate([item for turn in turns for item in turn["tool_checks"]]),
            "communication": _rate(communication),
        },
    }


def _rate(checks: list[dict]) -> float | None:
    return None if not checks else round(sum(item["passed"] for item in checks) / len(checks), 4)


def _select_cases(cases: list[ActivityBenchmarkCase], split: str, case_prefix: str | None) -> list[ActivityBenchmarkCase]:
    return [
        case for case in cases
        if (split == "all" or case.split == split)
        and (case_prefix is None or case.case_id.startswith(case_prefix))
    ]


def run_benchmark(
    manifest_path: Path, split: str, output: Path, case_prefix: str | None = None,
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
                case_results.append(_run_case(case, database, workspace, artifact_dir))
            except Exception as error:  # noqa: BLE001 - Save provider/runtime failures to the report.
                runtime_failures.append(
                    {"case_id": case.case_id, "error_type": type(error).__name__}
                )
                case_results.append(
                    {
                        "case_id": case.case_id,
                        "category": case.category,
                        "split": case.split,
                        "passed": False,
                        "runtime_error": type(error).__name__,
                        "state_checks": [],
                        "tool_checks": [],
                        "communication_checks": [],
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

    def macro_score(key: str, category: str | None = None) -> float | None:
        values = [
            case["scores"][key]
            for case in case_results
            if case["scores"][key] is not None
            and (category is None or case["category"] == category)
        ]
        return None if not values else round(sum(values) / len(values), 4)

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
            }
            for category in sorted({case["category"] for case in case_results})
        },
        "cases": case_results,
        "check_count": len(checks),
        "status": "failed" if runtime_failures else "completed",
        "runtime_failures": runtime_failures,
    }
    output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return report


def main() -> None:
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
    artifact_dir = ARTIFACTS / run_id
    output = args.output or artifact_dir / "report.json"
    output = output.resolve()
    if not output.is_relative_to(ARTIFACTS.resolve()):
        parser.error(f"Benchmark reports must be saved under {ARTIFACTS}")
    if output.exists() or output.parent.exists() and any(output.parent.iterdir()):
        parser.error(
            f"Refusing to overwrite an existing benchmark artifact directory: {output.parent}"
        )
    report = run_benchmark(
        args.manifest or DEFAULT_MANIFEST, args.split, output, args.case_prefix,
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
