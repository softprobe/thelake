"""Run the labeled activity task benchmark through Lisa's real agent runtime.

Offline harness only — see ``benchmark`` package docs. Do not relocate into
``lisa.evaluation``.
"""

from __future__ import annotations

import argparse
import asyncio
from hashlib import sha256
import json
import os
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

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

ROOT = Path(__file__).resolve().parents[2]
SKILLS = ROOT / "lisa" / "skills"
ARTIFACTS = ROOT / "target" / "test-artifacts" / "lisa-activity-benchmark"
OWNER = "activity-benchmark-owner"
OUTSIDER = "activity-benchmark-outsider"


def _interpolate(text: str, values: dict[str, str]) -> str:
    template = re.sub(r"\$(proposal_id|followup_id)", r"{\1}", text)
    return template.format_map({key: value for key, value in values.items()})


def _update_substitutions(services, substitutions: dict[str, str]) -> None:
    proposals = services.activity_store.list_policy_proposals()
    if proposals:
        substitutions["proposal_id"] = str(proposals[0]["id"])
    activities = services.activity_store.list_activity_feed(limit=100)
    followups = [
        followup
        for activity in activities
        for followup in services.activity_store.list_activity_followups(str(activity["id"]))
    ]
    if followups:
        substitutions["followup_id"] = str(followups[0]["id"])


def _score_state(services, expected: dict) -> list[dict]:
    feed = services.activity_store.list_activity_feed(limit=100)
    checks: list[dict] = []
    wanted_count = expected["activity_count"]
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
        found = next(
            (item for item, title in lowered_feed if label["title_contains"].casefold() in title),
            None,
        )
        ok = found is not None
        timeline_summaries: list[str] = []
        if found is not None:
            if label.get("status"):
                ok = ok and found["status"] == label["status"]
            if label.get("next_action_contains"):
                ok = (
                    ok
                    and label["next_action_contains"].casefold()
                    in (found.get("next_action") or "").casefold()
                )
            if label.get("min_timeline_actions", 0) or label.get("timeline_text_contains"):
                detail = services.activity_store.get_activity_thread(str(found["id"]))
                timeline_summaries = [
                    str(action.get("summary") or "") for action in detail["actions"]
                ]
            if label.get("min_timeline_actions", 0):
                ok = ok and len(detail["actions"]) >= label["min_timeline_actions"]
            if label.get("timeline_text_contains"):
                timeline = "\n".join(timeline_summaries).casefold()
                ok = ok and all(
                    phrase.casefold() in timeline for phrase in label["timeline_text_contains"]
                )
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
    for label in expected.get("followups", []):
        activity = next(
            (
                item
                for item, title in lowered_feed
                if label["activity_title_contains"].casefold() in title
            ),
            None,
        )
        followups = (
            services.activity_store.list_activity_followups(str(activity["id"])) if activity else []
        )
        matching = [item for item in followups if item["status"] == label["status"]]
        ok = bool(matching)
        if label.get("count") is not None:
            ok = ok and len(followups) == label["count"]
        if label.get("trigger_event_type") is not None:
            ok = ok and any(
                item.get("trigger_event_type") == label["trigger_event_type"] for item in matching
            )
        if label.get("prompt_contains"):
            prompt = "\n".join(str(item.get("prompt") or "") for item in matching).casefold()
            ok = ok and all(phrase.casefold() in prompt for phrase in label["prompt_contains"])
        checks.append(
            {
                "check": f"followup:{label['activity_title_contains']}:{label['status']}",
                "passed": ok,
                "actual_statuses": [item["status"] for item in followups],
                "actual_count": len(followups),
            }
        )
    proposals = services.activity_store.list_policy_proposals()
    if "pending_proposal_count" in expected:
        checks.append(
            {
                "check": "pending_proposal_count",
                "passed": len(proposals) == expected["pending_proposal_count"],
                "expected": expected["pending_proposal_count"],
                "actual": len(proposals),
            }
        )
    for label in expected.get("proposals", []):
        ok = any(
            label["policy_name_contains"].casefold() in item["policy_name"].casefold()
            and item["status"] == label["status"]
            for item in proposals
        )
        checks.append(
            {
                "check": f"proposal:{label['policy_name_contains']}:{label['status']}",
                "passed": ok,
                "actual_statuses": [
                    item["status"]
                    for item in proposals
                    if label["policy_name_contains"].casefold() in item["policy_name"].casefold()
                ],
            }
        )
    records = services.policy_service.read_policy_records()
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
    return checks


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
    for phrase in turn.tool_arguments_must_include:
        action_checks.append(
            {
                "check": f"tool_arguments_contain:{phrase}",
                "passed": phrase.casefold() in arguments.casefold(),
            }
        )
    database_queries = [
        str(value)
        for call in calls
        if call.get("tool_name") == "lisa_db"
        for value in call.get("arguments", {}).values()
        if isinstance(value, str)
    ]
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
    services.policy_service.update_policy(
        markdown,
        "No policy exists yet.",
        confirmed_title=policy_names[0],
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
            with psycopg.connect(services.context.database_url) as connection:
                result = connection.execute(
                    fixture_sql,
                    {"evaluation_id": run.id, "verdict": item.verdict, "score_id": score_id},
                )
                if result.rowcount != 1:
                    raise RuntimeError("Could not seed historical judge verdict")


def _run_case(
    case: ActivityBenchmarkCase,
    database: LisaTestDatabase,
    workspace: str,
    artifact_dir: Path,
) -> dict:
    from lisa.backends.openhands import OpenHandsBackend
    from lisa.bootstrap import build_services
    from lisa.context import WorkspaceContext
    from lisa.repositories.conversations import FileConversationRepository
    from lisa.runtime import (
        confirmed_followup_id,
        confirmed_policy_proposal_id,
        confirmed_policy_title,
        rejected_policy_proposal_id,
    )
    from lisa.services.conversations import ConversationService

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
    conversations = ConversationService(FileConversationRepository(data_dir))
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
        result = asyncio.run(
            conversations.run_user_turn(
                conversation_id,
                workspace,
                text,
                run_turn=OpenHandsBackend().run_turn,
                confirmed_policy_title=confirmed_policy_title(text),
                confirmed_policy_proposal_id=confirmed_policy_proposal_id(text),
                rejected_policy_proposal_id=rejected_policy_proposal_id(text),
                confirmed_followup_id=confirmed_followup_id(text),
                turn_source=source,
                actor_id=OWNER if turn.actor == "owner" else OUTSIDER,
                slack_channel_id=channel,
                slack_thread_ts=thread,
            )
        )
        _update_substitutions(services, substitutions)
        turns.append(_score_turn(turn, result, substitutions, index))
    state_checks = _score_state(services, case.expected.model_dump(exclude_unset=True))
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


def run_benchmark(manifest_path: Path, split: str, output: Path) -> dict:
    manifest, cases, manifest_digest = load_activity_dataset(manifest_path)
    selected = [case for case in cases if split == "all" or case.split == split]
    if not selected:
        raise ValueError(f"No activity benchmark cases for split {split!r}")
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
                break
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
    if not (
        os.getenv("LISA_BASE_URL")
        and os.getenv("LISA_API_KEY")
        or os.getenv("GOOGLE_API_KEY")
        or os.getenv("GEMINI_API_KEY")
    ):
        parser.error("Set the configured model credentials in the process environment")
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
    report = run_benchmark(args.manifest or DEFAULT_MANIFEST, args.split, output)
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
