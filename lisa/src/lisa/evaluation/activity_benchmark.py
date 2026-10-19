"""Run the labeled activity task benchmark through Lisa's real agent runtime."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse
from uuid import uuid4

import psycopg
from psycopg import sql

from .activity_dataset import (
    DEFAULT_MANIFEST,
    ActivityBenchmarkCase,
    ActivityTurn,
    load_activity_dataset,
)

ROOT = Path(__file__).resolve().parents[4]
SKILLS = ROOT / "lisa" / "skills"
TEST_STACK = ROOT / "scripts" / "test_stack.py"
COMPOSE = ROOT / "tests" / "compose.test.yaml"
ARTIFACTS = ROOT / "target" / "test-artifacts" / "lisa-activity-benchmark"
OWNER = "activity-benchmark-owner"
OUTSIDER = "activity-benchmark-outsider"
MUTATING_TOOLS = {
    "create_activity",
    "update_activity",
    "set_activity_progress",
    "complete_activity",
    "propose_activity_followup",
    "approve_activity_followup",
    "create_policy_proposal",
    "apply_policy_proposal",
    "reject_policy_proposal",
}


def _runtime_dsn(base, database: str, role: str, password: str) -> str:
    host = f"[{base.hostname}]" if base.hostname and ":" in base.hostname else base.hostname
    authority = f"{quote(role, safe='')}:{quote(password, safe='')}@{host}"
    if base.port:
        authority += f":{base.port}"
    return urlunparse(base._replace(netloc=authority, path=f"/{database}"))


def _provision_schema(dsn: str, workspace: str, writer: str, reader: str) -> None:
    path = ROOT / "lisa" / "scripts" / "provision_lisa.py"
    spec = importlib.util.spec_from_file_location("lisa_activity_benchmark_provision", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load Lisa's database provisioner")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    module.provision(dsn, workspace, writer_role=writer, reader_role=reader, recreate=True)


def _new_database(admin_dsn: str, base, workspace: str, case_id: str):
    suffix = uuid4().hex[:12]
    database = f"lisa_eval_{suffix}"
    writer, reader = f"lew_{suffix}", f"ler_{suffix}"
    writer_password, reader_password = uuid4().hex, uuid4().hex
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        connection.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(writer), sql.Literal(writer_password)
            )
        )
        connection.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(reader), sql.Literal(reader_password)
            )
        )
        connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    database_dsn = urlunparse(base._replace(path=f"/{database}"))
    _provision_schema(database_dsn, workspace, writer, reader)
    return (
        _runtime_dsn(base, database, writer, writer_password),
        _runtime_dsn(base, database, reader, reader_password),
        workspace,
    )


def _interpolate(text: str, values: dict[str, str]) -> str:
    template = re.sub(r"\$(proposal_id|followup_id)", r"{\1}", text)
    return template.format_map({key: value for key, value in values.items()})


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


def _run_case(case: ActivityBenchmarkCase, connection_info, artifact_dir: Path) -> dict:
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

    writer_url, reader_url, workspace = connection_info
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
        turns.append(_score_turn(turn, result, substitutions, index))
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


def _run_stack(artifact_dir: Path) -> dict:
    command = [
        sys.executable,
        str(TEST_STACK),
        "start-compose",
        "--suite",
        "lisa-activity-benchmark",
        "--compose-file",
        str(COMPOSE),
        "--service",
        "lisa-postgres",
        "--published-port",
        "lisa-postgres:5432",
        "--artifact-dir",
        str(artifact_dir),
        "--wait",
    ]
    result = subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def run_benchmark(manifest_path: Path, split: str, output: Path) -> dict:
    manifest, cases, manifest_digest = load_activity_dataset(manifest_path)
    selected = [case for case in cases if split == "all" or case.split == split]
    if not selected:
        raise ValueError(f"No activity benchmark cases for split {split!r}")
    artifact_dir = output.parent
    artifact_dir.mkdir(parents=True, exist_ok=True)
    stack = _run_stack(artifact_dir)
    port = stack["ports"]["lisa-postgres:5432"][0].rsplit(":", 1)[1]
    admin_url = f"postgresql://lisa_test:lisa_test@127.0.0.1:{port}/postgres"
    base = urlparse(admin_url)
    case_results = []
    runtime_failures = []
    try:
        for case in selected:
            workspace = str(uuid4())
            database = _new_database(admin_url, base, workspace, case.case_id)
            try:
                case_results.append(_run_case(case, database, artifact_dir))
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
        subprocess.run(
            [sys.executable, str(TEST_STACK), "stop", "--state-file", stack["state_file"]],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
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
    parser.add_argument("--split", choices=("development", "test", "all"), default="test")
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
