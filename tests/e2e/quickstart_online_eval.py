#!/usr/bin/env python3
"""Run a real agent and verify Lisa's autonomous scheduled evaluation."""

from __future__ import annotations

import json
import os
import platform
import re
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen
from uuid import NAMESPACE_URL, uuid4, uuid5

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "examples/quickstart"))
from evaluator import (  # noqa: E402
    AGENT_NAME,
    TheLakeApiError,
    request_json,
)


def refund_evaluation_result(session: dict, evaluator_name: str) -> dict | None:
    for score in session.get("scores", []):
        if score.get("source") == "evaluator" and score.get("name") == evaluator_name:
            return score
    return None


def lisa_report_describes_failure(reply: str, session_id: str) -> bool:
    normalized = reply.lower()
    failure_language = re.search(
        r"\b(fail(?:ed|ure)?|violat(?:ed|ion)|without|missing|did not)\b",
        normalized,
    )
    return (
        session_id in reply
        and "refund" in normalized
        and "eligibility" in normalized
        and failure_language is not None
    )


def run_sample_agent(api_url: str) -> str:
    compose_env = ROOT / "warehouse/quickstart/compose.env"
    command = [
        "docker",
        "compose",
        "--env-file",
        ".env",
        "--env-file",
        str(compose_env.relative_to(ROOT)),
        "--project-name",
        "thelake-quickstart",
        "--file",
        "examples/quickstart/compose.yaml",
        "run",
        "--rm",
        "refund-agent",
        "--agent-name",
        AGENT_NAME,
    ]
    env = os.environ.copy()
    parsed_url = urlsplit(api_url)
    if platform.system() == "Linux":
        env["THELAKE_QUICKSTART_AGENT_NETWORK"] = "host"
        agent_host = parsed_url.hostname or "127.0.0.1"
    else:
        agent_host = "host.docker.internal"
    agent_api_url = urlunsplit(
        (parsed_url.scheme, f"{agent_host}:{parsed_url.port}", parsed_url.path, "", "")
    )
    command.extend(["--api-url", agent_api_url])
    completed = subprocess.run(
        command, cwd=ROOT, env=env, text=True, capture_output=True, check=False
    )
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.returncode:
        if "prepayment credits are depleted" in completed.stderr.lower():
            raise RuntimeError(
                "Gemini rejected the sample-agent call because its project has no "
                "prepayment credits. Add Gemini API credits or update the key in "
                ".env, then rerun. This request did not produce a refund or an evaluator verdict."
            )
        print(completed.stderr[-2000:], end="", file=sys.stderr)
        raise RuntimeError(
            f"The sample agent exited with status {completed.returncode}."
        )
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    match = re.search(r"session_id=([A-Za-z0-9_-]+)", completed.stdout)
    if not match:
        raise RuntimeError("The sample agent did not report its session ID.")
    return match.group(1)


def wait_for_evaluation(
    api_url: str, session_id: str, evaluator_name: str, timeout_seconds: int = 180
) -> dict:
    deadline = time.monotonic() + timeout_seconds
    session_available = False
    while time.monotonic() < deadline:
        try:
            session = request_json(f"{api_url}/v1/sessions/{session_id}")
        except TheLakeApiError as error:
            if error.status_code != 404:
                raise
        else:
            session_available = True
            score = refund_evaluation_result(session, evaluator_name)
            if score:
                return score
        time.sleep(3)
    if not session_available:
        raise TimeoutError(
            f"Session {session_id} was not available in TheLake within {timeout_seconds} seconds."
        )
    raise TimeoutError(
        f"No '{evaluator_name}' score appeared for session {session_id} within "
        f"{timeout_seconds} seconds. Check theLake and lisa-agent logs."
    )


def wait_for_lisa_report(
    lisa_url: str,
    token: str,
    workspace_id: str,
    conversation_id: str,
    session_id: str,
    evaluator_name: str,
    previous_updated_at: str,
    timeout_seconds: int = 240,
) -> dict:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        conversation = lisa_request(
            lisa_url,
            token,
            f"/internal/v1/conversations/{conversation_id}?workspace_id={workspace_id}",
        )
        if conversation.get("updated_at") == previous_updated_at:
            time.sleep(3)
            continue
        replies = [
            message.get("text", "")
            for message in conversation.get("messages", [])
            if message.get("role") == "assistant"
        ]
        activities = conversation.get("activities", [])
        matching_activity = next(
            (
                item
                for item in activities
                if "evaluate_session returned evidence" in item.get("label", "")
                and session_id in item.get("detail", "")
                and evaluator_name in item.get("detail", "")
            ),
            None,
        )
        if (
            replies
            and matching_activity
            and lisa_report_describes_failure(replies[-1], session_id)
        ):
            conversation["reply"] = replies[-1]
            return conversation
        time.sleep(3)
    raise TimeoutError(
        f"Lisa's autonomous schedule did not record an evaluation for session {session_id} "
        f"within {timeout_seconds} seconds. Check lisa-agent logs."
    )


def lisa_request(
    lisa_url: str, token: str | None, path: str, payload: dict | None = None
) -> dict:
    body = None if payload is None else json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(
        f"{lisa_url}{path}",
        data=body,
        headers=headers,
        method="GET" if body is None else "POST",
    )
    with urlopen(request, timeout=150) as response:
        return json.loads(response.read())


def ask_lisa_to_approve_policy(lisa_url: str, policy_name: str) -> tuple[str, dict]:
    conversation = lisa_request(lisa_url, None, "/v1/conversations", {})
    conversation_id = conversation["conversation_id"]
    path = f"/v1/conversations/{conversation_id}/messages"
    lisa_request(
        lisa_url,
        None,
        path,
        {
            "text": (
                f"Propose an agent POLICY.md rule named '{policy_name}' for '{AGENT_NAME}': "
                "check refund eligibility and wait for the result before issuing a refund. "
                f"Read and save it with scope=agent and agent_name={AGENT_NAME}. "
                "When updating, use the raw Markdown returned by read_policy as expected_content, "
                "without any tool-display wrapper. Show the exact section with approved: yes "
                "for me to confirm. Do not save it yet."
            ),
            "consent": True,
        },
    )
    turn = lisa_request(
        lisa_url,
        None,
        path,
        {
            "text": f"Confirm policy rule: {policy_name}",
            "consent": True,
        },
    )
    saved = [
        item
        for item in turn.get("activities", [])
        if item.get("tool_name") == "update_policy"
    ]
    if not any(
        (args := json.loads(item.get("arguments") or "{}")).get("agent_name")
        == AGENT_NAME
        and args.get("scope") == "agent"
        and f"## {policy_name}\napproved: yes" in args.get("content", "")
        for item in saved
    ):
        raise RuntimeError(
            "Lisa did not save the owner-approved policy through her tools."
        )
    return conversation_id, turn


def wait_for_lisa_ready(lisa_url: str, timeout_seconds: int = 90) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            result = lisa_request(lisa_url, None, "/health")
        except (OSError, URLError):
            time.sleep(1)
            continue
        if result.get("ok") is True and result.get("model_configured") is True:
            return
        time.sleep(1)
    raise TimeoutError(f"Lisa did not become ready within {timeout_seconds} seconds.")


def read_compose_value(key: str) -> str:
    path = ROOT / "warehouse/quickstart/compose.env"
    for line in path.read_text(encoding="utf-8").splitlines():
        name, separator, value = line.partition("=")
        if separator and name == key:
            return value
    raise RuntimeError(f"{key} is missing from quickstart compose.env")


def main() -> int:
    api_file = ROOT / "warehouse/quickstart/api_url"
    if not api_file.exists():
        raise RuntimeError(
            "Start the quickstart first with bash examples/quickstart/start.sh."
        )
    api_url = api_file.read_text(encoding="utf-8").strip().rstrip("/")
    lisa_url = (
        (ROOT / "warehouse/quickstart/lisa_api_url")
        .read_text(encoding="utf-8")
        .strip()
        .rstrip("/")
    )
    workspace_id = read_compose_value("THELAKE_DEFAULT_WORKSPACE_ID")
    lisa_token = read_compose_value("LISA_THELAKE_TOKEN")
    evaluator_name = f"Quickstart refund check {uuid4().hex[:6]}"
    wait_for_lisa_ready(lisa_url)
    conversation_id, _turn = ask_lisa_to_approve_policy(lisa_url, evaluator_name)
    print(f"Lisa saved approved policy: {evaluator_name}")
    conversation_id = str(
        uuid5(NAMESPACE_URL, f"thelake:{workspace_id}:scheduled-agent-qa")
    )
    conversation = lisa_request(
        lisa_url,
        lisa_token,
        "/internal/v1/conversations",
        {"workspace_id": workspace_id, "conversation_id": conversation_id},
    )
    previous_updated_at = conversation.get("updated_at", "")
    print(
        "Running the deliberately flawed refund agent and sending its trace to TheLake..."
    )
    session_id = run_sample_agent(api_url)
    print(f"Lisa will evaluate session {session_id} on her next scheduled cycle...")
    result = wait_for_evaluation(api_url, session_id, evaluator_name)
    status = result.get("string_value", "unknown")
    comment = result.get("comment") or "No evaluator explanation was recorded."
    if status == "error":
        print(f"Online evaluation could not run: {comment}")
        print(
            f"Open Explorer and view session {session_id} to inspect the trace and evaluator error."
        )
        return 1
    print(f"\nLisa found: {status.upper()} — {comment}")
    print(
        f"Open Explorer and view session {session_id} to inspect the trace and evidence."
    )
    if status != "fail":
        print(
            "The walkthrough expected a failure; inspect this evaluator result before continuing."
        )
        return 1
    report = wait_for_lisa_report(
        lisa_url,
        lisa_token,
        workspace_id,
        conversation_id,
        session_id,
        evaluator_name,
        previous_updated_at,
    )
    print("Lisa evaluated the captured session and stored the result in TheLake.")
    print(f"Lisa report: {report['reply']}")
    print(f"Open Lisa in Explorer to continue the conversation: {api_url}/explorer/")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, TimeoutError) as error:
        print(f"Quickstart demo failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
