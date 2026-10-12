"""Workspace tools exposed through the standard Model Context Protocol."""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import logging
import os
import re
import tempfile
import threading
import urllib.request
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from fastmcp import FastMCP
from pydantic import ValidationError

from thelake_lisa.evaluation.contracts import EvaluationRequest, EvaluationResponse
from thelake_lisa.evaluation.evaluator import JudgeUnavailableError, evaluate
from thelake_lisa.security import redact_evidence, redact_text

mcp = FastMCP("Lisa workspace tools")
logger = logging.getLogger(__name__)
_locks: dict[str, threading.RLock] = {}
_locks_guard = threading.Lock()


def _workspace_id() -> str:
    try:
        return str(UUID(os.environ["LISA_WORKSPACE_ID"]))
    except (KeyError, ValueError) as error:
        raise RuntimeError("Lisa workspace scope is missing or invalid") from error


def _workspace_home() -> Path:
    data_dir = Path(os.getenv("LISA_DATA_DIR", "/var/lib/lisa/workspaces")).resolve()
    digest = hashlib.sha256(_workspace_id().encode()).hexdigest()
    home = data_dir / digest / "home"
    if home.is_symlink() or not home.resolve().is_relative_to(data_dir):
        raise ValueError("Lisa policy home escapes its workspace data directory")
    return home


def _policy_path(scope: str, agent_name: str = "") -> Path:
    home = _workspace_home()
    if scope == "workspace":
        path = home / "POLICY.md"
    elif scope == "agent" and agent_name.strip():
        digest = hashlib.sha256(agent_name.encode("utf-8")).hexdigest()
        path = home / "agents" / digest / "POLICY.md"
    else:
        raise ValueError("Use workspace scope or provide the exact agent name for agent scope")
    if path.is_symlink() or not path.parent.resolve().is_relative_to(home.resolve()):
        raise ValueError("Policy path escapes Lisa's workspace home or uses a symlink")
    return path


def _policy_lock(path: Path) -> threading.RLock:
    key = str(path.parent.resolve())
    with _locks_guard:
        return _locks.setdefault(key, threading.RLock())


def _policy_rules(markdown: str) -> dict[str, str]:
    rules: dict[str, str] = {}
    current_title = ""
    current_lines: list[str] = []

    def save() -> None:
        if current_title:
            rules[current_title] = "\n".join([f"## {current_title}", *current_lines]).strip()

    for line in markdown.splitlines():
        heading = re.fullmatch(r"##\s+(.+?)\s*", line)
        if heading:
            save()
            current_title = heading.group(1)
            current_lines = []
        elif current_title:
            current_lines.append(line)
    save()
    return rules


def _validate_policy_markdown(markdown: str) -> None:
    sections: list[tuple[str, list[str]]] = []
    current_title = ""
    current_lines: list[str] = []

    def save() -> None:
        if current_title:
            sections.append((current_title, current_lines.copy()))

    for line in markdown.splitlines():
        heading = re.fullmatch(r"##\s+(.+?)\s*", line)
        if heading:
            save()
            current_title = heading.group(1)
            current_lines = []
        elif current_title:
            current_lines.append(line)
    save()
    if not sections:
        raise ValueError("Policy Markdown must contain at least one '##' rule section")

    titles = [title.strip().casefold() for title, _lines in sections]
    if len(titles) != len(set(titles)):
        raise ValueError("Policy rule names must be unique within a POLICY.md file")

    for title, lines in sections:
        if not title.strip() or not "\n".join(lines).strip():
            raise ValueError("Each named policy rule must have readable Markdown content")


def _check_policy_changes(content: str, current: str) -> None:
    before = _policy_rules(current)
    after = _policy_rules(content)
    changed_titles = {
        title for title in before.keys() | after.keys() if before.get(title) != after.get(title)
    }
    if not changed_titles:
        return
    if os.getenv("LISA_TURN_SOURCE", "web") not in {"web", "slack"}:
        raise ValueError("Only an owner conversation can save policy changes")
    confirmed_title = os.getenv("LISA_CONFIRMED_POLICY_TITLE", "").strip()
    if not confirmed_title:
        raise ValueError("Propose the named policy in chat and wait for the owner's explicit confirmation")
    if changed_titles != {confirmed_title}:
        raise ValueError("Only the exact policy name the owner confirmed this turn may change")


@mcp.tool()
def list_policies() -> list[str] | str:
    """List Lisa's workspace and agent-specific POLICY.md memory files."""
    home = _workspace_home()
    files = ["POLICY.md"] if (home / "POLICY.md").is_file() else []
    agent_root = home / "agents"
    if agent_root.is_dir() and not agent_root.is_symlink():
        files.extend(
            f"agents/{directory.name}/POLICY.md"
            for directory in agent_root.iterdir()
            if directory.is_dir()
            and not directory.is_symlink()
            and (directory / "POLICY.md").is_file()
            and not (directory / "POLICY.md").is_symlink()
        )
    return files or "No Lisa policy files exist yet."


@mcp.tool()
def read_policy(scope: str = "workspace", agent_name: str = "") -> str:
    """Read workspace policy or the policy for one exact registered agent."""
    path = _policy_path(scope, agent_name)
    if path.is_symlink():
        raise ValueError("Policy files cannot be symlinks")
    if not path.is_file():
        return "No policy exists yet."
    if path.stat().st_size > 20_000:
        raise ValueError("Policy Markdown exceeds the 20000 character limit")
    content = path.read_text(encoding="utf-8")
    return redact_text(content)


@mcp.tool()
def update_policy(
    content: str,
    expected_content: str,
    scope: str = "workspace",
    agent_name: str = "",
) -> str:
    """Save complete policy Markdown after reading its current value."""
    content = redact_text(content)
    expected_content = redact_text(expected_content)
    if not content.strip() or len(content) > 20_000:
        raise ValueError("Policy Markdown must contain 1 to 20000 characters")
    _validate_policy_markdown(content)
    path = _policy_path(scope, agent_name)
    with _policy_lock(path):
        current = path.read_text(encoding="utf-8") if path.is_file() else ""
        normalized_expected = "" if expected_content == "No policy exists yet." else expected_content
        if redact_text(current) != normalized_expected:
            raise ValueError("Policy changed since it was read; read it again before updating")
        if current != content and os.getenv("LISA_TURN_SOURCE", "web") not in {"web", "slack"}:
            raise ValueError("Only an owner conversation can save policy changes")
        _check_policy_changes(content, current)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_symlink() or not path.parent.resolve().is_relative_to(_workspace_home().resolve()):
            raise ValueError("Policy path escapes Lisa's workspace home or uses a symlink")
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=path.parent, prefix=".policy-", delete=False
        ) as temporary:
            temporary.write(content)
            temporary_path = Path(temporary.name)
        temporary_path.replace(path)
    return f"Saved {scope} policy memory."


def _thelake_post(path: str, body: dict, *, include_status: bool = False) -> dict:
    base_url = os.getenv("LISA_THELAKE_API_URL", "").rstrip("/")
    token = os.getenv("LISA_THELAKE_TOKEN", "")
    workspace_id = _workspace_id()
    if not base_url or not token:
        raise RuntimeError("TheLake internal API is not configured")
    signature = hmac.new(
        token.encode(),
        f"thelake-lisa-workspace-v1:{workspace_id}".encode(),
        sha256,
    ).hexdigest()
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-TheLake-Workspace-ID": workspace_id,
            "X-TheLake-Workspace-Signature": signature,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            result = json.loads(response.read().decode("utf-8"))
            return (result, response.status) if include_status else result
    except urllib.error.HTTPError as error:
        try:
            response_body = json.loads(error.read().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            response_body = {}
        detail = response_body.get("error") or response_body.get("message")
        if not isinstance(detail, str) or not detail.strip():
            detail = error.reason
        detail = redact_text(detail)[:500]
        raise RuntimeError(f"TheLake API request failed ({error.code}): {detail}") from None


def run_evaluation(body: dict) -> dict:
    """Validate bounded trace evidence and run Lisa's local DeepEval judge."""
    try:
        request = EvaluationRequest.model_validate(body)
        return evaluate(request).model_dump(mode="json")
    except (ValidationError, JudgeUnavailableError):
        return _evaluation_error(body, "Evaluation could not run because the evidence was invalid or the judge is unavailable.", "evaluation_request_or_provider_error")
    except Exception as error:
        # Provider exceptions may contain trace content or credentials.
        logger.error("Lisa's local evaluator failed (%s)", type(error).__name__)
        return _evaluation_error(body, "The judge provider failed while evaluating this trace.", "judge_provider_error")


def _evaluation_error(body: dict, rationale: str, limitation: str) -> dict:
    """Return a controlled failure without including provider or trace details."""
    evaluator = body.get("evaluator") if isinstance(body.get("evaluator"), dict) else {}
    evidence = body.get("evidence") if isinstance(body.get("evidence"), dict) else {}
    try:
        version = max(1, int(evaluator.get("version", 1)))
    except (TypeError, ValueError):
        version = 1
    try:
        threshold = min(1.0, max(0.0, float(evaluator.get("threshold", 0.7))))
    except (TypeError, ValueError):
        threshold = 0.7
    return EvaluationResponse(
        run_id=str(body.get("run_id") or "evaluation-error")[:256],
        evaluator_id=str(evaluator.get("evaluator_id") or "unknown")[:256],
        evaluator_version=version,
        trace_id=str(evidence.get("trace_id") or "unknown")[:256],
        status="error",
        threshold=threshold,
        rationale=rationale,
        limitations=[limitation],
    ).model_dump(mode="json")


def _span_events(spans: list[dict]) -> tuple[list[dict], bool, bool]:
    events: list[tuple[str, str, dict]] = []
    tool_events_captured = False
    tool_order_certain = True
    for span in spans:
        span_id = str(span.get("span_id") or "")
        if not span_id:
            continue
        parent_span_id = span.get("parent_span_id")
        start = str(span.get("start_time") or datetime.now(UTC).isoformat())
        end = str(span.get("end_time") or start)
        attrs = span.get("attributes") if isinstance(span.get("attributes"), dict) else {}
        observation_type = str(span.get("observation_type") or span.get("type") or "").lower()
        tool_name = attrs.get("gen_ai.tool.name") or span.get("tool_name")
        if not tool_name and "tool" in observation_type:
            tool_name = span.get("name")
        if tool_name:
            tool_events_captured = True
            call_id = attrs.get("gen_ai.tool.call.id")
            events.append((start, span_id, {
                "span_id": span_id, "parent_span_id": parent_span_id,
                "timestamp": start, "kind": "tool_call", "name": str(tool_name),
                "call_id": call_id, "payload": attrs.get("sp.input", span.get("input")),
            }))
            if span.get("end_time"):
                events.append((end, span_id, {
                    "span_id": span_id, "parent_span_id": parent_span_id,
                    "timestamp": end, "kind": "tool_result", "name": str(tool_name),
                    "call_id": call_id, "payload": attrs.get("sp.output", span.get("output")),
                }))
        else:
            prompt = span.get("input", attrs.get("sp.input"))
            completion = span.get("output", attrs.get("sp.output"))
            if prompt is not None:
                events.append((start, span_id, {
                    "span_id": span_id, "parent_span_id": parent_span_id,
                    "timestamp": start, "kind": "user_message", "content": _text_value(prompt),
                }))
            if completion is not None:
                events.append((end, span_id, {
                    "span_id": span_id, "parent_span_id": parent_span_id,
                    "timestamp": end, "kind": "assistant_message", "content": _text_value(completion),
                }))
    events.sort(key=lambda item: (item[0], item[1]))
    previous_tool_timestamp = None
    normalized = []
    for sequence, (timestamp, _span_id, event) in enumerate(events[:1000]):
        if event["kind"] in {"tool_call", "tool_result"}:
            if timestamp == previous_tool_timestamp:
                tool_order_certain = False
            previous_tool_timestamp = timestamp
        normalized.append({"sequence": sequence, **event})
    return normalized, tool_events_captured, tool_order_certain


def _text_value(value: object) -> str:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return value[:20_000]
    if isinstance(value, dict):
        content = value.get("content") or value.get("text") or value.get("messages")
        if content is not None:
            return _text_value(content)
    return json.dumps(value, ensure_ascii=False)[:20_000]


def _session_timestamp(spans: list[dict]) -> str:
    candidates = [str(span.get("end_time") or span.get("start_time") or "") for span in spans]
    candidates = [value for value in candidates if value]
    if candidates:
        return max(candidates).replace("+00:00", "Z")
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _evaluation_score_id(evaluator: dict, trace_id: str) -> str:
    return sha256(
        f"{_workspace_id()}:{evaluator['evaluator_id']}:v{evaluator['version']}:{trace_id}".encode()
    ).hexdigest()


def _matches_evaluator_score(score: dict, evaluator: dict, trace_id: str) -> bool:
    return (
        score.get("trace_id") == trace_id
        and score.get("source") == "evaluator"
        and score.get("config_id") == f"evaluator:{evaluator['evaluator_id']}:v{evaluator['version']}"
    )


def _successful_evaluator_score(scores: list[dict], evaluator: dict, trace_id: str) -> dict | None:
    for score in scores:
        if score.get("string_value") == "error":
            continue
        if _matches_evaluator_score(score, evaluator, trace_id):
            return score
    return None


def _next_evaluation_score_id(base_id: str, scores: list[dict]) -> str:
    existing_ids = {score.get("score_id") for score in scores}
    for attempt in range(1, 1000):
        retry_id = sha256(f"{base_id}:retry:{attempt}".encode()).hexdigest()
        if retry_id not in existing_ids:
            return retry_id
    raise RuntimeError("Too many prior evaluation retries for this trace")


def _notification_receipt_path(
    evaluator: dict, trace_id: str, channel: str, thread_ts: str | None
) -> Path:
    identity = json.dumps([
        evaluator.get("evaluator_id"), evaluator.get("version"), trace_id, channel, thread_ts,
    ], separators=(",", ":"))
    digest = sha256(identity.encode("utf-8")).hexdigest()
    home = _workspace_home()
    directory = home / "notifications"
    if directory.is_symlink() or not directory.resolve().is_relative_to(home.resolve()):
        raise ValueError("Notification receipt path escapes Lisa's workspace home or uses a symlink")
    return directory / f"{digest}.json"


def _notify_failure(evaluator: dict, result: dict, session_id: str, trace_id: str) -> dict:
    evaluator_channel = str(evaluator.get("slack_channel_id") or "").strip()
    channel = evaluator_channel or os.getenv("LISA_SLACK_CHANNEL_ID", "").strip()
    if not channel or result.get("status") != "fail":
        return {"status": "not_configured"}
    token = os.getenv("LISA_SLACK_BOT_TOKEN", "")
    if not token:
        return {"status": "failed", "reason": "Slack bot token is not configured"}
    text = (
        f":warning: *Lisa found an agent issue:* {evaluator['name']}\n"
        f"Agent: `{evaluator['target_agent_name']}` · session: `{session_id}`\n"
        f"{redact_text(str(result.get('rationale', 'Evaluation failed.')))}"
    )
    payload = {"channel": channel, "text": text}
    # A thread timestamp is meaningful only with the channel it came from.
    thread_ts = evaluator.get("slack_thread_ts") if evaluator_channel else None
    if thread_ts:
        payload["thread_ts"] = thread_ts
    request = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    receipt_path = _notification_receipt_path(evaluator, trace_id, channel, thread_ts)
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = receipt_path.with_suffix(".lock")
    try:
        with lock_path.open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            if receipt_path.exists():
                return {"status": "already_sent", "session_id": session_id}
            try:
                with urllib.request.urlopen(request, timeout=10) as response:
                    body = json.loads(response.read().decode("utf-8"))
            except Exception as error:
                logger.exception("Lisa Slack notification request failed")
                return {"status": "failed", "reason": redact_text(str(error))}
            if not body.get("ok"):
                return {"status": "failed", "reason": redact_text(str(body.get("error", "Slack rejected the message")))}
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=receipt_path.parent,
                prefix=f".{receipt_path.name}.", suffix=".tmp", delete=False,
            ) as temporary:
                json.dump({"session_id": session_id, "message_ts": body.get("ts")}, temporary)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            temporary_path.replace(receipt_path)
            return {"status": "sent", "channel_id": channel, "message_ts": body.get("ts")}
    except Exception as error:
        logger.exception("Lisa could not persist Slack notification receipt")
        return {"status": "failed", "reason": redact_text(str(error))}


@mcp.tool()
def search_sessions(query: str = "", errors_only: bool = False) -> dict:
    """Search recent workspace sessions, trace evidence, and evaluation results."""
    result = _thelake_post(
        "/internal/v1/lisa/sessions/search",
        {"query": query, "errors_only": errors_only},
    )
    return _hide_evaluation_ids(redact_evidence(result))


def _hide_evaluation_ids(value):
    """Keep implementation identifiers out of Lisa's user-visible evidence."""
    hidden = {
        "evaluation_id", "evaluator_id", "evaluator_version",
        "config_id", "score_id", "run_id",
    }
    if isinstance(value, dict):
        return {
            key: _hide_evaluation_ids(item)
            for key, item in value.items()
            if key not in hidden
        }
    if isinstance(value, list):
        return [_hide_evaluation_ids(item) for item in value]
    return value


def _active_evaluations() -> list[dict]:
    result = _thelake_post("/internal/v1/lisa/evaluators/active", {})
    return result.get("items", [])


def _pending_failures_path() -> Path:
    home = _workspace_home()
    path = home / "pending-evaluation-failures.json"
    if path.is_symlink() or not path.parent.resolve().is_relative_to(home.resolve()):
        raise ValueError("Pending failure record escapes Lisa's workspace home or uses a symlink")
    return path


def _evaluation_lock_path(evaluator: dict, session_id: str) -> Path:
    home = _workspace_home()
    identity = json.dumps(
        [evaluator.get("evaluator_id"), evaluator.get("version"), session_id],
        separators=(",", ":"),
    )
    digest = sha256(identity.encode("utf-8")).hexdigest()
    directory = home / "evaluation-locks"
    if directory.is_symlink() or not directory.resolve().is_relative_to(home.resolve()):
        raise ValueError("Evaluation lock path escapes Lisa's workspace home or uses a symlink")
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{digest}.lock"
    if path.is_symlink():
        raise ValueError("Evaluation lock cannot be a symlink")
    return path


def _read_pending_failures(path: Path | None = None) -> list[dict]:
    path = path or _pending_failures_path()
    try:
        items = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    except json.JSONDecodeError as error:
        raise ValueError("Lisa's pending evaluation failure record is invalid") from error
    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        raise ValueError("Lisa's pending evaluation failure record has an invalid shape")
    return items


def _change_pending_failures(change) -> list[dict]:
    path = _pending_failures_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(".lock")
    if lock_path.is_symlink():
        raise ValueError("Pending failure lock cannot be a symlink")
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        pending = _read_pending_failures(path)
        pending = change(pending)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent,
            prefix=".pending-failures-", delete=False,
        ) as temporary:
            json.dump(pending, temporary, ensure_ascii=False)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        temporary_path.replace(path)
        return pending


def _record_pending_failure(
    evaluator: dict,
    session_id: str,
    trace_id: str,
    rationale: str,
    timestamp: str | None,
    *,
    score_payload: dict | None = None,
) -> None:
    item = {
        "evaluator_id": evaluator.get("evaluator_id"),
        "version": evaluator.get("version"),
        "name": evaluator.get("name"),
        "target_agent_name": evaluator.get("target_agent_name"),
        "criteria": evaluator.get("criteria"),
        "slack_channel_id": evaluator.get("slack_channel_id"),
        "slack_thread_ts": evaluator.get("slack_thread_ts"),
        "session_id": session_id,
        "trace_id": trace_id,
        "rationale": redact_text(rationale),
        "timestamp": timestamp,
        "state": "pending_score" if score_payload else "ready",
        "score_payload": score_payload,
    }
    key = (item["evaluator_id"], item["version"], item["session_id"], item["trace_id"])

    def upsert(items: list[dict]) -> list[dict]:
        items = [
            existing for existing in items
            if (
                existing.get("evaluator_id"), existing.get("version"),
                existing.get("session_id"), existing.get("trace_id"),
            ) != key
        ]
        if len(items) >= 10_000:
            raise RuntimeError("Lisa's pending failure queue is full; follow-ups need attention")
        items.append(item)
        return items

    _change_pending_failures(upsert)


def _pending_failure_key(item: dict) -> str:
    identity = json.dumps(
        [item.get("evaluator_id"), item.get("version"), item.get("session_id"), item.get("trace_id")],
        separators=(",", ":"),
    )
    return sha256(identity.encode("utf-8")).hexdigest()


def _reconcile_pending_failures() -> list[dict]:
    """Finish score writes interrupted after Lisa saved the failure locally."""
    pending = _read_pending_failures()
    for queued in pending:
        if queued.get("state") != "pending_score":
            continue
        payload = queued.get("score_payload")
        if not isinstance(payload, dict):
            raise ValueError("A pending failure is missing its recoverable score")
        evidence = redact_evidence(_thelake_post(
            "/internal/v1/lisa/sessions/evidence", {"session_id": queued["session_id"]}
        ))
        already_stored = any(
            score.get("score_id") == payload.get("score_id")
            and _matches_evaluator_score(score, queued, str(queued.get("trace_id", "")))
            for score in evidence.get("scores", [])
        )
        if not already_stored:
            _thelake_post("/internal/v1/lisa/scores", payload)

        key = (
            queued.get("evaluator_id"), queued.get("version"),
            queued.get("session_id"), queued.get("trace_id"),
        )

        def mark_ready(items: list[dict]) -> list[dict]:
            for item in items:
                if (
                    item.get("evaluator_id"), item.get("version"),
                    item.get("session_id"), item.get("trace_id"),
                ) == key:
                    item["state"] = "ready"
                    item.pop("score_payload", None)
                    break
            return items

        pending = _change_pending_failures(mark_ready)
    return pending


@mcp.tool()
def list_pending_evaluation_failures() -> list[dict] | str:
    """List stored evaluation failures that still need Lisa's follow-up."""
    items = _reconcile_pending_failures()
    if not items:
        return "No evaluation failures are awaiting follow-up."
    visible = []
    for item in items:
        if not isinstance(item, dict):
            continue
        visible.append({
            "failure_key": _pending_failure_key(item),
            **{
                key: item.get(key)
                for key in ("name", "target_agent_name", "session_id", "rationale", "timestamp")
            },
        })
    return visible or "No evaluation failures are awaiting follow-up."


@mcp.tool()
def complete_evaluation_failure_followup(
    failure_key: str,
) -> dict:
    """Clear a pending failure after Lisa investigated and reported it."""
    if os.getenv("LISA_TURN_SOURCE", "web") not in {"web", "slack", "scheduled"}:
        raise ValueError("This conversation cannot complete an evaluation follow-up")
    counts = {"before": 0, "after": 0}
    completed_session: list[str | None] = [None]

    def remove(items: list[dict]) -> list[dict]:
        counts["before"] = len(items)
        for index, item in enumerate(items):
            if _pending_failure_key(item) == failure_key:
                completed_session[0] = item.get("session_id")
                del items[index]
                break
        counts["after"] = len(items)
        return items

    _change_pending_failures(remove)
    return {
        "status": "completed" if counts["after"] < counts["before"] else "not_pending",
        "session_id": completed_session[0],
    }


@mcp.tool()
def list_active_evaluations() -> list[dict] | str:
    """List named policy evaluations Lisa applies during scheduled QA."""
    items = _active_evaluations()
    visible = []
    seen = set()
    for item in items:
        identity = (item.get("target_agent_name"), item.get("name"), item.get("criteria"))
        if identity in seen:
            continue
        seen.add(identity)
        visible.append({key: item.get(key) for key in ("name", "target_agent_name", "criteria")})
    return visible or "No active evaluations are configured."


@mcp.tool()
def evaluate_session(target_agent_name: str, name: str, session_id: str) -> dict:
    """Apply one named active evaluation to a session and store its result."""
    evaluator = next(
        (
            item for item in _active_evaluations()
            if item.get("target_agent_name") == target_agent_name
            and item.get("name") == name
        ),
        None,
    )
    if not evaluator:
        raise ValueError("No active evaluation with that name and agent exists in Lisa's workspace")
    lock_path = _evaluation_lock_path(evaluator, session_id)
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        return _evaluate_session_locked(target_agent_name, name, session_id)


def _evaluate_session_locked(target_agent_name: str, name: str, session_id: str) -> dict:
    """Recheck TheLake's score ledger and evaluate while holding the session lock."""
    evaluator = next(
        (
            item for item in _active_evaluations()
            if item.get("target_agent_name") == target_agent_name
            and item.get("name") == name
        ),
        None,
    )
    if not evaluator:
        raise ValueError("No active evaluation with that name and agent exists in Lisa's workspace")
    evaluator_id = evaluator["evaluator_id"]
    evidence = _thelake_post(
        "/internal/v1/lisa/sessions/evidence", {"session_id": session_id}
    )
    evidence = redact_evidence(evidence)
    agent_names = evidence.get("agent_names", [])
    if evaluator.get("target_agent_name") not in agent_names:
        return {
            "status": "skipped_agent_mismatch",
            "session_id": session_id,
            "expected_agent": evaluator.get("target_agent_name"),
            "observed_agents": agent_names,
            "stored": False,
        }
    spans = evidence.get("spans", [])
    trace_id = next((str(span.get("trace_id")) for span in spans if span.get("trace_id")), "")
    if not trace_id:
        return {"status": "insufficient_evidence", "session_id": session_id, "stored": False}
    scores = evidence.get("scores", [])
    run_id = _evaluation_score_id(evaluator, trace_id)
    completed = _successful_evaluator_score(scores, evaluator, trace_id)
    if completed:
        if completed.get("string_value") == "fail":
            _record_pending_failure(
                evaluator, session_id, trace_id,
                str(completed.get("comment") or "Evaluation failed."),
                completed.get("timestamp"),
            )
        return {
            "status": "already_evaluated",
            "verdict": completed.get("string_value"),
            "rationale": completed.get("comment"),
            "timestamp": completed.get("timestamp"),
            "session_id": session_id,
            "stored": True,
        }
    if any(score.get("score_id") == run_id for score in scores):
        run_id = _next_evaluation_score_id(run_id, scores)
    events, tool_events_captured, tool_order_certain = _span_events(spans)
    if not events:
        return {"status": "insufficient_evidence", "session_id": session_id, "stored": False}
    truncated = evidence.get("evidence_limited", False) or any(
        "[truncated]" in str(event.get("content", ""))
        or "[truncated]" in str(event.get("payload", ""))
        for event in events
    )
    limitations = []
    if truncated:
        limitations.append("TheLake returned bounded or truncated session evidence.")
    result = run_evaluation({
        "run_id": run_id,
        "evaluator": {
            "evaluator_id": evaluator_id,
            "version": evaluator["version"],
            "name": evaluator["name"],
            "criteria": evaluator["criteria"],
            "threshold": evaluator.get("threshold", 0.7),
            "uncertainty_margin": evaluator.get("uncertainty_margin", 0.1),
            "required_tool_order": evaluator.get("required_tool_order", []),
        },
        "evidence": {
            "trace_id": trace_id,
            "events": events,
            "complete": not truncated,
            "tool_events_captured": tool_events_captured,
            "tool_order_certain": tool_order_certain,
            "limitations": limitations,
        },
    })
    result = redact_evidence(result)
    result = _hide_evaluation_ids(result)
    if result.get("status") == "error":
        return {
            **result,
            "evaluation_name": evaluator["name"],
            "session_id": session_id,
            "stored": False,
        }
    limitations.extend(result.get("limitations", []))
    timestamp = _session_timestamp(spans)
    score_payload = {
        "score_id": run_id,
        "timestamp": timestamp,
        "trace_id": trace_id,
        "session_id": session_id,
        "name": evaluator["name"],
        "data_type": "categorical",
        "numeric_value": None,
        "string_value": result["status"],
        "boolean_value": None,
        "source": "evaluator",
        "comment": redact_text(str(result.get("rationale", ""))),
        "config_id": f"evaluator:{evaluator_id}:v{evaluator['version']}",
        "author_id": None,
        "metadata": {
            "run_id": run_id,
            "framework": result.get("framework", "deepeval"),
            "judge_score": str(result.get("score", "")),
            "threshold": str(evaluator.get("threshold", 0.7)),
            "evidence": json.dumps(result.get("evidence", [])),
            "limitations": json.dumps(limitations),
        },
    }
    if result.get("status") == "fail":
        _record_pending_failure(
            evaluator, session_id, trace_id,
            str(result.get("rationale") or "Evaluation failed."), timestamp,
            score_payload=score_payload,
        )
    stored, status_code = _thelake_post(
        "/internal/v1/lisa/scores", score_payload, include_status=True
    )
    if result.get("status") == "fail":
        key = (evaluator_id, evaluator["version"], session_id, trace_id)

        def mark_ready(items: list[dict]) -> list[dict]:
            for item in items:
                if (
                    item.get("evaluator_id"), item.get("version"),
                    item.get("session_id"), item.get("trace_id"),
                ) == key:
                    item["state"] = "ready"
                    item.pop("score_payload", None)
                    break
            return items

        _change_pending_failures(mark_ready)
    return {
        **result,
        "evaluation_name": evaluator["name"],
        "session_id": session_id,
        "stored": True,
        "new_score": status_code == 201,
    }


@mcp.tool()
def notify_evaluation_failure(
    target_agent_name: str,
    name: str,
    session_id: str,
    failure_key: str = "",
) -> dict:
    """Send a Slack alert for a stored failing score after Lisa reviews the evidence."""
    pending = _reconcile_pending_failures()
    evaluator = next(
        (
            item for item in pending
            if (not failure_key or _pending_failure_key(item) == failure_key)
            and item.get("target_agent_name") == target_agent_name
            and item.get("name") == name
            and item.get("session_id") == session_id
        ),
        None,
    )
    if not evaluator:
        if failure_key:
            return {"status": "not_pending", "session_id": session_id}
        evaluators = _active_evaluations()
        evaluator = next(
            (
                item for item in evaluators
                if item.get("target_agent_name") == target_agent_name
                and item.get("name") == name
            ),
            None,
        )
    if not evaluator:
        raise ValueError("No active or pending evaluation with that name and agent exists in Lisa's workspace")

    evidence = redact_evidence(_thelake_post(
        "/internal/v1/lisa/sessions/evidence", {"session_id": session_id}
    ))
    if evaluator.get("target_agent_name") not in evidence.get("agent_names", []):
        return {"status": "skipped_agent_mismatch", "session_id": session_id}
    spans = evidence.get("spans", [])
    trace_id = str(evaluator.get("trace_id") or "")
    if not trace_id:
        trace_id = next((str(span.get("trace_id")) for span in spans if span.get("trace_id")), "")
    elif not any(str(span.get("trace_id") or "") == trace_id for span in spans):
        return {"status": "trace_not_found", "session_id": session_id}
    if not trace_id:
        return {"status": "insufficient_evidence", "session_id": session_id}
    score = next(
        (
            item for item in evidence.get("scores", [])
            if item.get("string_value") == "fail"
            and _matches_evaluator_score(item, evaluator, trace_id)
        ),
        None,
    )
    if not score or score.get("string_value") != "fail":
        return {"status": "not_a_stored_failure", "session_id": session_id}
    result = {
        "status": "fail",
        "rationale": score.get("comment", "Lisa found a behavior check failure."),
    }
    return _notify_failure(evaluator, result, session_id, trace_id)


def _evaluation_id(target_agent_name: str, name: str) -> str:
    identity = f"{target_agent_name.strip()}\0{name.strip()}".encode("utf-8")
    return f"evaluation-{hashlib.sha256(identity).hexdigest()[:24]}"


def _require_owner_turn() -> None:
    if os.getenv("LISA_TURN_SOURCE", "web") not in {"web", "slack"}:
        raise ValueError("Only an owner conversation can create or activate an evaluation")


def _owner_safe_evaluation(result: dict, *, status: str) -> dict:
    """Keep storage identifiers and version bookkeeping out of Lisa's chat."""
    return {
        "name": result.get("name"),
        "target_agent_name": result.get("target_agent_name"),
        "status": status,
    }


@mcp.tool()
def create_evaluation_draft(
    target_agent_name: str,
    name: str,
) -> dict:
    """Create or reuse an inactive evaluation from an owner-approved named policy."""
    _require_owner_turn()
    approved = (
        os.getenv("LISA_EVALUATION_APPROVAL_TITLE", "").strip(),
        os.getenv("LISA_EVALUATION_APPROVAL_AGENT", "").strip(),
    )
    if (name.strip(), target_agent_name.strip()) != approved or not all(approved):
        raise ValueError(
            "First explain the named policy and exact target agent, then wait for the "
            "owner's exact command: Create evaluation: <exact policy name> for agent: "
            "<exact agent name>"
        )
    agent_rules = _policy_rules(read_policy("agent", target_agent_name))
    workspace_rules = _policy_rules(read_policy("workspace"))
    policy = agent_rules.get(name.strip()) or workspace_rules.get(name.strip())
    if not policy:
        raise ValueError("No policy with that exact name exists for this agent or workspace")
    criteria = policy.strip()
    if len(criteria) > 8000:
        raise ValueError("The named policy is too long to use as an evaluation rubric")
    channel_id = os.getenv("LISA_SLACK_CHANNEL_ID", "").strip()
    thread_ts = os.getenv("LISA_SLACK_THREAD_TS", "").strip()
    result = _thelake_post(
        "/internal/v1/lisa/evaluators",
        {
            "evaluator": {
                "evaluator_id": _evaluation_id(target_agent_name, name),
                "version": 0,
                "target_agent_name": target_agent_name,
                "name": name,
                "criteria": redact_text(criteria),
            },
            "slack_channel_id": channel_id or None,
            "slack_thread_ts": thread_ts or None,
        },
    )
    return {
        **_owner_safe_evaluation(result, status="active" if result.get("active") else "draft"),
        "criteria": criteria,
    }


@mcp.tool()
def activate_evaluation(target_agent_name: str, name: str) -> dict:
    """Activate a named evaluation only after the owner explicitly requests it."""
    _require_owner_turn()
    approved = (
        os.getenv("LISA_ACTIVATION_TITLE", "").strip(),
        os.getenv("LISA_ACTIVATION_AGENT", "").strip(),
    )
    if (name.strip(), target_agent_name.strip()) != approved or not all(approved):
        raise ValueError(
            "First obtain the owner's exact command: Activate evaluation: "
            "<exact policy name> for agent: <exact agent name>"
        )
    result = _thelake_post(
        "/internal/v1/lisa/evaluators/activate",
        {
            "evaluator_id": _evaluation_id(target_agent_name, name),
            "version": 0,
            "target_agent_name": target_agent_name,
            "name": name,
        },
    )
    return _owner_safe_evaluation(result, status="active")


if __name__ == "__main__":
    mcp.run()
