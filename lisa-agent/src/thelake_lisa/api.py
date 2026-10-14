from __future__ import annotations

import asyncio
import base64
import hmac
import json
import logging
import os
import re
import shutil
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

from thelake_lisa.runtime import (
    confirmed_policy_title,
    initialize_backend,
    run_turn,
)
from thelake_lisa.security import redact_text
from thelake_lisa.slack import SlackClient


@asynccontextmanager
async def lifespan(_app: FastAPI):
    initialize_backend()
    workspace_id = os.getenv("LISA_WORKSPACE_ID", "").strip()
    qa_scheduler = None
    slack_worker = None
    if workspace_id:
        _recover_slack_receipts(workspace_id)
        cron_schedule = os.getenv("LISA_QA_CRON", "0 * * * *").strip()
        try:
            trigger = CronTrigger.from_crontab(cron_schedule, timezone="UTC")
        except ValueError as error:
            raise RuntimeError(
                "LISA_QA_CRON must be a valid five-field UTC cron expression"
            ) from error
        qa_scheduler = AsyncIOScheduler(timezone="UTC")
        job = qa_scheduler.add_job(
            _run_scheduled_qa,
            trigger=trigger,
            args=[workspace_id],
            id="lisa-scheduled-qa",
            name="Lisa scheduled QA",
            coalesce=True,
            max_instances=1,
            misfire_grace_time=900,
            replace_existing=True,
        )
        qa_scheduler.start()
        logger.info(
            "Registered cron job '%s' with schedule '%s' UTC; next run at %s",
            job.name,
            cron_schedule,
            job.next_run_time,
        )
        slack_worker = asyncio.create_task(_slack_event_loop(workspace_id))
    yield
    if qa_scheduler is not None:
        qa_scheduler.shutdown(wait=False)
    if slack_worker is not None:
        slack_worker.cancel()
        try:
            await slack_worker
        except asyncio.CancelledError:
            pass
    active_slack_tasks = list(_slack_tasks.values())
    for task in active_slack_tasks:
        task.cancel()
    if active_slack_tasks:
        await asyncio.gather(*active_slack_tasks, return_exceptions=True)
    _slack_tasks.clear()


app = FastAPI(title="Lisa Agent Runtime", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        origin.strip()
        for origin in os.getenv("LISA_CORS_ORIGINS", "").split(",")
        if origin.strip()
    ],
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Softprobe-Assertion"],
)
logger = logging.getLogger(__name__)
DATA_DIR = Path(os.getenv("LISA_DATA_DIR", "/var/lib/lisa/workspaces")).resolve()
SERVICE_TOKEN = os.getenv("LISA_THELAKE_TOKEN", "")
_locks: dict[str, asyncio.Lock] = {}
_slack_tasks: dict[str, asyncio.Task] = {}
class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CreateConversation(StrictModel):
    workspace_id: str
    conversation_id: str


class WorkspaceRequest(StrictModel):
    workspace_id: str


class SendMessage(StrictModel):
    workspace_id: str
    text: str = Field(min_length=1, max_length=16_000)
    consent: bool


class UserSendMessage(StrictModel):
    text: str = Field(min_length=1, max_length=16_000)
    consent: bool


class EmptyRequest(StrictModel):
    pass


def workspace_directory(workspace_id: str) -> Path:
    try:
        normalized = str(UUID(workspace_id))
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Invalid workspace scope") from error
    return DATA_DIR / sha256(normalized.encode()).hexdigest()


def conversation_directory(workspace_id: str, conversation_id: str) -> Path:
    try:
        conversation_key = str(UUID(conversation_id))
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Invalid conversation ID") from error
    path = workspace_directory(workspace_id) / conversation_key
    if not path.resolve().is_relative_to(DATA_DIR):
        raise HTTPException(status_code=400, detail="Invalid conversation path")
    return path


def metadata_path(directory: Path) -> Path:
    return directory / "lisa.json"


def load_metadata(directory: Path, workspace_id: str) -> dict[str, Any]:
    try:
        metadata = json.loads(metadata_path(directory).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError) as error:
        raise HTTPException(status_code=404, detail="Conversation not found") from error
    if metadata.get("workspace_id_hash") != sha256(str(UUID(workspace_id)).encode()).hexdigest():
        raise HTTPException(status_code=404, detail="Conversation not found")
    return metadata


def save_metadata(directory: Path, metadata: dict[str, Any]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    target = metadata_path(directory)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(metadata, ensure_ascii=False), encoding="utf-8")
    temporary.replace(target)


def lock_for(directory: Path) -> asyncio.Lock:
    return _locks.setdefault(str(directory), asyncio.Lock())


def _main_thread_marker(workspace_id: str) -> Path:
    return workspace_directory(workspace_id) / "main-thread.json"


async def main_conversation(workspace_id: str) -> dict[str, Any]:
    """Return Lisa's one durable workspace conversation, migrating an old chat if present."""
    normalized = str(UUID(workspace_id))
    root = workspace_directory(normalized)
    root.mkdir(parents=True, exist_ok=True)
    async with lock_for(root / "main-thread.lock"):
        marker = _main_thread_marker(normalized)
        marked_item: dict[str, Any] | None = None
        try:
            conversation_id = str(UUID(json.loads(marker.read_text(encoding="utf-8"))["conversation_id"]))
            marked_item = load_metadata(conversation_directory(normalized, conversation_id), normalized)
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError, HTTPException):
            pass
        if marked_item and (marked_item.get("messages") or marked_item.get("activities")):
            item = _prepare_main_thread(marked_item)
            save_metadata(conversation_directory(normalized, item["conversation_id"]), item)
            return {**item, "messages": item.get("messages", []), "activities": item.get("activities", [])}

        # Prefer the latest web conversation. If Lisa only has scheduled QA
        # history, adopt that transcript so the single interface doesn't start
        # blank; its disposable OpenHands state is not mistaken for continuity.
        web_candidates: list[dict[str, Any]] = []
        scheduled_candidates: list[dict[str, Any]] = []
        for path in root.iterdir():
            if not path.is_dir() or not re.fullmatch(r"[0-9a-f-]{36}", path.name):
                continue
            try:
                item = load_metadata(path, normalized)
            except HTTPException:
                continue
            if item.get("event_source") == "schedule":
                scheduled_candidates.append(item)
            elif not item.get("event_source"):
                web_candidates.append(item)
        has_history = lambda item: bool(item.get("messages") or item.get("activities"))
        web_candidates.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
        scheduled_candidates.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
        if any(has_history(item) for item in web_candidates):
            item = next(item for item in web_candidates if has_history(item))
        elif any(has_history(item) for item in scheduled_candidates):
            item = next(item for item in scheduled_candidates if has_history(item))
        elif web_candidates:
            item = web_candidates[0]
        elif marked_item:
            item = marked_item
        else:
            item = {}
        if item:
            conversation_id = str(UUID(item["conversation_id"]))
        else:
            conversation_id = str(uuid5(NAMESPACE_URL, f"lisa:main:{UUID(normalized)}"))
            item = await create_conversation(CreateConversation(
                workspace_id=normalized,
                conversation_id=conversation_id,
            ))
        item = _prepare_main_thread(item)
        save_metadata(conversation_directory(normalized, conversation_id), item)
        marker.write_text(json.dumps({"conversation_id": conversation_id}), encoding="utf-8")
        return {**item, "messages": item.get("messages", []), "activities": item.get("activities", [])}


def _prepare_main_thread(item: dict[str, Any]) -> dict[str, Any]:
    """Normalize old scheduled transcripts without exposing their internal prompts."""
    if item.get("event_source") == "schedule":
        for message in item.get("messages", []):
            message["turn_source"] = "scheduled"
            if message.get("role") == "user":
                message["hidden"] = True
            # Old scheduled cycles used disposable event streams, so their
            # sequence values cannot be compared across cycles.
            message.pop("sequence", None)
        for activity in item.get("activities", []):
            activity.pop("sequence", None)
            activity["turn_source"] = "scheduled"
        item["transcript_only_migration"] = True
    item["title"] = "Lisa"
    item.pop("event_source", None)
    return item


def browser_conversation(item: dict[str, Any]) -> dict[str, Any]:
    """Keep internal scheduled prompts in Lisa's memory, outside the browser payload."""
    return {
        **item,
        "messages": [message for message in item.get("messages", []) if not message.get("hidden")],
    }


def _tag_turn(
    messages: list[dict[str, Any]],
    activities: list[dict[str, Any]],
    previous_messages: list[dict[str, Any]],
    turn_source: str,
    hidden_message_ids: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    known_sources = {item.get("id"): item.get("turn_source") for item in previous_messages}
    previously_hidden = {item.get("id") for item in previous_messages if item.get("hidden")}
    previous_ids = set(known_sources)
    for message in messages:
        if message.get("id") not in previous_ids:
            known_sources[message.get("id")] = turn_source
        source = known_sources.get(message.get("id")) or message.get("turn_source") or "web"
        message["turn_source"] = source
        if message.get("id") in previously_hidden or (
            source == "scheduled" and message.get("role") == "user"
        ):
            hidden_message_ids.add(str(message.get("id", "")))
    for activity in activities:
        activity["turn_source"] = turn_source
    return messages, activities, hidden_message_ids


def _merge_transcript_history(
    previous_messages: list[dict[str, Any]], current_messages: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Keep migrated records that OpenHands cannot reload as conversation state."""
    merged: list[dict[str, Any]] = []
    positions: dict[str, int] = {}
    for message in [*previous_messages, *current_messages]:
        message_id = str(message.get("id", ""))
        if message_id and message_id in positions:
            merged[positions[message_id]] = message
        else:
            if message_id:
                positions[message_id] = len(merged)
            merged.append(message)
    return merged


async def _run_turn_without_releasing_lock_early(**kwargs: Any):
    """Keep the conversation lock until its worker thread stops, even on cancellation."""
    # Keep the executor future out of asyncio's task set: request cancellation
    # must not cancel the awaitable while its thread continues using OpenHands.
    worker = asyncio.get_running_loop().run_in_executor(
        None, lambda: run_turn(**kwargs)
    )
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        # Complete the turn and let the caller save its transcript before it
        # releases the lock; otherwise OpenHands state and Lisa metadata diverge.
        while True:
            try:
                return await asyncio.shield(worker)
            except asyncio.CancelledError:
                if worker.done():
                    if worker.cancelled():
                        raise RuntimeError("Lisa turn worker was cancelled before completion")
                    return worker.result()
                continue


async def require_service_token(authorization: str | None = Header(default=None)) -> None:
    if not SERVICE_TOKEN or not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Service authentication required")
    candidate = authorization.removeprefix("Bearer ").strip()
    if not hmac.compare_digest(candidate, SERVICE_TOKEN):
        raise HTTPException(status_code=401, detail="Service authentication required")


def _decode_assertion_part(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _workspace_for_assertion(token: str) -> str | None:
    secret = os.getenv("SOFTPROBE_ASSERTION_HMAC_SECRET") or os.getenv("ASSERTION_HMAC_SECRET")
    if not secret:
        return None
    parts = token.strip().split(".")
    if len(parts) != 3:
        return None
    try:
        header = json.loads(_decode_assertion_part(parts[0]))
        claims = json.loads(_decode_assertion_part(parts[1]))
        signature = _decode_assertion_part(parts[2])
    except (ValueError, json.JSONDecodeError):
        return None
    if header.get("alg") != "HS256":
        return None
    expected = hmac.new(secret.encode(), f"{parts[0]}.{parts[1]}".encode(), sha256).digest()
    if not hmac.compare_digest(expected, signature):
        return None
    audience = claims.get("aud")
    audiences = audience if isinstance(audience, list) else [audience]
    if claims.get("iss") != "softprobe-edge" or "sp-backend" not in audiences:
        return None
    if not str(claims.get("sub", "")).strip():
        return None
    expires = claims.get("exp")
    if expires is not None and (not isinstance(expires, (int, float)) or expires < datetime.now(UTC).timestamp()):
        return None
    try:
        workspace_id = str(UUID(str(claims.get("workspace_id", ""))))
        configured_workspace = str(UUID(os.environ["LISA_WORKSPACE_ID"]))
    except (ValueError, KeyError):
        return None
    return workspace_id if workspace_id == configured_workspace else None


def user_workspace(request: Request) -> str:
    try:
        configured_workspace = str(UUID(os.environ["LISA_WORKSPACE_ID"]))
    except (ValueError, KeyError) as error:
        raise HTTPException(status_code=503, detail="Lisa workspace is not configured") from error
    if os.getenv("LISA_LOCAL_ANONYMOUS") == "1":
        return configured_workspace
    assertion = request.headers.get("x-softprobe-assertion", "")
    workspace_id = _workspace_for_assertion(assertion)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Workspace authentication required")
    return workspace_id


@app.get("/health")
async def health() -> dict[str, bool]:
    base_url = os.getenv("LISA_BASE_URL")
    configured = bool(
        (base_url and os.getenv("LISA_MODEL"))
        or (not base_url and (os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")))
    )
    return {"ok": True, "model_configured": configured, "service_auth_configured": bool(SERVICE_TOKEN)}


@app.get("/v1/conversations")
async def user_list_conversations(request: Request) -> list[dict[str, Any]]:
    return [browser_conversation(await main_conversation(user_workspace(request)))]


@app.post("/v1/conversations")
async def user_create_conversation(
    request: Request, body: EmptyRequest | None = None
) -> dict[str, Any]:
    del body
    return browser_conversation(await main_conversation(user_workspace(request)))


@app.get("/v1/conversations/{conversation_id}")
async def user_get_conversation(conversation_id: str, request: Request) -> dict[str, Any]:
    main = await main_conversation(user_workspace(request))
    if str(UUID(conversation_id)) != main["conversation_id"]:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return browser_conversation(await get_conversation(conversation_id, user_workspace(request)))


@app.delete("/v1/conversations/{conversation_id}")
async def user_delete_conversation(conversation_id: str, request: Request) -> dict[str, bool]:
    main = await main_conversation(user_workspace(request))
    if str(UUID(conversation_id)) != main["conversation_id"]:
        raise HTTPException(status_code=404, detail="Conversation not found")
    raise HTTPException(status_code=409, detail="Lisa's main conversation is persistent")


@app.post("/v1/conversations/{conversation_id}/messages")
async def user_send_message(
    conversation_id: str, request: Request, message: UserSendMessage
) -> dict[str, Any]:
    main = await main_conversation(user_workspace(request))
    if str(UUID(conversation_id)) != main["conversation_id"]:
        raise HTTPException(status_code=404, detail="Conversation not found")
    result = await send_message(
        conversation_id,
        SendMessage(workspace_id=user_workspace(request), text=message.text, consent=message.consent),
    )
    return browser_conversation(result)


def _slack_event_path(workspace_id: str, event_id: str) -> Path:
    digest = sha256(event_id.encode("utf-8")).hexdigest()
    directory = workspace_directory(workspace_id) / "slack_events"
    if directory.is_symlink() or not directory.resolve().is_relative_to(DATA_DIR):
        raise HTTPException(status_code=400, detail="Invalid Slack event path")
    return directory / f"{digest}.json"


def _save_slack_event(path: Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(event, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def _slack_signature_valid(secret: str, timestamp: str, signature: str, body: bytes) -> bool:
    try:
        timestamp_number = int(timestamp)
    except ValueError:
        return False
    if abs(datetime.now(UTC).timestamp() - timestamp_number) > 300:
        return False
    expected = "v0=" + hmac.new(
        secret.encode("utf-8"), b"v0:" + timestamp.encode() + b":" + body, sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def _slack_user_allowed(user_id: str, channel_id: str) -> bool:
    users = {item.strip() for item in os.getenv("LISA_SLACK_ALLOWED_USERS", "").split(",") if item.strip()}
    channels = {item.strip() for item in os.getenv("LISA_SLACK_ALLOWED_CHANNELS", "").split(",") if item.strip()}
    return user_id in users or channel_id in channels


def _slack_conversation_id(workspace_id: str, channel_id: str, thread_ts: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"lisa:slack:{UUID(workspace_id)}:{channel_id}:{thread_ts}"))


def _recover_slack_receipts(workspace_id: str) -> None:
    directory = workspace_directory(workspace_id) / "slack_events"
    if not directory.is_dir() or directory.is_symlink():
        return
    for path in directory.glob("*.json"):
        try:
            receipt = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if receipt.get("status") == "processing":
            receipt["status"] = "pending"
            _save_slack_event(path, receipt)


async def _slack_event_loop(workspace_id: str) -> None:
    root = workspace_directory(workspace_id) / "slack_events"
    while True:
        if root.is_dir() and not root.is_symlink():
            for path in root.glob("*.json"):
                key = str(path)
                task = _slack_tasks.get(key)
                if task is not None and task.done():
                    _slack_tasks.pop(key, None)
                    task = None
                if task is None:
                    try:
                        receipt = json.loads(path.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError):
                        continue
                    if receipt.get("status") == "pending":
                        _slack_tasks[key] = asyncio.create_task(_process_slack_event(path))
        await asyncio.sleep(1)


def _post_slack_reply(channel_id: str, thread_ts: str, text: str) -> None:
    SlackClient(os.getenv("LISA_SLACK_BOT_TOKEN", "")).post_message(
        channel_id, text, thread_ts, timeout=15,
    )


async def _process_slack_event(path: Path) -> None:
    async with lock_for(path):
        try:
            receipt = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if receipt.get("status") != "pending":
            return
        receipt["status"] = "processing"
        receipt["updated_at"] = datetime.now(UTC).isoformat()
        _save_slack_event(path, receipt)
        workspace_id = receipt["workspace_id"]
        channel_id = receipt["channel_id"]
        thread_ts = receipt["thread_ts"]
        conversation_id = _slack_conversation_id(workspace_id, channel_id, thread_ts)
        directory = conversation_directory(workspace_id, conversation_id)
        try:
            async with lock_for(directory):
                if metadata_path(directory).exists():
                    metadata = load_metadata(directory, workspace_id)
                else:
                    now = datetime.now(UTC).isoformat()
                    metadata = {
                        "conversation_id": conversation_id,
                        "workspace_id_hash": sha256(str(UUID(workspace_id)).encode()).hexdigest(),
                        "title": receipt["text"][:60],
                        "created_at": now,
                        "updated_at": now,
                    }
                turn = await asyncio.to_thread(
                    run_turn,
                    conversation_id=conversation_id,
                    directory=str(directory),
                    workspace_id=workspace_id,
                    text=receipt["text"],
                    turn_source="slack",
                    confirmed_policy_title=confirmed_policy_title(receipt["text"]),
                    slack_channel_id=channel_id,
                    slack_thread_ts=thread_ts,
                )
                latest_reply = next(
                    (message.get("text", "") for message in reversed(turn.messages)
                     if message.get("role") == "assistant"),
                    "Lisa could not prepare a reply.",
                )
                metadata["event_source"] = "slack"
                metadata["updated_at"] = datetime.now(UTC).isoformat()
                metadata["messages"] = turn.messages
                metadata["activities"] = [*metadata.get("activities", []), *turn.activities][-100:]
                save_metadata(directory, metadata)
            await asyncio.to_thread(_post_slack_reply, channel_id, thread_ts, latest_reply)
            receipt["status"] = "complete"
            receipt["conversation_id"] = conversation_id
            receipt.pop("text", None)
        except Exception as error:
            logger.exception("Lisa Slack event processing failed")
            receipt["status"] = "failed"
            receipt["error"] = redact_text(str(error))
        receipt["updated_at"] = datetime.now(UTC).isoformat()
        _save_slack_event(path, receipt)


@app.post("/slack/events")
async def slack_events(request: Request) -> dict[str, Any]:
    signing_secret = os.getenv("LISA_SLACK_SIGNING_SECRET", "")
    timestamp = request.headers.get("x-slack-request-timestamp", "")
    signature = request.headers.get("x-slack-signature", "")
    body = await request.body()
    if not signing_secret or not _slack_signature_valid(signing_secret, timestamp, signature, body):
        raise HTTPException(status_code=401, detail="Invalid Slack request signature")
    try:
        envelope = json.loads(body)
    except json.JSONDecodeError as error:
        raise HTTPException(status_code=400, detail="Malformed Slack event") from error
    if envelope.get("type") == "url_verification":
        return {"challenge": envelope.get("challenge", "")}
    if envelope.get("type") != "event_callback":
        return {"ok": True}
    if envelope.get("team_id") != os.getenv("LISA_SLACK_TEAM_ID"):
        raise HTTPException(status_code=403, detail="Slack team is not authorized")
    workspace_id = os.getenv("LISA_WORKSPACE_ID", "")
    try:
        workspace_id = str(UUID(workspace_id))
    except ValueError as error:
        raise HTTPException(status_code=503, detail="Lisa workspace is not configured") from error
    event_id = str(envelope.get("event_id", ""))
    event = envelope.get("event") or {}
    kind = event.get("type")
    eligible = kind == "app_mention" or (
        kind == "message" and event.get("thread_ts") and not event.get("subtype")
    )
    if not eligible or event.get("bot_id"):
        return {"ok": True}
    channel_id = str(event.get("channel", ""))
    user_id = str(event.get("user", ""))
    message_ts = str(event.get("ts", ""))
    text = str(event.get("text", "")).strip()
    if not event_id or not channel_id or not message_ts or not text:
        return {"ok": True}
    if not _slack_user_allowed(user_id, channel_id):
        raise HTTPException(status_code=403, detail="Slack author is not authorized")
    thread_ts = str(event.get("thread_ts") or message_ts)
    path = _slack_event_path(workspace_id, event_id)
    if not path.exists():
        _save_slack_event(path, {
            "status": "pending",
            "workspace_id": workspace_id,
            "event_id": event_id,
            "channel_id": channel_id,
            "thread_ts": thread_ts,
            "text": redact_text(text),
            "created_at": datetime.now(UTC).isoformat(),
        })
    return {"ok": True}


@app.post("/internal/v1/scheduled-cycle", dependencies=[Depends(require_service_token)])
async def scheduled_cycle(request: WorkspaceRequest) -> dict[str, Any]:
    return await run_scheduled_cycle(request.workspace_id)


@app.post("/internal/v1/conversations/search", dependencies=[Depends(require_service_token)])
async def list_conversations(request: WorkspaceRequest) -> list[dict[str, Any]]:
    root = workspace_directory(request.workspace_id)
    if not root.exists():
        return []
    rows = []
    for path in root.iterdir():
        if not path.is_dir() or not re.fullmatch(r"[0-9a-f-]{36}", path.name):
            continue
        try:
            item = load_metadata(path, request.workspace_id)
        except HTTPException:
            continue
        rows.append({
            key: item[key]
            for key in (
                "conversation_id", "title", "updated_at", "event_source", "event_status",
                "event_id", "event_error",
            )
            if key in item
        })
    return sorted(rows, key=lambda row: row["updated_at"], reverse=True)


@app.post("/internal/v1/conversations", dependencies=[Depends(require_service_token)])
async def create_conversation(request: CreateConversation) -> dict[str, Any]:
    directory = conversation_directory(request.workspace_id, request.conversation_id)
    if metadata_path(directory).exists():
        existing = load_metadata(directory, request.workspace_id)
        return {**existing, "messages": existing.get("messages", [])}
    now = datetime.now(UTC).isoformat()
    item = {
        "conversation_id": str(UUID(request.conversation_id)),
        "workspace_id_hash": sha256(str(UUID(request.workspace_id)).encode()).hexdigest(),
        "title": "Lisa conversation",
        "created_at": now,
        "updated_at": now,
    }
    save_metadata(directory, item)
    return {**item, "messages": [], "activities": []}


@app.get("/internal/v1/conversations/{conversation_id}", dependencies=[Depends(require_service_token)])
async def get_conversation(conversation_id: str, workspace_id: str) -> dict[str, Any]:
    directory = conversation_directory(workspace_id, conversation_id)
    metadata = load_metadata(directory, workspace_id)
    return {**metadata, "messages": metadata.get("messages", []), "activities": metadata.get("activities", [])}


@app.delete("/internal/v1/conversations/{conversation_id}", dependencies=[Depends(require_service_token)])
async def delete_conversation(conversation_id: str, workspace_id: str) -> dict[str, bool]:
    directory = conversation_directory(workspace_id, conversation_id)
    load_metadata(directory, workspace_id)
    async with lock_for(directory):
        load_metadata(directory, workspace_id)
        await asyncio.to_thread(shutil.rmtree, directory)
    return {"deleted": True}


@app.post("/internal/v1/conversations/{conversation_id}/messages", dependencies=[Depends(require_service_token)])
async def send_message(conversation_id: str, request: SendMessage) -> dict[str, Any]:
    if not request.consent:
        raise HTTPException(status_code=403, detail="Provider data consent is required")
    text = redact_text(request.text.strip())
    if not text:
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    directory = conversation_directory(request.workspace_id, conversation_id)
    metadata = load_metadata(directory, request.workspace_id)
    async with lock_for(directory):
        metadata = load_metadata(directory, request.workspace_id)
        previous_messages = metadata.get("messages", [])
        try:
            turn = await _run_turn_without_releasing_lock_early(
                conversation_id=conversation_id,
                directory=str(directory),
                workspace_id=request.workspace_id,
                text=text,
                turn_source="web",
                confirmed_policy_title=confirmed_policy_title(text),
            )
        except RuntimeError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error
        except Exception as error:  # Provider/runtime diagnostics stay in container logs.
            raise HTTPException(status_code=502, detail="Lisa could not complete this turn") from error
        if metadata["title"] == "Lisa conversation":
            metadata["title"] = "Lisa"
        hidden_ids = {
            str(message["id"])
            for message in previous_messages
            if message.get("hidden") and message.get("id")
        }
        messages, activities, hidden_ids = _tag_turn(
            turn.messages, turn.activities, previous_messages, "web", hidden_ids
        )
        for message in messages:
            message["hidden"] = message.get("id") in hidden_ids
        if metadata.get("transcript_only_migration"):
            messages = _merge_transcript_history(previous_messages, messages)
        metadata["updated_at"] = datetime.now(UTC).isoformat()
        metadata["provider_consent_at"] = datetime.now(UTC).isoformat()
        metadata["messages"] = messages
        metadata["activities"] = [*metadata.get("activities", []), *activities][-100:]
        save_metadata(directory, metadata)
        return {
            "conversation_id": conversation_id,
            "messages": messages,
            "activities": activities,
        }


SCHEDULED_PROMPT = """This scheduled turn is Lisa's QA work in her ongoing workspace conversation. Follow the own-agent-qa skill using workspace tools. Resume pending failures, inspect recent sessions, and evaluate relevant POLICY.md rules marked approved: yes. Rules marked approved: no or missing approval are proposals, not enforceable business requirements. Use recent QA updates in this conversation to avoid repeating unchanged proposals. Do not ask the owner to approve routine QA work. Do not create or change policy during a scheduled turn. Report evidenced risks, uncertainty, and coverage limits. Never claim unevaluated sessions passed."""


async def run_scheduled_cycle(workspace_id: str) -> dict[str, Any]:
    """Have Lisa run scheduled QA in her continuing workspace conversation."""
    try:
        normalized_workspace_id = str(UUID(workspace_id))
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Invalid workspace scope") from error
    main = await main_conversation(normalized_workspace_id)
    conversation_id = main["conversation_id"]
    directory = conversation_directory(normalized_workspace_id, conversation_id)
    async with lock_for(directory):
        metadata = load_metadata(directory, normalized_workspace_id)
        previous_messages = metadata.get("messages", [])
        try:
            turn = await _run_turn_without_releasing_lock_early(
                conversation_id=conversation_id,
                directory=str(directory),
                workspace_id=normalized_workspace_id,
                text=SCHEDULED_PROMPT,
                turn_source="scheduled",
            )
        except RuntimeError as error:
            logger.exception("Lisa scheduled QA cycle failed to start")
            raise HTTPException(status_code=503, detail="Lisa could not start scheduled QA") from error
        except Exception as error:
            logger.exception("Lisa scheduled QA cycle failed")
            raise HTTPException(status_code=502, detail="Lisa could not complete scheduled QA") from error
        hidden_ids = {
            str(message["id"])
            for message in previous_messages
            if message.get("hidden") and message.get("id")
        }
        messages, activities, hidden_ids = _tag_turn(
            turn.messages, turn.activities, previous_messages, "scheduled", hidden_ids
        )
        for message in messages:
            message["hidden"] = message.get("id") in hidden_ids
        if metadata.get("transcript_only_migration"):
            messages = _merge_transcript_history(previous_messages, messages)
        metadata["updated_at"] = datetime.now(UTC).isoformat()
        metadata["messages"] = messages
        metadata["activities"] = [*metadata.get("activities", []), *activities][-100:]
        save_metadata(directory, metadata)
        return {
            "conversation_id": conversation_id,
            "messages": messages,
            "activities": activities,
        }


async def _run_scheduled_qa(workspace_id: str) -> None:
    try:
        result = await run_scheduled_cycle(str(UUID(workspace_id)))
        logger.info(
            "Cron job 'Lisa scheduled QA' completed for conversation %s",
            result["conversation_id"],
        )
    except Exception:
        logger.exception("Cron job 'Lisa scheduled QA' failed")
