from __future__ import annotations

import base64
import hmac
import json
import logging
import os
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from lisa.bootstrap import WorkspaceServices
from lisa.models.contracts import (
    ApprovePolicySuggestion,
    CreateConversation,
    DismissPolicySuggestion,
    EmptyRequest,
    SendMessage,
    UserSendMessage,
    WorkspaceRequest,
)
from lisa.repositories.conversations import (
    ConversationNotFound,
)
from lisa.repositories.session_traces import SessionTraceConflict
from lisa.runtime import run_turn
from lisa.security import redact_text
from lisa.services.conversations import (
    ConversationTurnError,
)
from lisa.services.session_trace_intake import SessionTraceQueueUnavailable
from lisa.views.conversations import conversation_summary

router = APIRouter()
logger = logging.getLogger(__name__)
SERVICE_TOKEN = os.getenv("LISA_SERVICE_TOKEN", "")


def _services(request: Request) -> WorkspaceServices:
    services = getattr(request.app.state, "workspace_services", None)
    if services is None:
        raise HTTPException(status_code=503, detail="Lisa services are not initialized")
    return services


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
    if expires is not None and (
        not isinstance(expires, (int, float)) or expires < datetime.now(UTC).timestamp()
    ):
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


def _user_actor_id(request: Request) -> str:
    if os.getenv("LISA_LOCAL_ANONYMOUS") == "1":
        return os.getenv("LISA_LOCAL_ACTOR_ID", "local").strip()
    try:
        token = request.headers["x-softprobe-assertion"]
        return str(json.loads(_decode_assertion_part(token.split(".")[1])).get("sub", "")).strip()
    except (KeyError, IndexError, ValueError, json.JSONDecodeError):
        raise HTTPException(status_code=401, detail="Workspace authentication required")


@router.get("/health")
async def health() -> dict[str, bool]:
    base_url = os.getenv("LISA_BASE_URL")
    configured = bool(
        (base_url and os.getenv("LISA_MODEL"))
        or (not base_url and (os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")))
    )
    return {
        "ok": True,
        "model_configured": configured,
        "service_auth_configured": bool(SERVICE_TOKEN),
    }


@router.post("/internal/v1/session-traces", status_code=202)
async def push_session_traces(request: Request) -> dict[str, str]:
    """Persist one complete OTLP JSON session and enqueue its policy evaluation."""
    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/json":
        raise HTTPException(status_code=415, detail="Expected application/json OTLP JSON")
    services = _services(request)
    intake = services.session_trace_intake
    try:
        max_bytes = int(os.getenv("LISA_MAX_SESSION_TRACE_BYTES", str(16 * 1024 * 1024)))
    except ValueError:
        raise HTTPException(status_code=503, detail="Lisa trace size limit is misconfigured") from None
    if not 1 <= max_bytes <= 64 * 1024 * 1024:
        raise HTTPException(status_code=503, detail="Lisa trace size limit is misconfigured")
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > max_bytes:
                raise HTTPException(status_code=413, detail="OTLP session bundle is too large")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid Content-Length") from None
    chunks = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(status_code=413, detail="OTLP session bundle is too large")
        chunks.append(chunk)
    try:
        bundle = json.loads(b"".join(chunks))
        receipt = intake.accept(bundle)
    except SessionTraceConflict as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except SessionTraceQueueUnavailable as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except Exception as error:
        logger.exception("Failed to enqueue pushed session for evaluation")
        raise HTTPException(status_code=503, detail="Lisa could not queue the session evaluation") from error
    return receipt


@router.get("/v1/conversations")
async def user_list_conversations(request: Request) -> list[dict[str, Any]]:
    services = _services(request)
    return [
        await services.conversations.main_conversation(user_workspace(request))
    ]


@router.post("/v1/conversations")
async def user_create_conversation(
    request: Request, body: EmptyRequest | None = None
) -> dict[str, Any]:
    del body
    services = _services(request)
    return await services.conversations.main_conversation(user_workspace(request))


@router.get("/v1/conversations/{conversation_id}")
async def user_get_conversation(conversation_id: str, request: Request) -> dict[str, Any]:
    services = _services(request)
    workspace_id = user_workspace(request)
    main = await services.conversations.main_conversation(workspace_id)
    if str(UUID(conversation_id)) != main["conversation_id"]:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return services.conversations.get_conversation(workspace_id, conversation_id)


@router.delete("/v1/conversations/{conversation_id}")
async def user_delete_conversation(conversation_id: str, request: Request) -> dict[str, bool]:
    main = await _services(request).conversations.main_conversation(user_workspace(request))
    if str(UUID(conversation_id)) != main["conversation_id"]:
        raise HTTPException(status_code=404, detail="Conversation not found")
    raise HTTPException(status_code=409, detail="Lisa's main conversation is persistent")


@router.post("/v1/conversations/{conversation_id}/messages")
async def user_send_message(
    conversation_id: str, request: Request, message: UserSendMessage
) -> dict[str, Any]:
    services = _services(request)
    workspace_id = user_workspace(request)
    main = await services.conversations.main_conversation(workspace_id)
    if str(UUID(conversation_id)) != main["conversation_id"]:
        raise HTTPException(status_code=404, detail="Conversation not found")
    result = await send_message(
        conversation_id,
        SendMessage(
            workspace_id=workspace_id, text=message.text, consent=message.consent
        ),
        services=services,
        actor_id=_user_actor_id(request),
    )
    return result


@router.get("/v1/activities")
async def user_list_activities(
    request: Request,
    limit: int = 50,
    activity_type: str | None = None,
    status: str | None = None,
    query: str | None = None,
    open_only: bool = False,
    due_only: bool = False,
) -> list[dict[str, Any]]:
    user_workspace(request)
    store = _services(request).activity_store
    if store is None:
        raise HTTPException(status_code=503, detail="Lisa activity storage is not configured")
    try:
        return store.list_activity_feed(
            limit=limit, activity_type=activity_type, status=status, query=query,
            open_only=open_only, due_only=due_only,
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.get("/v1/activities/{activity_id}")
async def user_get_activity(activity_id: str, request: Request) -> dict[str, Any]:
    user_workspace(request)
    store = _services(request).activity_store
    if store is None:
        raise HTTPException(status_code=503, detail="Lisa activity storage is not configured")
    try:
        activity_id = str(UUID(activity_id))
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Invalid activity ID") from error
    result = store.get_activity_thread(activity_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Activity not found")
    return result


@router.get("/v1/policies")
async def user_list_policies(
    request: Request, limit: int = 100, offset: int = 0
) -> dict[str, Any]:
    """Return a bounded page from the read-only policy projection."""
    user_workspace(request)
    try:
        records, next_offset = _services(request).policy_service.list_policy_records_page(
            limit=limit, offset=offset
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    return {
        "items": [
            {
                "id": row.id,
                "scope": row.scope,
                "target_agent_name": row.target_agent_name,
                "name": row.name,
                "criteria": row.criteria,
                "approved": row.approved,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
            for row in records
        ],
        "next_offset": next_offset,
    }


@router.get("/v1/policy-suggestions")
async def user_list_policy_suggestions(
    request: Request, status: str | None = "pending", limit: int = 50
) -> dict[str, Any]:
    user_workspace(request)
    service = _services(request).policy_discovery
    if service is None:
        raise HTTPException(status_code=503, detail="Policy discovery is not configured")
    actor_id = _user_actor_id(request)
    try:
        service.policy_service.require_owner_actor(actor_id)
        return {
            "items": service.list_suggestions(status=status, limit=limit),
            "can_review": actor_id in _services(request).context.owner_user_ids,
        }
    except ValueError as error:
        if "authorized Lisa owner" in str(error):
            raise HTTPException(status_code=403, detail=str(error)) from error
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.get("/v1/policy-suggestions/{suggestion_id}")
async def user_get_policy_suggestion(suggestion_id: str, request: Request) -> dict[str, Any]:
    user_workspace(request)
    service = _services(request).policy_discovery
    if service is None:
        raise HTTPException(status_code=503, detail="Policy discovery is not configured")
    try:
        service.policy_service.require_owner_actor(_user_actor_id(request))
        return service.get_suggestion(suggestion_id)
    except ValueError as error:
        if "authorized Lisa owner" in str(error):
            raise HTTPException(status_code=403, detail=str(error)) from error
        status = 404 if "not found" in str(error).lower() else 400
        raise HTTPException(status_code=status, detail=str(error)) from error


@router.post("/v1/policy-suggestions/{suggestion_id}/approve")
async def user_approve_policy_suggestion(
    suggestion_id: str, request: Request, body: ApprovePolicySuggestion
) -> dict[str, Any]:
    user_workspace(request)
    services = _services(request)
    service = services.policy_discovery
    if service is None:
        raise HTTPException(status_code=503, detail="Policy discovery is not configured")
    actor_id = _user_actor_id(request)
    if actor_id not in services.context.owner_user_ids:
        raise HTTPException(status_code=403, detail="This action requires an authorized Lisa owner")
    try:
        return service.approve(
            suggestion_id=suggestion_id, actor_id=actor_id,
            name=body.name, criteria=body.criteria,
        )
    except ValueError as error:
        message = str(error)
        status = 404 if "not found" in message.lower() else 409 if "already exists" in message.lower() or "changed" in message.lower() else 400
        raise HTTPException(status_code=status, detail=message) from error


@router.post("/v1/policy-suggestions/{suggestion_id}/dismiss")
async def user_dismiss_policy_suggestion(
    suggestion_id: str, request: Request, body: DismissPolicySuggestion
) -> dict[str, Any]:
    del body
    user_workspace(request)
    services = _services(request)
    service = services.policy_discovery
    if service is None:
        raise HTTPException(status_code=503, detail="Policy discovery is not configured")
    actor_id = _user_actor_id(request)
    if actor_id not in services.context.owner_user_ids:
        raise HTTPException(status_code=403, detail="This action requires an authorized Lisa owner")
    try:
        return service.dismiss(suggestion_id=suggestion_id, actor_id=actor_id)
    except ValueError as error:
        message = str(error)
        status = 404 if "not found" in message.lower() else 409 if "only a pending" in message.lower() else 400
        raise HTTPException(status_code=status, detail=message) from error


def _slack_signature_valid(secret: str, timestamp: str, signature: str, body: bytes) -> bool:
    try:
        timestamp_number = int(timestamp)
    except ValueError:
        return False
    if abs(datetime.now(UTC).timestamp() - timestamp_number) > 300:
        return False
    expected = (
        "v0="
        + hmac.new(
            secret.encode("utf-8"), b"v0:" + timestamp.encode() + b":" + body, sha256
        ).hexdigest()
    )
    return hmac.compare_digest(expected, signature)


def _slack_user_allowed(user_id: str, channel_id: str) -> bool:
    users = {
        item.strip()
        for item in os.getenv("LISA_SLACK_ALLOWED_USERS", "").split(",")
        if item.strip()
    }
    channels = {
        item.strip()
        for item in os.getenv("LISA_SLACK_ALLOWED_CHANNELS", "").split(",")
        if item.strip()
    }
    return user_id in users or channel_id in channels


@router.post("/slack/events")
async def slack_events(
    request: Request, services: WorkspaceServices = Depends(_services)
) -> dict[str, Any]:
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
    try:
        services.slack_event_service.queue(
            {
                "status": "pending",
                "workspace_id": workspace_id,
                "event_id": event_id,
                "channel_id": channel_id,
                "thread_ts": thread_ts,
                "actor_id": user_id,
                "text": redact_text(text),
                "created_at": datetime.now(UTC).isoformat(),
            }
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Invalid Slack event path") from error
    return {"ok": True}


@router.post("/internal/v1/conversations/search", dependencies=[Depends(require_service_token)])
async def list_conversations(
    request: WorkspaceRequest, services: WorkspaceServices = Depends(_services)
) -> list[dict[str, Any]]:
    records = services.conversations.list_conversations(request.workspace_id)
    return [conversation_summary(item) for item in records]


@router.post("/internal/v1/conversations", dependencies=[Depends(require_service_token)])
async def create_conversation(
    request: CreateConversation, services: WorkspaceServices = Depends(_services)
) -> dict[str, Any]:
    try:
        return services.conversations.create_conversation(
            request.workspace_id, request.conversation_id
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Invalid conversation scope") from error


@router.get(
    "/internal/v1/conversations/{conversation_id}", dependencies=[Depends(require_service_token)]
)
async def get_conversation(
    conversation_id: str,
    workspace_id: str,
    services: WorkspaceServices = Depends(_services),
) -> dict[str, Any]:
    try:
        return services.conversations.get_conversation(workspace_id, conversation_id)
    except ConversationNotFound as error:
        raise HTTPException(status_code=404, detail="Conversation not found") from error
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Invalid conversation scope") from error


@router.delete(
    "/internal/v1/conversations/{conversation_id}", dependencies=[Depends(require_service_token)]
)
async def delete_conversation(
    conversation_id: str,
    workspace_id: str,
    services: WorkspaceServices = Depends(_services),
) -> dict[str, bool]:
    try:
        await services.conversations.delete_conversation(workspace_id, conversation_id)
    except ConversationNotFound as error:
        raise HTTPException(status_code=404, detail="Conversation not found") from error
    except ValueError as error:
        raise HTTPException(status_code=400, detail="Invalid conversation scope") from error
    return {"deleted": True}


@router.post(
    "/internal/v1/conversations/{conversation_id}/messages",
    dependencies=[Depends(require_service_token)],
)
async def send_message(
    conversation_id: str,
    request: SendMessage,
    services: WorkspaceServices = Depends(_services),
    actor_id: str = "",
) -> dict[str, Any]:
    if not request.consent:
        raise HTTPException(status_code=403, detail="Provider data consent is required")
    text = redact_text(request.text.strip())
    if not text:
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    try:
        return await services.conversations.run_user_turn(
            conversation_id,
            request.workspace_id,
            text,
            run_turn=run_turn,
            actor_id=actor_id,
        )
    except ConversationTurnError as error:
        raise HTTPException(status_code=error.status_code, detail=str(error)) from error
    except ConversationNotFound as error:
        raise HTTPException(status_code=404, detail="Conversation not found") from error
