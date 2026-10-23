"""Expose workspace domain services through the Model Context Protocol."""

from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal

from fastmcp import FastMCP
from fastmcp.server.context import Context
from fastmcp.server.transforms.visibility import Visibility
from pydantic import BaseModel, Field

from ..bootstrap import WorkspaceServices
from ..llm_recording import recording_maintenance
from ..security import redact_evidence
from ..views.database import LISA_DATABASE_SCHEMA
from ..views.failures import render_pending_failures
from ..views.mcp import McpSessionSearchResponse, McpSessionTrajectory
from ..views.policies import render_policy_list, render_policy_records
from ..views.tools import hide_evaluation_ids

NAMESPACE_TAGS = {
    "activities": "lisa:activities",
    "policies": "lisa:policies",
    "sessions": "lisa:sessions",
    "evaluation": "lisa:evaluation",
    "database": "lisa:database",
    "conversation": "lisa:conversation",
}
TOOL_NAMESPACES = {
    "activities": {
        "description": "Find, continue, update, and inspect Lisa activity feed work.",
        "keywords": "activity activities work item task feed thread continue update progress",
    },
    "policies": {
        "description": "Find, create, update, approve, and delete workspace policies.",
        "keywords": "policy policies rule criteria approval approve create update delete",
    },
    "sessions": {
        "description": "Search Lisa's locally stored agent sessions and inspect a trajectory.",
        "keywords": "session sessions traces trajectory telemetry agent behavior",
    },
    "evaluation": {
        "description": "Run policy evaluation or inspect evaluation failures and verdicts.",
        "keywords": "evaluation evaluate judge verdict failure triage qa quality",
    },
    "database": {
        "description": "Run targeted read-only diagnostics against approved Lisa database views.",
        "keywords": "database sql query diagnostics schema rows history",
    },
    "conversation": {
        "description": "Read or replace Lisa's persistent checklist for this main chat.",
        "keywords": "checklist todo todos plan next actions remember list",
    },
}


class TodoItem(BaseModel):
    id: str
    text: str
    status: Literal["open", "completed", "cancelled"] = "open"


def create_server(services: WorkspaceServices) -> FastMCP:
    def require_owner() -> None:
        if (
            not services.context.actor_id
            or services.context.actor_id not in services.context.owner_user_ids
        ):
            raise ValueError("This action requires an authorized Lisa owner identity")

    @asynccontextmanager
    async def lifespan(_server):
        async with recording_maintenance(services.context):
            yield {}

    server = FastMCP("Lisa workspace tools", lifespan=lifespan)
    conversation_context = None
    if services.context.conversation_id:
        directory = services.conversation_repository.conversation_directory(
            services.context.workspace_id(), services.context.conversation_id
        )
        conversation_context = services.conversation_repository.context_repository(directory)
    loaded_namespaces = (
        conversation_context.loaded_namespaces() if conversation_context is not None else set()
    )

    @server.tool()
    def discover_tool_namespaces(query: str = "") -> list[dict[str, str]]:
        """Discover Lisa tool groups by searching their purpose and supported work."""
        terms = {part.lower() for part in query.split() if len(part) > 1}
        ranked = []
        for name, details in TOOL_NAMESPACES.items():
            searchable = f"{name} {details['description']} {details['keywords']}".lower()
            score = sum(term in searchable for term in terms)
            if not terms or score:
                ranked.append((score, name, details["description"]))
        ranked.sort(key=lambda item: (-item[0], item[1]))
        return [{"name": name, "description": description} for _, name, description in ranked]

    @server.tool()
    async def load_tool_namespace(namespace: str, ctx: Context) -> dict[str, Any]:
        """Load a tool group's full MCP schemas into the current model context."""
        tag = NAMESPACE_TAGS.get(namespace)
        if tag is None:
            raise ValueError(f"Unknown Lisa tool namespace: {namespace}")
        # Namespaces expose tools and, for database access, schema resources.
        await ctx.enable_components(tags={tag}, components={"tool", "resource"})
        if conversation_context is not None:
            loaded = conversation_context.loaded_namespaces()
            loaded.add(namespace)
            conversation_context.set_loaded_namespaces(loaded)
        names = sorted(_NAMESPACE_TOOL_NAMES[namespace])
        return {"namespace": namespace, "loaded": True, "tools": names}

    for handler in (
        services.failures.complete_evaluation_failure_followup,
        services.evaluations.notify_evaluation_failure,
    ):
        server.tool(handler, tags={NAMESPACE_TAGS["evaluation"]})

    @server.tool(tags={NAMESPACE_TAGS["evaluation"]})
    def list_pending_evaluation_failures() -> list[dict] | str:
        """List stored evaluation failures that still need Lisa's follow-up."""
        return render_pending_failures(services.failures.list_pending_evaluation_failures())

    @server.tool(tags={NAMESPACE_TAGS["policies"]})
    def list_policies() -> list[str] | str:
        """List active policies across scopes with IDs and applicability."""
        return render_policy_list(services.policy_service.list_policy_records())

    @server.tool(tags={NAMESPACE_TAGS["policies"]})
    def read_policy(scope: str = "workspace", agent_name: str = "") -> str:
        """Read workspace policy by default, or one agent's rules by exact name."""
        records = services.policy_service.read_policy_records(scope, agent_name)
        return render_policy_records(records, scope)

    @server.tool(tags={NAMESPACE_TAGS["evaluation"]})
    def evaluate_session(target_agent_name: str, session_id: str) -> dict[str, Any]:
        """Triage approved policies for suspected violations and judge escalated policies."""
        activity_id = None
        if services.context.turn_source in {"web", "slack"}:
            if services.activity_store is None or not services.context.chat_turn_id:
                raise RuntimeError("Chat evaluation requires an active resolved activity")
            activity_id = services.activity_store.active_chat_activity_for_turn(
                services.context.chat_turn_id
            )
            if activity_id is None:
                raise ValueError("Resolve the chat request before evaluating a session")
        result = services.evaluations.evaluate_session(
            target_agent_name,
            session_id,
            activity_id=activity_id,
        )
        return hide_evaluation_ids(redact_evidence(result))

    @server.tool(tags={NAMESPACE_TAGS["activities"]})
    def resolve_chat_activity(
        disposition: Literal["create", "continue"],
        request_key: str,
        title: str | None = None,
        summary: str | None = None,
        activity_id: str | None = None,
    ) -> dict[str, Any]:
        """Required before any task-specific tool work: create or continue one request, bind the current turn, and scope its later tool events. For continuations, call find_activity then get_activity_thread first, then resolve with disposition="continue" and the matching activity_id. Without resolution, the runtime creates a separate fallback activity for this turn."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        if services.context.turn_source not in {"web", "slack"}:
            raise ValueError("Chat activities can only be resolved for web or Slack turns")
        require_owner()
        if not services.context.chat_turn_id:
            raise RuntimeError("Chat turn identity is not configured")
        activity = services.activity_store.resolve_chat_activity(
            turn_id=services.context.chat_turn_id,
            request_key=request_key,
            disposition=disposition,
            source=services.context.turn_source,
            title=title or "",
            summary=summary or "",
            activity_id=activity_id,
            origin_conversation_id=services.context.conversation_id or None,
            origin_slack_channel_id=services.context.slack_channel or None,
            origin_slack_thread_ts=services.context.slack_thread or None,
        )
        return {
            "disposition": disposition,
            "activity_id": activity.id,
            "created": activity.created,
            "request_key": request_key,
        }

    @server.tool(tags={NAMESPACE_TAGS["activities"]})
    def skip_chat_activity_for_turn(reason: str) -> dict[str, bool]:
        """Explicitly leave greetings and conversational filler out of the feed."""
        if services.context.turn_source not in {"web", "slack"}:
            raise ValueError("Chat activity skip is only valid for web or Slack turns")
        if not reason.strip():
            raise ValueError("A skip reason is required")
        return {"skipped": True}

    @server.tool(tags={NAMESPACE_TAGS["activities"]})
    def update_activity(
        activity_id: str,
        title: str | None = None,
        subtitle: str | None = None,
        status_title: str | None = None,
        task_label: str | None = None,
        details: dict[str, Any] | None = None,
        event_type: str | None = None,
        event_title: str | None = None,
        event_summary: str | None = None,
    ) -> dict[str, Any]:
        """Update a card; pass both event_type and event_title, or neither, for a timeline action."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        services.activity_store.update_activity_presentation(
            activity_id,
            title=title,
            subtitle=subtitle,
            status_title=status_title,
            task_label=task_label,
            details=details,
            event_type=event_type,
            event_title=event_title,
            event_summary=event_summary,
        )
        return {"activity_id": activity_id, "updated": True}

    @server.tool(tags={NAMESPACE_TAGS["activities"]})
    def set_activity_progress(
        activity_id: str,
        status: Literal["queued", "running", "waiting"],
        summary: str,
        next_action: str | None = None,
        next_action_at: str | None = None,
    ) -> dict[str, Any]:
        """Record the current work state, next action, and optional due time."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        services.activity_store.update_activity_progress(
            activity_id,
            status=status,
            summary=summary,
            next_action=next_action,
            next_action_at=next_action_at,
        )
        return {"activity_id": activity_id, "status": status, "updated": True}

    @server.tool(tags={NAMESPACE_TAGS["activities"]})
    def complete_activity(
        activity_id: str,
        status: Literal["success", "error", "stopped"],
        summary: str,
    ) -> dict[str, Any]:
        """Finish agent-owned work, deferring the current chat's activities."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        chat_turn = (
            services.context.turn_source in {"web", "slack"}
            and services.context.chat_turn_id
            and services.context.actor_id in services.context.owner_user_ids
        )
        if chat_turn:
            turn_id = services.context.chat_turn_id
            if activity_id not in services.activity_store.chat_activities_for_turn(turn_id):
                services.activity_store.resolve_chat_activity(
                    turn_id=turn_id,
                    request_key=f"complete_activity:{activity_id}",
                    disposition="continue",
                    source=services.context.turn_source,
                    activity_id=activity_id,
                    origin_conversation_id=services.context.conversation_id or None,
                    origin_slack_channel_id=services.context.slack_channel or None,
                    origin_slack_thread_ts=services.context.slack_thread or None,
                )
            return {
                "activity_id": activity_id,
                "status": "deferred",
                "requested_status": status,
                "completed": False,
                "summary": "The chat runtime will finalize this activity after Lisa's reply.",
            }
        terminal_status = {"success": "succeeded", "error": "failed", "stopped": "cancelled"}[
            status
        ]
        finished = services.activity_store.complete_agent_activity(
            activity_id,
            summary,
            status=terminal_status,
        )
        if not finished:
            raise ValueError("Activity was not found or is no longer queued")
        return {"activity_id": activity_id, "status": status, "completed": True}

    @server.tool(tags={NAMESPACE_TAGS["activities"]})
    def list_recent_activities(
        limit: int = 50,
        is_goal: bool | None = None,
        activity_type: str | None = None,
        status: str | None = None,
        query: str | None = None,
        open_only: bool = False,
        due_only: bool = False,
    ) -> list[dict[str, Any]]:
        """List recent feed cards, optionally filtered by goal or activity type."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        return services.activity_store.list_activity_feed(
            limit=limit,
            is_goal=is_goal,
            activity_type=activity_type,
            status=status,
            query=query,
            open_only=open_only,
            due_only=due_only,
        )

    @server.tool(tags={NAMESPACE_TAGS["activities"]})
    def find_activity(query: str, open_only: bool = True, limit: int = 20) -> list[dict[str, Any]]:
        """For continuations, call find_activity first with a short description, then inspect a matching thread before resolving it."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        return services.activity_store.list_activity_feed(
            limit=limit,
            query=query,
            open_only=open_only,
        )

    @server.tool(tags={NAMESPACE_TAGS["policies"]})
    def write_policy(
        scope: Literal["workspace", "agent"],
        name: str,
        criteria: str,
        agent_name: str = "",
        policy_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a policy or update the existing row selected by its readable ID."""
        record = services.policy_service.write_policy(
            scope=scope,
            agent_name=agent_name,
            name=name,
            criteria=criteria,
            policy_id=policy_id,
        )
        return {"policy_id": record.id, "name": record.name, "approved": record.approved}

    @server.tool(tags={NAMESPACE_TAGS["policies"]})
    def approve_policy(policy_id: str) -> dict[str, Any]:
        """Approve the policy with this ID for an authorized owner.

        The policy ID argument selects the row. Before calling, verify the current actor is
        an owner and the readable ID exists. If the actor is not an owner or the policy is
        unknown, explain why and do not call this tool. If it is already approved, report
        that state without a call.
        """
        record = services.policy_service.approve_policy(policy_id)
        return {"policy_id": record.id, "name": record.name, "approved": record.approved}

    @server.tool(tags={NAMESPACE_TAGS["policies"]})
    def delete_policy(policy_id: str) -> dict[str, Any]:
        """Delete the policy with this exact ID for an authorized workspace owner."""
        services.policy_service.delete_policy(policy_id)
        return {"policy_id": policy_id, "deleted": True}

    @server.tool(tags={NAMESPACE_TAGS["activities"]})
    def get_activity_thread(activity_id: str) -> dict[str, Any] | None:
        """Read one activity thread, including ordered actions and their linked evidence IDs."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        return services.activity_store.get_activity_thread(activity_id)

    @server.tool(tags={NAMESPACE_TAGS["activities"]})
    def set_activity_section(
        activity_id: str,
        section_key: str,
        title: str = "",
        status: str = "pending",
        section_order: int = 0,
    ) -> dict[str, Any]:
        """Create or update the activity's monitor section."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        services.activity_store.set_activity_section(
            activity_id,
            section_key,
            title=title,
            status=status,
            section_order=section_order,
        )
        return {"activity_id": activity_id, "section_key": section_key}

    @server.tool(tags={NAMESPACE_TAGS["activities"]})
    def link_activity_reference(
        activity_id: str,
        reference_type: Literal["agent", "message", "runtime_work", "carrier_message"],
        external_id: str,
    ) -> dict[str, Any]:
        """Link an agent, message, runtime work item, or carrier message to a thread."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        services.activity_store.link_activity_reference(
            activity_id,
            reference_type,
            external_id,
        )
        return {
            "activity_id": activity_id,
            "reference_type": reference_type,
            "external_id": external_id,
        }

    @server.tool(tags={NAMESPACE_TAGS["database"]})
    def lisa_db(query: Annotated[str, Field(min_length=1, max_length=5000)]) -> dict[str, Any]:
        """Run one bounded, read-only SELECT against approved Lisa views and policy columns in the schema resource."""
        return services.database_inspection.query(query)

    @server.resource("lisa://database/schema", tags={NAMESPACE_TAGS["database"]})
    def lisa_database_schema() -> str:
        """Describe the safe read-only Lisa database views and example queries."""
        return LISA_DATABASE_SCHEMA

    @server.tool(tags={NAMESPACE_TAGS["sessions"]})
    def search_sessions(
        from_time: Annotated[str | None, Field(alias="from")] = None,
        to_time: Annotated[str | None, Field(alias="to")] = None,
        has_errors: bool | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        agent_name: str | None = None,
        roots_only: bool = True,
        order_by: Literal["start_time", "error_count", "duration", "total_tokens"] = "start_time",
        order: Literal["asc", "desc"] = "desc",
        limit: int = 50,
        cursor: str | None = None,
    ) -> McpSessionSearchResponse:
        """Search bounded summaries for sessions stored in Lisa's local evidence store."""
        result = services.sessions.search(
            from_time=from_time,
            to_time=to_time,
            has_errors=has_errors,
            user_id=user_id,
            model_name=model_name,
            agent_name=agent_name,
            roots_only=roots_only,
            order_by=order_by,
            order=order,
            limit=limit,
            cursor=cursor,
        )
        return McpSessionSearchResponse.model_validate(redact_evidence(result))

    @server.tool(tags={NAMESPACE_TAGS["sessions"]})
    def get_session(session_id: str) -> McpSessionTrajectory:
        """Read one session as a compact user/tool/assistant trajectory."""
        result = services.sessions.get_trajectory(session_id)
        return McpSessionTrajectory.model_validate(
            hide_evaluation_ids(redact_evidence(result))
        ).model_dump(exclude_none=True)

    @server.tool(tags={NAMESPACE_TAGS["conversation"]})
    def todo_read() -> list[dict[str, Any]]:
        """Read the open items in Lisa's persistent main-chat checklist."""
        return conversation_context.read_todo() if conversation_context is not None else []

    @server.tool(tags={NAMESPACE_TAGS["conversation"]})
    def todo_write(items: list[TodoItem]) -> dict[str, Any]:
        """Replace the main-chat checklist; completed and cancelled items are removed."""
        if conversation_context is None:
            raise RuntimeError("A conversation must be active to save its checklist")
        active = conversation_context.write_todo([item.model_dump() for item in items])
        return {"items": active, "count": len(active)}

    server.add_transform(
        Visibility(
            False,
            tags=set(NAMESPACE_TAGS.values()),
        )
    )
    for namespace in sorted(loaded_namespaces):
        tag = NAMESPACE_TAGS.get(namespace)
        if tag is not None:
            server.add_transform(Visibility(True, tags={tag}))

    return server


_NAMESPACE_TOOL_NAMES = {
    "activities": {
        "resolve_chat_activity",
        "skip_chat_activity_for_turn",
        "update_activity",
        "set_activity_progress",
        "complete_activity",
        "list_recent_activities",
        "find_activity",
        "get_activity_thread",
        "set_activity_section",
        "link_activity_reference",
    },
    "policies": {"list_policies", "read_policy", "write_policy", "approve_policy", "delete_policy"},
    "sessions": {"search_sessions", "get_session"},
    "evaluation": {
        "list_pending_evaluation_failures",
        "complete_evaluation_failure_followup",
        "notify_evaluation_failure",
        "evaluate_session",
    },
    "database": {"lisa_db"},
    "conversation": {"todo_read", "todo_write"},
}
