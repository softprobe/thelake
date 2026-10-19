"""Expose workspace domain services through the Model Context Protocol."""

from contextlib import asynccontextmanager
from typing import Annotated, Any, Literal

from fastmcp import FastMCP
from pydantic import Field

from ..bootstrap import WorkspaceServices
from ..llm_recording import recording_maintenance
from ..security import redact_evidence
from ..views.database import LISA_DATABASE_SCHEMA
from ..views.failures import render_pending_failures
from ..views.mcp import McpSessionSearchResponse, McpSessionTrajectory
from ..views.policies import render_policy_list, render_policy_records, render_policy_saved
from ..views.tools import hide_evaluation_ids


def create_server(services: WorkspaceServices) -> FastMCP:
    @asynccontextmanager
    async def lifespan(_server):
        async with recording_maintenance(services.context):
            yield {}

    server = FastMCP("Lisa workspace tools", lifespan=lifespan)
    for handler in (
        services.failures.complete_evaluation_failure_followup,
        services.evaluations.notify_evaluation_failure,
    ):
        server.tool(handler)

    @server.tool()
    def list_pending_evaluation_failures() -> list[dict] | str:
        """List stored evaluation failures that still need Lisa's follow-up."""
        return render_pending_failures(services.failures.list_pending_evaluation_failures())

    @server.tool()
    def list_policies() -> list[str] | str:
        """List active policies across scopes with IDs and applicability."""
        return render_policy_list(services.policy_service.list_policy_records())

    @server.tool()
    def read_policy(scope: str = "workspace", agent_name: str = "") -> str:
        """Read workspace policy by default, or one agent's rules by exact name."""
        records = services.policy_service.read_policy_records(scope, agent_name)
        return render_policy_records(records, scope)

    @server.tool()
    def update_policy(
        content: str,
        expected_content: str,
        scope: str = "workspace",
        agent_name: str = "",
    ) -> str:
        """Replace a policy scope in one transaction after owner confirmation."""
        count = services.policy_service.update_policy(
            content, expected_content, scope, agent_name
        )
        return render_policy_saved(count)

    @server.tool()
    def evaluate_session(target_agent_name: str, session_id: str) -> dict[str, Any]:
        """Triage approved policies for suspected violations and judge escalated policies."""
        result = services.evaluations.evaluate_session(target_agent_name, session_id)
        return hide_evaluation_ids(redact_evidence(result))

    @server.tool()
    def create_activity(
        title: str, activity_type: str, source: str,
        subtitle: str | None = None, task_label: str | None = None,
        is_goal: bool = False, agent_id: str | None = None,
        message_id: str | None = None,
        details: dict[str, Any] | None = None, parent_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a Lisa activity and its feed card and monitor thread."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        activity = services.activity_store.create_activity(
            title=title, kind=activity_type, activity_type=activity_type,
            source=source, subtitle=subtitle, task_label=task_label,
            is_goal=is_goal, agent_id=agent_id, message_id=message_id,
            details=details,
            parent_id=parent_id,
        )
        return {"activity_id": activity.id, "context_path": activity.context_path}

    @server.tool()
    def update_activity(
        activity_id: str, title: str | None = None, subtitle: str | None = None,
        status_title: str | None = None, task_label: str | None = None,
        details: dict[str, Any] | None = None,
        event_type: str | None = None, event_title: str | None = None,
        event_summary: str | None = None,
    ) -> dict[str, Any]:
        """Update an activity card and optionally append a visible timeline action."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        services.activity_store.update_activity_presentation(
            activity_id, title=title, subtitle=subtitle, status_title=status_title,
            task_label=task_label, details=details, event_type=event_type,
            event_title=event_title, event_summary=event_summary,
        )
        return {"activity_id": activity_id, "updated": True}

    @server.tool()
    def complete_activity(
        activity_id: str,
        status: Literal["success", "error", "stopped"],
        summary: str,
    ) -> dict[str, Any]:
        """Finish a queued Lisa activity with its outcome and final summary."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        terminal_status = {"success": "succeeded", "error": "failed", "stopped": "cancelled"}[status]
        finished = services.activity_store.complete_agent_activity(
            activity_id, summary, status=terminal_status,
        )
        if not finished:
            raise ValueError("Activity was not found or is no longer queued")
        return {"activity_id": activity_id, "status": status, "completed": True}

    @server.tool()
    def list_recent_activities(
        limit: int = 50, is_goal: bool | None = None,
        activity_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """List recent feed cards, optionally filtered by goal or activity type."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        return services.activity_store.list_activity_feed(
            limit=limit, is_goal=is_goal, activity_type=activity_type,
        )

    @server.tool()
    def get_activity_thread(activity_id: str) -> dict[str, Any] | None:
        """Read one activity monitor thread, including its sections and actions."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        return services.activity_store.get_activity_thread(activity_id)

    @server.tool()
    def set_activity_section(
        activity_id: str, section_key: str, title: str = "",
        status: str = "pending", section_order: int = 0,
    ) -> dict[str, Any]:
        """Create or update the activity's monitor section."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        services.activity_store.set_activity_section(
            activity_id, section_key, title=title, status=status,
            section_order=section_order,
        )
        return {"activity_id": activity_id, "section_key": section_key}

    @server.tool()
    def link_activity_reference(
        activity_id: str,
        reference_type: Literal["agent", "message", "runtime_work", "carrier_message"],
        external_id: str,
    ) -> dict[str, Any]:
        """Link an agent, message, runtime work item, or carrier message to a thread."""
        if services.activity_store is None:
            raise RuntimeError("Lisa activity storage is not configured")
        services.activity_store.link_activity_reference(
            activity_id, reference_type, external_id,
        )
        return {"activity_id": activity_id, "reference_type": reference_type,
                "external_id": external_id}

    @server.tool()
    def lisa_db(query: Annotated[str, Field(min_length=1, max_length=5000)]) -> dict[str, Any]:
        """Run one bounded, read-only SELECT against Lisa's approved views and policy table."""
        return services.database_inspection.query(query)

    @server.resource("lisa://database/schema")
    def lisa_database_schema() -> str:
        """Describe the safe read-only Lisa database views and example queries."""
        return LISA_DATABASE_SCHEMA

    @server.tool()
    def search_sessions(
        from_time: Annotated[str | None, Field(alias="from")] = None,
        to_time: Annotated[str | None, Field(alias="to")] = None,
        has_errors: bool | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        agent_name: str | None = None,
        roots_only: bool = True,
        order_by: Literal[
            "start_time", "error_count", "duration", "total_tokens", "total_cost"
        ] = "start_time",
        order: Literal["asc", "desc"] = "desc",
        limit: int = 50,
        cursor: str | None = None,
    ) -> McpSessionSearchResponse:
        """Search session summaries using the same filters and page shape as TheLake's session API."""
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

    @server.tool()
    def get_session(session_id: str) -> McpSessionTrajectory:
        """Read one session as a compact user/tool/assistant trajectory."""
        result = services.sessions.get_trajectory(session_id)
        return McpSessionTrajectory.model_validate(
            hide_evaluation_ids(redact_evidence(result))
        ).model_dump(exclude_none=True)

    return server
