"""Expose workspace domain services through the Model Context Protocol."""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal

from fastmcp import FastMCP
from pydantic import BaseModel, ConfigDict, Field

from .context import WorkspaceContext
from .presentation import hide_evaluation_ids
from .security import redact_evidence
from .services import WorkspaceServices, build_services


class McpSessionSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str
    start_time: str
    end_time: str | None = None
    trace_count: int
    span_count: int
    error_count: int
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    total_cost: float | None = None
    agent_name: str | None = None
    user_ids: list[str] = Field(default_factory=list)
    models: list[str] = Field(default_factory=list)


class McpSessionSearchResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[McpSessionSummary]
    next_cursor: str | None = None
    cursor_supported: bool


class McpTrajectoryEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sequence: int
    span_id: str
    parent_span_id: str | None = None
    timestamp: str
    kind: Literal["user_message", "assistant_message", "tool_call", "tool_result"]
    name: str | None = None
    call_id: str | None = None
    status: str | None = None
    content: str | None = None
    payload: Any | None = None


class McpSessionTrajectory(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str
    agent_names: list[str]
    trace_ids: list[str]
    events: list[McpTrajectoryEvent]
    complete: bool
    tool_events_captured: bool
    tool_order_certain: bool
    message_order_certain: bool
    limitations: list[str]


def create_server(services: WorkspaceServices | None = None) -> FastMCP:
    services = services or build_services(WorkspaceContext.from_environment())
    server = FastMCP("Lisa workspace tools")
    for handler in (
        services.policies.list_policies,
        services.policies.read_policy,
        services.policies.update_policy,
        services.failures.list_pending_evaluation_failures,
        services.failures.complete_evaluation_failure_followup,
        services.evaluations.list_approved_policies,
        services.evaluations.evaluate_session,
        services.evaluations.notify_evaluation_failure,
    ):
        server.tool(handler)

    @server.tool()
    def search_sessions(
        from_time: Annotated[str | None, Field(alias="from")] = None,
        to_time: Annotated[str | None, Field(alias="to")] = None,
        has_errors: bool | None = None,
        user_id: str | None = None,
        model_name: str | None = None,
        agent_name: str | None = None,
        roots_only: bool = True,
        order_by: Literal["start_time", "error_count", "duration", "total_tokens", "total_cost"] = "start_time",
        order: Literal["asc", "desc"] = "desc",
        limit: int = 50,
        cursor: str | None = None,
    ) -> McpSessionSearchResponse:
        """Search session summaries using the same filters and page shape as TheLake's session API."""
        now = datetime.now(UTC)
        result = services.client.post(
            "/v1/sessions/search",
            {
                "from": from_time or (now - timedelta(days=7)).isoformat(),
                "to": to_time or now.isoformat(),
                "has_errors": has_errors,
                "user_id": user_id,
                "model_name": model_name,
                "agent_name": agent_name,
                "roots_only": roots_only,
                "order_by": order_by,
                "order": order,
                "limit": limit,
                "cursor": cursor,
            },
        )
        return McpSessionSearchResponse.model_validate(redact_evidence(result))

    @server.tool()
    def get_session(session_id: str) -> McpSessionTrajectory:
        """Read one session as a compact user/tool/assistant trajectory."""
        result = services.evaluations.get_session_trajectory(session_id)
        return McpSessionTrajectory.model_validate(
            hide_evaluation_ids(redact_evidence(result))
        ).model_dump(exclude_none=True)

    return server


mcp = create_server()

if __name__ == "__main__":
    mcp.run()
