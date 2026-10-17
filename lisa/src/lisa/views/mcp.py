"""Typed views returned from Lisa's session MCP tools."""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


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
