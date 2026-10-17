"""Validated request shapes shared by Lisa's HTTP controllers."""

from pydantic import BaseModel, ConfigDict, Field


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
