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


class ApprovePolicySuggestion(StrictModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    criteria: str | None = Field(default=None, min_length=1, max_length=20_000)


class DismissPolicySuggestion(StrictModel):
    pass


class EmptyRequest(StrictModel):
    pass
