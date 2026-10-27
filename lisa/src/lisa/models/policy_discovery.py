"""Structured policy-gap evidence and owner-reviewed policy suggestions."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

MIN_POLICY_SUGGESTION_ERROR_SAMPLES = 3


def has_minimum_error_samples(signals: list[dict]) -> bool:
    """Return whether signals represent enough distinct sampled sessions for a policy."""
    session_ids = {
        str(item["session_id"])
        for item in signals
        if item.get("session_id") is not None
    }
    return len(session_ids) >= MIN_POLICY_SUGGESTION_ERROR_SAMPLES


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


PolicyText = Annotated[str, Field(min_length=1, max_length=20_000)]


class PolicyGapSignal(StrictModel):
    """A concise, cited error signal from one session selected for sampling."""

    detected: bool
    summary: str = Field(default="", max_length=2_000)
    span_ids: list[str] = Field(default_factory=list, max_length=20)

    @model_validator(mode="after")
    def require_evidence_for_signal(self) -> PolicyGapSignal:
        if self.detected and (not self.summary.strip() or not self.span_ids):
            raise ValueError("A detected policy gap must include a rationale and cited spans")
        if not self.detected and (self.summary or self.span_ids):
            raise ValueError("A negative policy-gap signal cannot include a rationale or citations")
        if len(set(self.span_ids)) != len(self.span_ids):
            raise ValueError("Policy-gap span citations must be unique")
        return self


class PolicyOverlap(StrictModel):
    policy_id: str = Field(min_length=1, max_length=64)
    relation: Literal["duplicate", "overlap", "conflict"]
    rationale: str = Field(min_length=1, max_length=1_000)


class PolicySuggestionDraft(StrictModel):
    """A proposed policy synthesized only after three distinct error samples."""

    name: str = Field(min_length=1, max_length=200)
    criteria: PolicyText
    scope: Literal["workspace", "agent"]
    target_agent_name: str = Field(default="", max_length=512)
    rationale: str = Field(min_length=1, max_length=4_000)
    signal_ids: list[str] = Field(min_length=MIN_POLICY_SUGGESTION_ERROR_SAMPLES, max_length=100)
    overlaps: list[PolicyOverlap] = Field(default_factory=list, max_length=20)

    @model_validator(mode="after")
    def validate_scope(self) -> PolicySuggestionDraft:
        if len(set(self.signal_ids)) != len(self.signal_ids):
            raise ValueError("Policy suggestions must cite unique error samples")
        if self.scope == "workspace" and self.target_agent_name:
            raise ValueError("Workspace suggestions cannot target one agent")
        if self.scope == "agent" and not self.target_agent_name.strip():
            raise ValueError("Agent suggestions must name the exact target agent")
        return self


class PolicySuggestion(StrictModel):
    id: str
    name: str
    criteria: str
    scope: Literal["workspace", "agent"]
    target_agent_name: str
    rationale: str
    signal_count: int = Field(ge=MIN_POLICY_SUGGESTION_ERROR_SAMPLES)
    evidence: list[dict]
    overlaps: list[dict] = Field(default_factory=list)
    status: Literal["pending", "approved", "dismissed"]
    policy_id: str | None = None
    created_at: str
    reviewed_at: str | None = None
