from __future__ import annotations

import json
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

Identifier = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=256)]
BoundedText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=20_000)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EvidenceEvent(StrictModel):
    sequence: int = Field(ge=0)
    span_id: Identifier
    parent_span_id: str | None = Field(default=None, max_length=256)
    timestamp: Annotated[str, StringConstraints(max_length=64)]
    kind: Literal["user_message", "assistant_message", "context_message", "tool_call", "tool_result"]
    name: str | None = Field(default=None, max_length=512)
    call_id: str | None = Field(default=None, max_length=256)
    content: str | None = Field(default=None, max_length=20_000)
    payload: Any | None = None


class TraceEvidence(StrictModel):
    trace_id: Identifier
    events: list[EvidenceEvent] = Field(min_length=1, max_length=1_000)
    complete: bool
    tool_events_captured: bool
    tool_order_certain: bool = True
    message_order_certain: bool = True
    limitations: list[str] = Field(default_factory=list, max_length=50)

    @model_validator(mode="after")
    def bound_serialized_evidence(self) -> "TraceEvidence":
        # Bound the full serialized payload as well as individual fields. `payload`
        # may contain nested tool arguments/results that field-length limits miss.
        encoded = json.dumps(self.model_dump(mode="json"), ensure_ascii=False)
        if len(encoded.encode("utf-8")) > 1_000_000:
            raise ValueError("trace evidence exceeds the 1 MB evaluation limit")
        text_chars = sum(
            len(event.content or "")
            + (len(json.dumps(event.payload, ensure_ascii=False)) if event.payload is not None else 0)
            for event in self.events
        )
        if text_chars > 120_000:
            raise ValueError("trace evidence exceeds the 120,000 character content limit")
        return self


class OrderedToolRequirement(StrictModel):
    before: Identifier
    action: Identifier
    require_result_before_action: bool = True


class EvaluatorSpec(StrictModel):
    evaluator_id: Identifier
    version: int = Field(ge=1)
    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
    criteria: BoundedText
    threshold: float = Field(default=0.7, ge=0.0, le=1.0)
    uncertainty_margin: float = Field(default=0.1, ge=0.0, le=0.49)
    required_tool_order: list[OrderedToolRequirement] = Field(default_factory=list, max_length=20)


class EvaluationRequest(StrictModel):
    run_id: Identifier
    evaluator: EvaluatorSpec
    evidence: TraceEvidence


class EvidenceReference(StrictModel):
    span_id: Identifier
    kind: str
    name: str | None = None


class EvaluationResponse(StrictModel):
    run_id: str
    evaluator_id: str
    evaluator_version: int
    trace_id: str
    status: Literal["pass", "fail", "uncertain", "insufficient_evidence", "error"]
    score: float | None = None
    threshold: float
    rationale: str
    evidence: list[EvidenceReference] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    framework: str = "deepeval"
