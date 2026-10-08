from __future__ import annotations

import json
import os
import re

from deepeval.models import GeminiModel
from deepeval.metrics import ConversationalGEval
from deepeval.test_case import (
    ConversationalTestCase,
    MultiTurnParams,
    ToolCall,
    Turn,
)

from .contracts import (
    EvaluationRequest,
    EvaluationResponse,
    EvidenceReference,
)

_SENSITIVE_KEY = re.compile(
    r"(?:authorization|cookie|password|passwd|secret|api[_-]?key|access[_-]?token)", re.I
)
_BEARER = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")
_KEY_LIKE = re.compile(
    r"\b(?:sk[-_][A-Za-z0-9_-]{16,}|AIza[A-Za-z0-9_-]{30,}|"
    r"ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"
)


def create_judge_model() -> GeminiModel:
    """Build the configured Gemini judge used for online G-Eval."""
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY or GEMINI_API_KEY is required for Gemini evaluation")
    return GeminiModel(
        model=os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash"),
        api_key=api_key,
        temperature=0,
    )


def _redact(value: object, key: str = "") -> object:
    if _SENSITIVE_KEY.search(key):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(k): _redact(v, str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, str):
        if value.lstrip().startswith(("{", "[")):
            try:
                return json.dumps(_redact(json.loads(value)), ensure_ascii=False)
            except (json.JSONDecodeError, TypeError):
                pass
        return _KEY_LIKE.sub("[REDACTED]", _BEARER.sub("Bearer [REDACTED]", value))
    return value


def _ordered_tool_failure(request: EvaluationRequest) -> tuple[str, str, list[str]] | None:
    events = sorted(request.evidence.events, key=lambda item: item.sequence)
    tool_positions = [
        (event.name, index, event)
        for index, event in enumerate(events)
        if event.kind == "tool_call" and event.name
    ]
    for requirement in request.evaluator.required_tool_order:
        before_positions = [event for name, _, event in tool_positions if name == requirement.before]
        action_positions = [event for name, _, event in tool_positions if name == requirement.action]
        if not action_positions:
            # The prerequisite is only required when the consequential action occurs.
            continue
        if not before_positions:
            return (
                "fail",
                f"Required tool order not observed: {requirement.before} before {requirement.action}.",
                [event.span_id for event in action_positions],
            )

        first_action = min(action_positions, key=lambda event: event.sequence)
        eligible_before = [
            event for event in before_positions if event.sequence < first_action.sequence
        ]
        if not eligible_before:
            return (
                "fail",
                f"Observed {requirement.action} before {requirement.before}.",
                [first_action.span_id, *(event.span_id for event in before_positions)],
            )

        if requirement.require_result_before_action:
            completed_before = [
                event
                for event in eligible_before
                if any(
                    result.kind == "tool_result"
                    and result.sequence < first_action.sequence
                    and (
                        (event.call_id and result.call_id == event.call_id)
                        or (not event.call_id and result.span_id == event.span_id)
                    )
                    for result in events
                )
            ]
            if not completed_before:
                return (
                    "insufficient_evidence",
                    f"The trace has no correlated result for {requirement.before}; tool order cannot be verified safely.",
                    [first_action.span_id, *(event.span_id for event in eligible_before)],
                )
    return None


def _turns(request: EvaluationRequest) -> list[Turn]:
    turns: list[Turn] = []
    events = sorted(request.evidence.events, key=lambda item: item.sequence)
    result_by_call = {
        (event.call_id or event.span_id): event.payload if event.payload is not None else event.content
        for event in events
        if event.kind == "tool_result"
    }
    for event in events:
        if event.kind == "tool_call":
            call_key = event.call_id or event.span_id
            turns.append(
                Turn(
                    role="assistant",
                    content="",
                    tools_called=[
                        ToolCall(
                            name=event.name or "unknown_tool",
                            input_parameters=(
                                _redact(event.payload)
                                if isinstance(event.payload, dict)
                                else None
                            ),
                            output=_redact(result_by_call.get(call_key)),
                        )
                    ],
                )
            )
            continue
        if event.kind == "tool_result":
            continue
        role = "assistant" if event.kind == "assistant_message" else "user"
        content = str(_redact(event.content or ""))
        if event.kind == "context_message":
            content = f"[System or developer instructions]\n{content}"
        turns.append(Turn(role=role, content=content))
    return turns


def evaluate(request: EvaluationRequest) -> EvaluationResponse:
    evidence = request.evidence
    refs = [
        EvidenceReference(span_id=event.span_id, kind=event.kind, name=event.name)
        for event in sorted(evidence.events, key=lambda item: item.sequence)
    ]

    if len({event.sequence for event in evidence.events}) != len(evidence.events):
        return EvaluationResponse(
            run_id=request.run_id,
            evaluator_id=request.evaluator.evaluator_id,
            evaluator_version=request.evaluator.version,
            trace_id=evidence.trace_id,
            status="insufficient_evidence",
            threshold=request.evaluator.threshold,
            rationale="Trace events do not have a unique order, so the evaluator cannot judge them safely.",
            evidence=refs,
            limitations=evidence.limitations + ["event_order_ambiguous"],
        )

    if not evidence.complete:
        return EvaluationResponse(
            run_id=request.run_id,
            evaluator_id=request.evaluator.evaluator_id,
            evaluator_version=request.evaluator.version,
            trace_id=evidence.trace_id,
            status="insufficient_evidence",
            threshold=request.evaluator.threshold,
            rationale="The trace is not complete, so this evaluation cannot reach a verdict.",
            evidence=refs,
            limitations=evidence.limitations or ["trace_incomplete"],
        )

    if request.evaluator.required_tool_order and not evidence.tool_events_captured:
        return EvaluationResponse(
            run_id=request.run_id,
            evaluator_id=request.evaluator.evaluator_id,
            evaluator_version=request.evaluator.version,
            trace_id=evidence.trace_id,
            status="insufficient_evidence",
            threshold=request.evaluator.threshold,
            rationale="This evaluator requires tool-call evidence that the trace does not contain.",
            evidence=refs,
            limitations=evidence.limitations + ["tool_events_not_captured"],
        )

    if request.evaluator.required_tool_order and not evidence.tool_order_certain:
        return EvaluationResponse(
            run_id=request.run_id,
            evaluator_id=request.evaluator.evaluator_id,
            evaluator_version=request.evaluator.version,
            trace_id=evidence.trace_id,
            status="insufficient_evidence",
            threshold=request.evaluator.threshold,
            rationale="The trace has tool calls, but their order cannot be reconstructed reliably.",
            evidence=refs,
            limitations=evidence.limitations + ["tool_order_ambiguous"],
        )

    tool_failure = _ordered_tool_failure(request)
    if tool_failure:
        status, rationale, span_ids = tool_failure
        return EvaluationResponse(
            run_id=request.run_id,
            evaluator_id=request.evaluator.evaluator_id,
            evaluator_version=request.evaluator.version,
            trace_id=evidence.trace_id,
            status=status,
            score=0.0 if status == "fail" else None,
            threshold=request.evaluator.threshold,
            rationale=rationale,
            evidence=[ref for ref in refs if ref.span_id in span_ids],
            limitations=evidence.limitations,
        )

    if not evidence.message_order_certain:
        return EvaluationResponse(
            run_id=request.run_id,
            evaluator_id=request.evaluator.evaluator_id,
            evaluator_version=request.evaluator.version,
            trace_id=evidence.trace_id,
            status="insufficient_evidence",
            threshold=request.evaluator.threshold,
            rationale="Conversation messages from separate spans have tied timestamps, so their order is ambiguous.",
            evidence=refs,
            limitations=evidence.limitations + ["message_order_ambiguous"],
        )

    turns = _turns(request)
    if not turns:
        return EvaluationResponse(
            run_id=request.run_id,
            evaluator_id=request.evaluator.evaluator_id,
            evaluator_version=request.evaluator.version,
            trace_id=evidence.trace_id,
            status="insufficient_evidence",
            threshold=request.evaluator.threshold,
            rationale="No user or assistant turns were found in the trace evidence.",
            evidence=refs,
            limitations=evidence.limitations + ["conversation_turns_missing"],
        )

    test_case = ConversationalTestCase(turns=turns)
    evaluation_params = [MultiTurnParams.CONTENT]
    if evidence.tool_events_captured:
        evaluation_params.append(MultiTurnParams.TOOLS_CALLED)
    metric = ConversationalGEval(
        name=request.evaluator.name,
        criteria=str(_redact(request.evaluator.criteria)),
        evaluation_params=evaluation_params,
        model=create_judge_model(),
        threshold=request.evaluator.threshold,
        async_mode=False,
        verbose_mode=False,
    )
    metric.measure(test_case)
    score = metric.score
    reason = metric.reason or "DeepEval did not return a rationale."
    if score is None:
        status = "uncertain"
    elif score >= request.evaluator.threshold + request.evaluator.uncertainty_margin:
        status = "pass"
    elif score <= request.evaluator.threshold - request.evaluator.uncertainty_margin:
        status = "fail"
    else:
        status = "uncertain"
    return EvaluationResponse(
        run_id=request.run_id,
        evaluator_id=request.evaluator.evaluator_id,
        evaluator_version=request.evaluator.version,
        trace_id=evidence.trace_id,
        status=status,
        score=score,
        threshold=request.evaluator.threshold,
        rationale=reason,
        evidence=refs,
        limitations=evidence.limitations + ["sensitive_fields_redacted_before_provider_call"],
    )
