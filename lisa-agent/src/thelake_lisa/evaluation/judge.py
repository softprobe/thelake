"""Bound and validate judge requests; do not expose provider exceptions to chat."""

from __future__ import annotations

import logging

from pydantic import ValidationError

from .contracts import EvaluationRequest
from .evaluator import JudgeUnavailableError, evaluate

logger = logging.getLogger(__name__)


def run_evaluation(body: dict) -> dict:
    try:
        return evaluate(EvaluationRequest.model_validate(body)).model_dump(mode="json")
    except (ValidationError, JudgeUnavailableError):
        reason = "The policy/evidence was invalid or the judge is unavailable."
    except Exception as error:
        logger.error("Lisa's local evaluator failed (%s)", type(error).__name__)
        reason = "The judge provider failed while evaluating this trace."
    # Execution errors are not persisted as verdicts, so a later run can retry.
    return {"status": "error", "rationale": reason, "limitations": ["judge_execution_error"]}
