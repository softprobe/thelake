"""Opt-in paid test of Decisions routing followed by the Gemini policy judge."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from lisa.evaluation.contracts import EvaluationRequest
from lisa.evaluation.judge import run_evaluation
from lisa.evaluation.policy_selection import OpenAIPolicySelector
from lisa.repositories.policies import InMemoryPolicyRepository
from lisa.services.policies import PolicyService

FIXTURES = Path(__file__).parent / "fixtures"
GOLD_FILE = FIXTURES / "state_bench_gold.jsonl"
POLICY_FILE = FIXTURES / "state_bench_policies" / "POLICY.md"
LIVE_ENABLED = os.getenv("LISA_RUN_STATE_BENCH_LIVE") == "1"
HAS_OPENAI_KEY = bool(os.getenv("OPENAI_API_KEY"))
HAS_GEMINI_KEY = bool(os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY"))


@pytest.mark.skipif(
    not (LIVE_ENABLED and HAS_OPENAI_KEY and HAS_GEMINI_KEY),
    reason=(
        "set LISA_RUN_STATE_BENCH_LIVE=1 plus OPENAI_API_KEY and GEMINI_API_KEY "
        "to run the paid two-stage model evaluation"
    ),
)
def test_state_bench_gold_policy_selection_then_evaluation():
    rules = PolicyService(context=None, repository=InMemoryPolicyRepository()).parse_rules(
        POLICY_FILE.read_text(encoding="utf-8"), "workspace"
    )
    cases = [
        json.loads(line)
        for line in GOLD_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    selector = OpenAIPolicySelector()

    for case in cases:
        expected_name = case["expected"]["selected_policy"]
        selection = selector.select(case["input"]["trajectory"], list(rules.values()))
        selected_names = [item["name"] for item in selection["selected_policies"]]
        assert selected_names == [expected_name], case["name"]

        rule = rules[expected_name]
        request = EvaluationRequest.model_validate(
            {
                "run_id": case["input"]["run_id"],
                "policy": {
                    "name": rule.name,
                    "scope": rule.scope,
                    "approved": True,
                    "criteria": rule.criteria,
                },
                "trajectory": case["input"]["trajectory"],
            }
        )
        verdict = run_evaluation(request.model_dump(mode="json"))
        assert verdict["status"] == case["expected"]["verdict"], case["name"]
