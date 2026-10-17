"""Shared labeled STATE-Bench inputs for formatting and live quality checks."""

import json
from pathlib import Path

from lisa.repositories.policies import InMemoryPolicyRepository
from lisa.services.policies import PolicyService

FIXTURES = Path(__file__).parent / "fixtures"


def load_gold_cases():
    return [
        json.loads(line)
        for line in (FIXTURES / "state_bench_gold.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def load_gold_rules():
    return PolicyService(context=None, repository=InMemoryPolicyRepository()).parse_rules(
        (FIXTURES / "state_bench_policies" / "POLICY.md").read_text(encoding="utf-8"),
        "workspace",
    )
