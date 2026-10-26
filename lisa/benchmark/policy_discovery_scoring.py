"""Scorer-only checks for the labeled policy-discovery benchmark."""

from __future__ import annotations

import re
from typing import Any

from lisa.models.policy_discovery import PolicySuggestionDraft


def score_policy_consolidation_case(expected: dict[str, Any], suggestions: list[PolicySuggestionDraft]) -> dict[str, Any]:
    """Compare a model's proposals with scorer-only count and relation labels."""
    actual_overlaps = sorted(
        (overlap.policy_id, overlap.relation)
        for suggestion in suggestions
        for overlap in suggestion.overlaps
    )
    expected_overlaps = sorted(
        (item["policy_id"], item["relation"])
        for item in expected.get("overlaps", [])
    )
    proposal_expectation = expected.get("proposal")
    quality_checks = []
    if proposal_expectation is not None and len(suggestions) == 1:
        proposal = suggestions[0]
        criteria_tokens = set(re.findall(r"[a-z0-9]+", proposal.criteria.casefold()))
        concept_checks = [
            any(variant in criteria_tokens for variant in concept)
            for concept in proposal_expectation["criteria_concepts"]
        ]
        confirmation_check = (
            _affirms_confirmation_before_action(proposal.criteria)
            if proposal_expectation.get("requires_confirmation_before_action")
            else True
        )
        cost_check = (
            _affirms_cost_acceptance_before_commit(proposal.criteria)
            if proposal_expectation.get("requires_cost_acceptance_before_commit")
            else True
        )
        comparison_check = (
            _affirms_comparison_before_commit(proposal.criteria)
            if proposal_expectation.get("requires_compare_before_commit")
            else True
        )
        expected_signal_ids = set(proposal_expectation["signal_ids"])
        quality_checks = [
            proposal.scope == proposal_expectation["scope"],
            proposal.target_agent_name == proposal_expectation["target_agent_name"],
            all(concept_checks),
            confirmation_check,
            cost_check,
            comparison_check,
            expected_signal_ids.issubset(set(proposal.signal_ids)),
        ]
    elif proposal_expectation is not None:
        quality_checks = [False]
    proposal_quality_correct = all(quality_checks) if proposal_expectation is not None else True
    return {
        "expected_suggestion_count": expected["suggestion_count"],
        "suggestion_count": len(suggestions),
        "expected_overlaps": [
            {"policy_id": policy_id, "relation": relation}
            for policy_id, relation in expected_overlaps
        ],
        "actual_overlaps": [
            {"policy_id": policy_id, "relation": relation}
            for policy_id, relation in actual_overlaps
        ],
        "proposal_quality_correct": proposal_quality_correct,
        "correct": (
            len(suggestions) == expected["suggestion_count"]
            and actual_overlaps == expected_overlaps
            and proposal_quality_correct
        ),
    }


def _affirms_confirmation_before_action(criteria: str) -> bool:
    """Recognize affirmative confirmation safeguards and reject common inversions."""
    text = " ".join(criteria.casefold().split())
    negated = (
        r"\b(?:do not|don't|never|avoid|skip|bypass)\s+(?:ask|request|obtain|require|seek)\b.{0,80}\bconfirm\w*\b",
        r"\bconfirm\w*\b.{0,40}\b(?:not required|unnecessary|not needed|optional)\b",
        r"\b(?:no|without)\s+(?:explicit\s+)?(?:user\s+)?confirm\w*\b",
        r"\b(?:not|never)\s+(?:ask|require|request)\b.{0,80}\bconfirm\w*\b",
    )
    if any(re.search(pattern, text) for pattern in negated):
        return False
    affirmative = (
        r"\b(?:ask|request|obtain|receive|get|require|seek)\b.{0,80}\bconfirm\w*\b.{0,100}\b(?:before|prior to)\b.{0,100}\b(?:delete|delet\w*|remove|remov\w*|destruct\w*|irreversib\w*|proceed|execut\w*)\b",
        r"\b(?:before|prior to)\b.{0,100}\b(?:delete|delet\w*|remove|remov\w*|destruct\w*|irreversib\w*|proceed|execut\w*)\b.{0,100}\b(?:ask|request|obtain|receive|get|require|seek)\b.{0,50}\bconfirm\w*\b",
        r"\b(?:do not|don't|never)\s+(?:delete|delet\w*|remove|remov\w*|destruct\w*|proceed|execut\w*)\b.{0,80}\b(?:until|unless)\b.{0,60}\bconfirm\w*\b",
        r"\bwithout\b.{0,40}\bconfirm\w*\b.{0,80}\b(?:do not|don't|never)\s+(?:delete|delet\w*|remove|remov\w*|destruct\w*|proceed|execut\w*)\b",
        r"\b(?:must not|should not)\s+(?:delete|delet\w*|remove|remov\w*|destruct\w*|proceed|execut\w*)\b.{0,100}\bwithout\b.{0,80}\bconfirm\w*\b",
    )
    return any(re.search(pattern, text) for pattern in affirmative)


def _affirms_cost_acceptance_before_commit(criteria: str) -> bool:
    """Require material cost disclosure and customer acceptance before commitment."""
    text = " ".join(criteria.casefold().split())
    negated = (
        r"\b(?:do not|don't|never|avoid|skip|bypass)\s+(?:clearly\s+)?(?:disclos\w*|show\w*|present\w*|quot\w*|obtain|require|seek|request)\b",
        r"\b(?:no need to|not necessary to|unnecessary to|need not)\s+(?:clearly\s+)?(?:disclos\w*|show\w*|present\w*|quot\w*|obtain|require|seek|request|get|ask|accept\w*|approv\w*|confirm\w*)\b",
        r"\b(?:accept\w*|approv\w*|confirm\w*)\b.{0,40}\b(?:not required|unnecessary|not needed|optional)\b",
    )
    if any(re.search(pattern, text) for pattern in negated):
        return False
    if not re.search(r"\b(?:fee|fees|cost|costs|charge|charges|price|refund)\b", text):
        return False
    if not re.search(r"\b(?:accept\w*|approv\w*|confirm\w*)\b", text):
        return False
    before_commit = (
        r"\b(?:before|prior to)\b.{0,100}\b(?:commit\w*|process\w*|execut\w*|finaliz\w*)\b",
        r"\b(?:disclos\w*|show\w*|present\w*|quot\w*)\b.{0,100}\b(?:fee|fees|cost|costs|charge|charges|price|refund)\b.{0,100}\b(?:before|prior to)\b.{0,80}\b(?:commit\w*|process\w*|execut\w*|finaliz\w*)\b",
    )
    if not any(re.search(pattern, text) for pattern in before_commit):
        return False
    disclosure = re.search(r"\b(?:disclos\w*|show\w*|present\w*|quot\w*)\b", text)
    acceptance = re.search(r"\b(?:accept\w*|approv\w*|confirm\w*)\b", text)
    action = re.search(r"\b(?:commit\w*|process\w*|execut\w*|finaliz\w*)\b", text)
    before = re.search(r"\b(?:before|prior to)\b", text)
    if not (
        disclosure and acceptance and action and before
        and disclosure.start() < acceptance.start()
        and before.start() < action.start()
    ):
        return False
    disclose_and_accept_before = disclosure.start() < action.start() and acceptance.start() < action.start()
    disclose_and_accept_after_before = disclosure.start() > before.start() and acceptance.start() > before.start()
    return disclose_and_accept_before or disclose_and_accept_after_before


def _affirms_comparison_before_commit(criteria: str) -> bool:
    """Require comparing options before a booking or other state change."""
    text = " ".join(criteria.casefold().split())
    if re.search(r"\b(?:do not|don't|never|avoid|skip|bypass|no need to|not necessary to|unnecessary to|need not)\s+(?:compare|compar\w*|evaluat\w*|assess\w*)\b", text):
        return False
    comparison = re.search(r"\b(?:compar\w*|evaluat\w*|assess\w*)\b", text)
    alternatives = re.search(r"\b(?:options?|alternatives?|paths?)\b", text)
    action = re.search(r"\b(?:commit\w*|execut\w*|finaliz\w*|submit\w*|purchase\w*)\b", text)
    if not (comparison and alternatives and action):
        return False
    before = re.search(r"\b(?:before|prior to)\b", text)
    return bool(before and comparison.start() < before.start() < action.start() and alternatives.start() < action.start())
