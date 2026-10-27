from benchmark.policy_discovery_scoring import score_policy_consolidation_case
from lisa.models.policy_discovery import PolicySuggestionDraft


def _proposal(overlaps=()):
    return PolicySuggestionDraft.model_validate({
        "name": "Confirm destructive action",
        "criteria": "Ask for confirmation before deleting production data.",
        "scope": "agent",
        "target_agent_name": "cleanup-agent",
        "rationale": "Repeated traces show deletion without confirmation.",
        "signal_ids": ["signal-1", "signal-2", "signal-3"],
        "overlaps": list(overlaps),
    })


def test_benchmark_scores_duplicate_and_conflict_suppression():
    for check in ("duplicate_suppression", "conflict_suppression"):
        expected = {"suggestion_count": 0, "checks": [check]}
        assert score_policy_consolidation_case(expected, [])["correct"]
        assert not score_policy_consolidation_case(expected, [_proposal()])["correct"]


def test_benchmark_requires_compatible_overlap_to_be_reported_as_overlap():
    expected = {
        "suggestion_count": 1,
        "overlaps": [{"policy_id": "P1.1", "relation": "overlap"}],
        "proposal": {
            "scope": "agent",
            "target_agent_name": "cleanup-agent",
            "criteria_concepts": [["confirm", "confirmation"], ["delete", "deleting"]],
            "signal_ids": ["signal-1", "signal-2", "signal-3"],
        },
        "checks": ["compatible_overlap_reporting"],
    }
    compatible = _proposal([{
        "policy_id": "P1.1", "relation": "overlap",
        "rationale": "Both rules protect production data.",
    }])
    conflict = _proposal([{
        "policy_id": "P1.1", "relation": "conflict",
        "rationale": "The rules differ.",
    }])
    assert score_policy_consolidation_case(expected, [compatible])["correct"]
    assert not score_policy_consolidation_case(expected, [conflict])["correct"]


def test_positive_proposal_must_be_relevant_scoped_and_grounded_in_signals():
    expected = {
        "suggestion_count": 1,
        "proposal": {
            "scope": "agent",
            "target_agent_name": "cleanup-agent",
            "criteria_concepts": [["confirm", "confirmation"], ["delete", "deleting"]],
            "signal_ids": ["signal-1", "signal-2", "signal-3"],
        },
    }
    unrelated = PolicySuggestionDraft.model_validate({
        **_proposal().model_dump(),
        "criteria": "Check whether the itinerary is available before booking.",
        "signal_ids": ["other-signal-1", "other-signal-2", "other-signal-3"],
    })
    assert not score_policy_consolidation_case(expected, [unrelated])["correct"]


def test_benchmark_rejects_negated_confirmation_rule_and_accepts_safe_condition():
    expected = {
        "suggestion_count": 1,
        "proposal": {
            "scope": "agent",
            "target_agent_name": "cleanup-agent",
            "criteria_concepts": [["delete", "deleting", "deletion"]],
            "requires_confirmation_before_action": True,
            "signal_ids": ["signal-1", "signal-2", "signal-3"],
        },
    }
    negated = _proposal().model_copy(update={
        "criteria": "Do not ask for confirmation before deleting production data."
    })
    safe_condition = _proposal().model_copy(update={
        "criteria": "Do not delete production data until the user confirms."
    })
    must_not_without = _proposal().model_copy(update={
        "criteria": "The agent must not execute permanent deletion without first requesting and receiving explicit user confirmation."
    })
    assert not score_policy_consolidation_case(expected, [negated])["correct"]
    assert score_policy_consolidation_case(expected, [safe_condition])["correct"]
    assert score_policy_consolidation_case(expected, [must_not_without])["correct"]


def test_benchmark_requires_fee_disclosure_and_acceptance_before_commit():
    expected = {
        "suggestion_count": 1,
        "proposal": {
            "scope": "workspace",
            "target_agent_name": "",
            "criteria_concepts": [["fee", "charge"], ["disclose", "disclosure", "show", "present"], ["accept", "acceptance", "confirm"]],
            "signal_ids": ["cost-1", "cost-2", "cost-3"],
            "requires_cost_acceptance_before_commit": True,
        },
    }
    good = PolicySuggestionDraft.model_validate({
        **_proposal().model_dump(),
        "scope": "workspace",
        "target_agent_name": "",
        "criteria": "Disclose any cancellation fee and get the customer's acceptance before committing the cancellation.",
        "signal_ids": ["cost-1", "cost-2", "cost-3"],
    })
    unsafe = good.model_copy(update={
        "criteria": "Commit the cancellation, then disclose the fee and ask whether the customer accepts it."
    })
    negated = good.model_copy(update={
        "criteria": "Do not disclose the fee or require customer acceptance before processing a cancellation."
    })
    no_need = good.model_copy(update={
        "criteria": "No need to disclose the cancellation fee or get customer acceptance before processing."
    })
    no_acceptance_needed = good.model_copy(update={
        "criteria": "Disclose the fee, but there is no need to ask for customer acceptance before processing."
    })
    need_not_approve = good.model_copy(update={
        "criteria": "Customers need not approve the fee before processing a cancellation."
    })
    wrong_order = good.model_copy(update={
        "criteria": "Obtain the customer's acceptance before disclosing the fee and committing a cancellation."
    })
    alternate_safe = good.model_copy(update={
        "criteria": "Before committing a cancellation, disclose any fee and obtain the customer's acceptance."
    })
    assert score_policy_consolidation_case(expected, [good])["correct"]
    assert score_policy_consolidation_case(expected, [alternate_safe])["correct"]
    assert not score_policy_consolidation_case(expected, [unsafe])["correct"]
    assert not score_policy_consolidation_case(expected, [negated])["correct"]
    assert not score_policy_consolidation_case(expected, [no_need])["correct"]
    assert not score_policy_consolidation_case(expected, [no_acceptance_needed])["correct"]
    assert not score_policy_consolidation_case(expected, [need_not_approve])["correct"]
    assert not score_policy_consolidation_case(expected, [wrong_order])["correct"]


def test_benchmark_requires_comparison_before_committing_booking_change():
    expected = {
        "suggestion_count": 1,
        "proposal": {
            "scope": "workspace",
            "target_agent_name": "",
            "criteria_concepts": [["compare", "evaluate"], ["options", "alternatives"], ["booking", "book"]],
            "signal_ids": ["travel-1", "travel-2", "travel-3"],
            "requires_compare_before_commit": True,
        },
    }
    good = PolicySuggestionDraft.model_validate({
        **_proposal().model_dump(),
        "scope": "workspace",
        "target_agent_name": "",
        "criteria": "Compare available booking options and their costs before committing a booking change.",
        "signal_ids": ["travel-1", "travel-2", "travel-3"],
    })
    unsafe = good.model_copy(update={
        "criteria": "Commit the booking change before comparing available options and their costs."
    })
    negated = good.model_copy(update={
        "criteria": "Do not compare booking options before committing a change."
    })
    no_need = good.model_copy(update={
        "criteria": "No need to compare booking options before committing a change."
    })
    assert score_policy_consolidation_case(expected, [good])["correct"]
    assert not score_policy_consolidation_case(expected, [unsafe])["correct"]
    assert not score_policy_consolidation_case(expected, [negated])["correct"]
    assert not score_policy_consolidation_case(expected, [no_need])["correct"]
