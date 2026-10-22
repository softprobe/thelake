from benchmark.policy_seed import seed_policy_document

from dataclasses import replace

import pytest
from policy_cases import save_rule

from lisa.bootstrap import build_services


def test_non_chat_turn_cannot_change_policy(lisa_test_context):
    context = lisa_test_context(
        turn_source="worker",
        actor_id="owner-1",
        owner_user_ids=frozenset({"owner-1"}),
    )
    services = build_services(replace(context, turn_source="worker"))
    with pytest.raises(ValueError, match="owner conversation"):
        services.policy_service.write_policy(
            scope="workspace", agent_name="", name="Rule", criteria="Rule content"
        )
