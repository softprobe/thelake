from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import psycopg
import pytest
from policy_cases import list_policies, read_policy

from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.repositories.activities import (
    DeploymentBindingError,
    PostgresActivityStore,
    StaleLeaseError,
)
from lisa.repositories.policies import PostgresPolicyRepository
from lisa.services.policies import PolicyService

WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"


def _test_sql(name: str) -> str:
    return (Path(__file__).parent / "sql" / "activities" / name).read_text()


@pytest.fixture
def store(lisa_test_db) -> PostgresActivityStore:
    return PostgresActivityStore(lisa_test_db.writer_url, WORKSPACE_ID)


def _create_evaluation(store: PostgresActivityStore, request_key: str | None = None):
    request_key = request_key or str(uuid4())
    policy_id = _test_policy_id(store)
    return store.create_evaluation(
        title="Check refund eligibility",
        source="test",
        target_agent_name="quickstart-refund-agent",
        session_id=f"session-{uuid4()}",
        policy_name="refund eligibility",
        policy_id=policy_id,
        policy_hash="policy-sha256",
        judge_config={"model": "test-judge"},
        request_key=request_key,
    )


def _test_policy_id(store: PostgresActivityStore) -> str:
    repository = PostgresPolicyRepository(store.dsn, WORKSPACE_ID)
    agent_name = "quickstart-refund-agent"
    policies = repository.list_scope("agent", agent_name)
    existing = next((item for item in policies if item.name == "refund eligibility"), None)
    if existing:
        return existing.id
    incoming = [
        {"name": item.name, "criteria": item.criteria, "approved": item.approved}
        for item in policies
    ]
    incoming.append(
        {
            "name": "refund eligibility",
            "criteria": "Refunds require checking eligibility.",
            "approved": True,
        }
    )
    saved = repository.replace_scope(
        "agent",
        agent_name,
        [
            {"name": item.name, "criteria": item.criteria, "approved": item.approved}
            for item in policies
        ],
        incoming,
    )
    return next(item.id for item in saved if item.name == "refund eligibility")


def test_provision_binds_one_database_to_its_workspace(store: PostgresActivityStore):
    with pytest.raises(DeploymentBindingError):
        PostgresActivityStore(store.dsn, "1480b512-0b71-44e3-90a1-25b5a4f90d72")


def test_schema_recreation_starts_with_empty_policy_catalog(store, lisa_test_db):
    from conftest import provision_lisa_schema

    store.create_activity(title="Temporary activity", source="test")
    repository = PostgresPolicyRepository(store.dsn, WORKSPACE_ID)
    created = repository.replace_scope(
        "workspace",
        "",
        [],
        [{"name": "Temporary rule", "criteria": "This data is discarded.", "approved": True}],
    )
    writer_role = urlparse(lisa_test_db.writer_url).username
    reader_role = urlparse(lisa_test_db.reader_url).username

    provision_lisa_schema(
        lisa_test_db.provision_url,
        WORKSPACE_ID,
        writer_role=writer_role,
        reader_role=reader_role,
    )

    assert created
    assert repository.list_scope("workspace") == []
    recreated = PostgresActivityStore(lisa_test_db.writer_url, WORKSPACE_ID)
    assert recreated.list_activity_feed(limit=10) == []


def test_activity_feed_and_monitor_thread_follow_activity_lifecycle(store):
    activity = store.create_activity(
        title="Review refund session",
        source="mcp",
        kind="policy_evaluation",
        activity_type="task_running",
        subtitle="Checking approved policies",
        task_label="Refund review",
        agent_id="refund-agent",
    )

    recent = store.list_activity_feed(activity_type="task_running", limit=10)
    assert len(recent) == 1
    assert recent[0]["activity_key"] == activity.id
    assert recent[0]["status"] == "pending"
    assert recent[0]["title"] == "Review refund session"
    assert recent[0]["task_label"] == "Refund review"

    detail = store.get_activity_thread(activity.id)
    assert detail["thread"]["activity_thread_id"] == activity.id
    assert detail["thread"]["status"] == "pending"
    assert detail["actions"][0]["title"] == "Review refund session"
    assert detail["actions"][0]["status"] == "pending"

    store.finish_activity_without_claim(activity.id, "Review complete.")

    recent = store.list_activity_feed(activity_type="task_running", limit=10)
    assert recent[0]["status"] == "success"
    detail = store.get_activity_thread(activity.id)
    assert detail["thread"]["status"] == "success"
    assert detail["thread"]["finish_message"] == "Review complete."
    assert all(action["status"] == "success" for action in detail["actions"])


def test_activity_update_and_sections_are_visible_in_thread(store):
    activity = store.create_activity(title="Prepare report", source="mcp")
    store.update_activity_presentation(
        activity.id,
        title="Prepare weekly report",
        subtitle="Collecting current results",
        status_title="In progress",
        task_label="Weekly report",
        details={"source": "scheduled"},
    )
    store.set_activity_section(
        activity.id,
        section_key="research",
        title="Research",
        status="pending",
        section_order=1,
    )
    store.set_activity_section(
        activity.id,
        section_key="draft",
        title="Draft",
        status="pending",
        section_order=2,
    )

    entry = store.list_activity_feed(limit=10)[0]
    detail = store.get_activity_thread(activity.id)
    assert entry["title"] == "Prepare weekly report"
    assert entry["subtitle"] == "Collecting current results"
    assert entry["status_title"] == "In progress"
    assert entry["details_json"] == {"source": "scheduled"}
    assert detail["thread"]["name"] == "Prepare weekly report"
    assert [section["section_key"] for section in detail["sections"]] == [
        "research", "draft"
    ]


def test_activity_progress_search_and_next_action_are_durable(store):
    activity = store.create_activity(
        title="Investigate refund workflow", source="web",
        next_action="Inspect the latest session", origin_conversation_id="conversation-1",
        origin_slack_channel_id="C123", origin_slack_thread_ts="123.45",
    )
    store.update_activity_progress(
        activity.id, status="running", summary="Found the matching session.",
        next_action="Check the policy result", next_action_at="2026-10-20T10:00:00+00:00",
    )
    rows = store.list_activity_feed(query="refund", open_only=True, limit=10)
    assert len(rows) == 1
    assert rows[0]["next_action"] == "Check the policy result"
    detail = store.get_activity_thread(activity.id)
    assert detail["actions"][-1]["summary"] == "Found the matching session."
    assert detail["activity"]["origin_slack_channel_id"] == "C123"
    assert store.complete_agent_activity(activity.id, "Finished.", status="succeeded")
    assert store.list_activity_feed(open_only=True, limit=10) == []


def test_progress_cannot_change_evaluator_or_leased_activity(store):
    from lisa.repositories.activities import _sql

    evaluator = store.create_activity(title="Evaluate policy", source="evaluator", kind="policy_evaluation")
    with pytest.raises(ValueError, match="already complete"):
        store.update_activity_progress(
            evaluator.id, status="waiting", summary="Not permitted",
        )
    leased = store.create_activity(title="Claimed work", source="worker")
    with store._connect() as connection, connection.transaction():
        connection.execute(_sql("claim_activity.sql"), {
            "activity_id": leased.id, "owner": "worker-1", "lease_seconds": 300,
        })
    with pytest.raises(ValueError, match="already complete"):
        store.update_activity_progress(leased.id, status="waiting", summary="Not permitted")


def test_activity_followup_requires_approval_and_claims_once(store):
    activity = store.create_activity(title="Review next session", source="web")
    followup_id = store.create_activity_followup(
        activity_id=activity.id, due_at="2000-01-01T00:00:00+00:00",
        trigger_event_type=None, prompt="Review the next matching session.", requested_by="web",
    )
    assert store.claim_due_activity_followups() == []
    assert store.approve_activity_followup(followup_id)
    claimed = store.claim_due_activity_followups()
    assert len(claimed) == 1 and str(claimed[0]["id"]) == followup_id
    assert store.claim_due_activity_followups() == []
    store.finish_activity_followup(followup_id)


def test_open_followup_keeps_parent_open_until_execution_starts(store):
    activity = store.create_activity(title="Review next session", source="web")
    followup_id = store.create_activity_followup(
        activity_id=activity.id, due_at="2000-01-01T00:00:00+00:00",
        trigger_event_type=None, prompt="Review the next matching session.", requested_by="web",
    )
    assert not store.complete_agent_activity(activity.id, "Done.", status="succeeded")
    store.approve_activity_followup(followup_id)
    assert not store.complete_agent_activity(activity.id, "Done.", status="succeeded")
    store.claim_due_activity_followups()
    assert not store.complete_agent_activity(activity.id, "Done.", status="succeeded")
    store.finish_activity_followup(followup_id)
    assert store.complete_agent_activity(activity.id, "Follow-up completed.", status="succeeded")


def test_event_triggered_followup_waits_for_matching_activity_event(store):
    activity = store.create_activity(title="Wait for approval result", source="web")
    followup_id = store.create_activity_followup(
        activity_id=activity.id, due_at=None, trigger_event_type="approval_received",
        prompt="Review the approval outcome.", requested_by="web",
    )
    store.approve_activity_followup(followup_id)
    assert store.claim_due_activity_followups() == []
    store.record_activity_event(activity.id, "approval_received", title="Approval received")
    claimed = store.claim_due_activity_followups()
    assert len(claimed) == 1 and str(claimed[0]["id"]) == followup_id


def test_policy_proposal_is_shared_and_resolved_by_another_conversation(store):
    activity = store.create_activity(title="Update refund policy", source="web")
    proposal_id = store.create_policy_proposal(
        activity_id=activity.id, scope="workspace", target_agent_name="",
        policy_name="Refund eligibility", expected_content="No policy exists yet.",
        proposed_content="## Refund eligibility\napproved: no\n\nCheck eligibility.",
        created_by_conversation="main-chat",
    )
    pending = store.get_policy_proposal(proposal_id)
    assert pending["created_by_conversation"] == "main-chat"
    assert str(store.claim_policy_proposal(proposal_id)["id"]) == proposal_id
    assert store.resolve_policy_proposal(
        proposal_id, status="applied", conversation_id="slack-thread",
    )
    assert store.get_policy_proposal(proposal_id) is None
    assert store.get_activity_thread(activity.id)["actions"][-1]["title"] == "Policy proposal applied"


def test_policy_proposal_redacts_credentials_before_persistence(store):
    activity = store.create_activity(title="Review credentials", source="web")
    proposal_id = store.create_policy_proposal(
        activity_id=activity.id, scope="workspace", target_agent_name="",
        policy_name="Credential handling", expected_content="No policy exists yet.",
        proposed_content="## Credential handling\napproved: no\n\napi_key=sk-12345678901234567890",
        created_by_conversation="main-chat",
    )
    proposal = store.get_policy_proposal(proposal_id)
    assert "sk-12345678901234567890" not in proposal["proposed_content"]
    assert "[REDACTED]" in proposal["proposed_content"]


def test_agent_owned_activity_can_be_completed_once(store):
    activity = store.create_activity(title="Prepare report", source="mcp")

    assert store.finish_activity_without_claim(
        activity.id, "Report delivered.", status="succeeded"
    )
    assert not store.finish_activity_without_claim(
        activity.id, "Duplicate completion.", status="succeeded"
    )

    detail = store.get_activity_thread(activity.id)
    assert detail["thread"]["status"] == "success"
    assert detail["thread"]["finish_message"] == "Report delivered."
    assert all(action["status"] == "success" for action in detail["actions"])


def test_evaluation_activity_cannot_be_completed_by_agent_tool(store):
    pending_evaluation = store.create_activity(
        title="Evaluate session", source="evaluation", kind="policy_evaluation"
    )
    assert not store.complete_agent_activity(
        pending_evaluation.id, "Evaluation still owns this lifecycle."
    )

    evaluation = _create_evaluation(store)

    assert not store.complete_agent_activity(
        evaluation.activity_id, "Evaluation still owns this lifecycle."
    )
    assert store.get_activity_thread(evaluation.activity_id)["thread"]["status"] == "pending"


def test_activity_thread_references_are_queryable(store):
    activity = store.create_activity(
        title="Run task", source="mcp", agent_id="agent-1", message_id="message-1"
    )
    store.link_activity_reference(activity.id, "runtime_work", "work-1")
    store.link_activity_reference(activity.id, "carrier_message", "carrier-1")

    detail = store.get_activity_thread(activity.id)
    assert detail["references"] == [
        {"reference_type": "agent", "reference_id": "agent-1"},
        {"reference_type": "carrier_message", "reference_id": "carrier-1"},
        {"reference_type": "message", "reference_id": "message-1"},
        {"reference_type": "runtime_work", "reference_id": "work-1"},
    ]


def test_release_activity_updates_thread_action_projection(store):
    activity = _create_evaluation(store)
    claim = store.claim_activity(activity.id, "worker-a", lease_seconds=60)
    assert claim is not None

    store.release_activity(
        activity.id, actor="worker-a", fencing_token=claim.fencing_token,
        reason="retry after provider timeout",
    )

    detail = store.get_activity_thread(activity.id)
    assert detail["actions"][-1]["event_type"] == "retryable_error"
    assert all(action["status"] == "pending" for action in detail["actions"])


def test_on_disk_policy_markdown_is_not_loaded_into_postgres(
    store: PostgresActivityStore, tmp_path: Path
):
    writer_dsn = store.dsn
    agent_name = f"disk-agent-{uuid4()}"
    context = WorkspaceContext(
        workspace=WORKSPACE_ID,
        data_dir=tmp_path,
        database_url=writer_dsn,
    )
    agent_hash = sha256(agent_name.encode("utf-8")).hexdigest()
    policy_path = context.home() / "agents" / agent_hash / "POLICY.md"
    policy_path.parent.mkdir(parents=True)
    policy_path.write_text(
        "## Disk cancellation rule\napproved: yes\n\nCheck ticket eligibility first.",
        encoding="utf-8",
    )

    services = build_services(context)
    repository = PostgresPolicyRepository(writer_dsn, WORKSPACE_ID)

    assert services.policy_service.approved_rules(agent_name) == []
    assert repository.list_scope_history("agent", agent_name) == []
    assert list_policies(services) == "No policies exist yet."


def test_evaluation_creation_is_idempotent_and_intentional_rerun_is_distinct(
    store: PostgresActivityStore,
):
    request_key = str(uuid4())
    first = _create_evaluation(store, request_key)
    retry = store.create_evaluation(
        title="Check refund eligibility",
        source="test",
        target_agent_name="quickstart-refund-agent",
        session_id=first.session_id,
        policy_name="refund eligibility",
        policy_id=first.policy_id,
        policy_hash="policy-sha256",
        judge_config={"model": "test-judge"},
        request_key=request_key,
    )
    rerun = store.create_evaluation(
        title="Check refund eligibility again",
        source="test",
        target_agent_name="quickstart-refund-agent",
        session_id=first.session_id,
        policy_name="refund eligibility",
        policy_id=first.policy_id,
        policy_hash="policy-sha256",
        judge_config={"model": "test-judge"},
        request_key=str(uuid4()),
    )

    assert retry.id == first.id
    assert not retry.created
    assert rerun.id != first.id
    assert rerun.created
    assert store.get_evaluation(first.id).policy_id == first.policy_id


def test_policy_triage_decisions_are_durable_and_pair_keyed(store: PostgresActivityStore):
    policy_id = _test_policy_id(store)
    decisions = [{"id": policy_id, "decision": "no_violation_found"}]

    store.record_policy_triage_decisions("session-1", decisions)
    store.record_policy_triage_decisions(
        "session-1", [{"id": policy_id, "decision": "suspected_violation"}]
    )

    assert store.policy_triage_decisions("session-1", [policy_id]) == {
        policy_id: "no_violation_found"
    }
    assert store.policy_triage_decisions("session-2", [policy_id]) == {}


def test_concurrent_retries_create_one_evaluation(store: PostgresActivityStore):
    request_key = str(uuid4())
    session_id = f"session-{uuid4()}"
    policy_id = _test_policy_id(store)

    def create():
        return store.create_evaluation(
            title="Concurrent evaluation",
            source="test",
            target_agent_name="quickstart-refund-agent",
            session_id=session_id,
            policy_name="refund eligibility",
            policy_id=policy_id,
            policy_hash="policy-sha256-concurrent",
            judge_config={"model": "test-judge"},
            request_key=request_key,
        )

    with ThreadPoolExecutor(max_workers=6) as workers:
        results = list(workers.map(lambda _: create(), range(6)))

    assert len({result.id for result in results}) == 1
    assert sum(result.created for result in results) == 1


def test_concurrent_claims_have_one_owner(store: PostgresActivityStore):
    activity = _create_evaluation(store)

    def claim(index: int):
        return store.claim_activity(activity.id, f"worker-{index}", lease_seconds=60)

    with ThreadPoolExecutor(max_workers=6) as workers:
        claims = list(workers.map(claim, range(6)))

    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1


def test_concurrent_event_appends_allocate_unique_ordered_sequences(
    store: PostgresActivityStore,
):
    activity = _create_evaluation(store)
    claim = store.claim_activity(activity.id, "worker-a", lease_seconds=60)
    assert claim is not None

    def append(index: int):
        return store.append_event(
            activity.id,
            "progress",
            title=f"Progress {index}",
            actor="worker-a",
            fencing_token=claim.fencing_token,
        )

    with ThreadPoolExecutor(max_workers=8) as workers:
        sequences = list(workers.map(append, range(20)))

    assert sorted(sequences) == list(range(3, 23))


def test_claim_fencing_rejects_expired_worker_writes(store: PostgresActivityStore):
    activity = _create_evaluation(store)
    stale_claim = store.claim_activity(activity.id, "worker-a", lease_seconds=1)
    assert stale_claim is not None
    time.sleep(1.05)
    current_claim = store.claim_activity(activity.id, "worker-b", lease_seconds=60)

    assert current_claim is not None
    assert current_claim.fencing_token > stale_claim.fencing_token
    with pytest.raises(StaleLeaseError):
        store.append_event(
            activity.id,
            "progress",
            title="Old worker update",
            actor="worker-a",
            fencing_token=stale_claim.fencing_token,
        )


def test_expired_lease_is_rejected_after_waiting_for_row_lock(
    store: PostgresActivityStore,
):
    activity = _create_evaluation(store)
    claim = store.claim_activity(activity.id, "worker-a", lease_seconds=1)
    assert claim is not None
    blocker = store._connect()
    blocker.execute("BEGIN")
    blocker.execute(_test_sql("hold_activity_row.sql"), (activity.id,))
    try:
        with ThreadPoolExecutor(max_workers=1) as workers:
            pending = workers.submit(
                store.append_event,
                activity.id,
                "progress",
                title="late update",
                actor="worker-a",
                fencing_token=claim.fencing_token,
            )
            time.sleep(1.1)
            blocker.commit()
            with pytest.raises(StaleLeaseError):
                pending.result(timeout=5)
    finally:
        blocker.close()


def test_runtime_roles_are_separated_from_provision(
    store: PostgresActivityStore, lisa_test_db
):

    with psycopg.connect(lisa_test_db.reader_url, autocommit=True) as reader:
        assert reader.execute(_test_sql("count_readable_activities.sql")).fetchone()
        reader.execute(_test_sql("read_policies.sql"))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute(_test_sql("write_policies.sql"))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute(_test_sql("read_base_activities.sql"))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute(_test_sql("reader_ddl.sql"))

    with psycopg.connect(lisa_test_db.writer_url, autocommit=True) as writer:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            writer.execute(_test_sql("rewrite_deployment.sql"))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            writer.execute(_test_sql("writer_ddl.sql"))


def test_postgres_policy_records_keep_string_ids_and_agent_scope(store, tmp_path):
    agent_name = f"policy-test-{uuid4()}"
    title = f"rule-{uuid4()}"
    repository = PostgresPolicyRepository(store.dsn, WORKSPACE_ID)
    policies = PolicyService(
        WorkspaceContext(
            workspace=WORKSPACE_ID,
            data_dir=tmp_path,
            database_url=store.dsn,
            confirmed_policy_title=title,
        ),
        repository=repository,
    )
    policies.update_policy(
        f"## {title}\napproved: yes\n\nWorkspace requirement.",
        "",
        "workspace",
        "",
    )
    workspace_rule = next(
        rule for rule in policies.approved_rules(agent_name) if rule.name == title
    )
    policies.update_policy(
        f"## {title}\napproved: yes\n\nAgent requirement.",
        "",
        "agent",
        agent_name,
    )
    first = next(rule for rule in policies.approved_rules(agent_name) if rule.name == title)
    services = build_services(policies.context)
    listing = list_policies(services)
    workspace_text = read_policy(services)
    agent_text = read_policy(services, "agent", agent_name)
    policies.update_policy(
        f"## {title}\napproved: yes\n\nCheck the changed requirement.",
        read_policy(services, "agent", agent_name),
        "agent",
        agent_name,
    )
    updated = next(rule for rule in policies.approved_rules(agent_name) if rule.name == title)

    assert re.fullmatch(r"P1\.[1-9][0-9]*", first.id)
    assert workspace_rule.id != first.id
    assert any(
        f"{workspace_rule.id} | {title} | Applies to: all agents by default" in row
        for row in listing
    )
    assert any(
        f"{first.id} | {title} | Applies to: {agent_name} only" in row
        for row in listing
    )
    assert f"## {workspace_rule.id} | {title}" in workspace_text
    assert "Workspace requirement." in workspace_text
    assert f"## {first.id} | {title}" in agent_text
    assert "Agent requirement." in agent_text
    assert updated.id == first.id
    assert updated.criteria == "Check the changed requirement."


def test_policy_primary_key_column_uses_postgres_text(store):

    with psycopg.connect(store.dsn) as connection:
        row = connection.execute(_test_sql("policy_id_column_type.sql")).fetchone()
    assert row == ("text",)


def test_claim_can_be_renewed_and_still_rejects_old_fencing_tokens(
    store: PostgresActivityStore,
):
    activity = _create_evaluation(store)
    claim = store.claim_activity(activity.id, "worker-a", lease_seconds=1)
    assert claim is not None
    renewed = store.renew_lease(activity.id, "worker-a", claim.fencing_token, lease_seconds=60)
    assert renewed is not None
    assert renewed.fencing_token == claim.fencing_token
    assert renewed.lease_expires_at > claim.lease_expires_at
    store.append_event(
        activity.id,
        "progress",
        title="still active",
        actor="worker-a",
        fencing_token=renewed.fencing_token,
    )


def test_events_and_context_are_ordered_and_require_current_claim(
    store: PostgresActivityStore,
):
    activity = _create_evaluation(store)
    claim = store.claim_activity(activity.id, "worker-a", lease_seconds=60)
    assert claim is not None

    first_seq = store.append_event(
        activity.id,
        "session_detail_requested",
        title="Retrieving session detail",
        actor="worker-a",
        fencing_token=claim.fencing_token,
    )
    second_seq = store.append_event(
        activity.id,
        "evaluation_inputs_saved",
        title="Evaluation inputs saved",
        actor="worker-a",
        fencing_token=claim.fencing_token,
    )
    context_id = store.save_context_item(
        activity.id,
        "evaluation_inputs",
        {
            "session_id": activity.session_id,
            "policy_id": "P1.1",
            "policy_name": "Rule",
            "target_agent_name": "agent",
            "spans": ["span-1"],
        },
        actor="worker-a",
        fencing_token=claim.fencing_token,
    )

    assert (first_seq, second_seq) == (3, 4)
    assert [event.title for event in store.list_events(activity.id)] == [
        "Check refund eligibility",
        "Activity started",
        "Retrieving session detail",
        "Evaluation inputs saved",
        "Saved evaluation inputs",
    ]
    context = store.get_context_item(context_id)
    assert context.kind == "evaluation_inputs"
    assert context.content["session_id"] == activity.session_id


def test_activity_cannot_finish_before_score_acknowledgement(store: PostgresActivityStore):
    activity = _create_evaluation(store)
    claim = store.claim_activity(activity.id, "worker-a", lease_seconds=60)
    assert claim is not None

    with pytest.raises(ValueError, match="score outbox"):
        store.finish_activity(
            activity.id,
            status="succeeded",
            actor="worker-a",
            fencing_token=claim.fencing_token,
        )


def test_verdict_constraint_accepts_current_evaluation_outcomes(
    store: PostgresActivityStore,
):

    activity = _create_evaluation(store)
    with psycopg.connect(store.dsn) as connection:
        for verdict in ("pass", "fail", "uncertain", "insufficient_evidence"):
            connection.execute(
                _test_sql("set_evaluation_verdict.sql"),
                (verdict, activity.id),
            )


def test_activity_records_generic_events_and_multiple_evaluations(store: PostgresActivityStore):
    activity = store.create_activity(title="Evaluate session", source="test")
    policy_ids = [_test_policy_id(store)]
    policy_repo = PostgresPolicyRepository(store.dsn, WORKSPACE_ID)
    current = policy_repo.list_scope("agent", "quickstart-refund-agent")
    saved = policy_repo.replace_scope(
        "agent",
        "quickstart-refund-agent",
        [{"name": item.name, "criteria": item.criteria, "approved": item.approved}
         for item in current],
        [{"name": item.name, "criteria": item.criteria, "approved": item.approved}
         for item in current]
        + [{"name": f"second-{uuid4()}", "criteria": "Another rule.", "approved": True}],
    )
    policy_ids.append(saved[-1].id)
    run_ids = []
    for policy_id in policy_ids:
        evaluation = store.create_evaluation(
            activity_id=activity.id,
            title="Evaluate session",
            source="test",
            target_agent_name="quickstart-refund-agent",
            session_id="same-session",
            policy_name="Rule",
            policy_id=policy_id,
            policy_hash=f"hash-{policy_id}",
            judge_config={"model": "test-judge"},
            request_key=str(uuid4()),
        )
        run_ids.append(evaluation.id)

    store.record_activity_event(
        activity.id, "message_received", title="Owner asked for a review", summary="chat"
    )
    store.record_activity_event(
        activity.id, "report_prepared", title="Review summary prepared", summary="report.md"
    )

    assert len(set(run_ids)) == 2
    assert {store.get_evaluation(run_id).activity_id for run_id in run_ids} == {activity.id}
    assert [event.event_type for event in store.list_events(activity.id)][-2:] == [
        "message_received",
        "report_prepared",
    ]
