from __future__ import annotations

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import pytest
from sql_cases import provision_lisa_schema

from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.repositories.policies import PostgresPolicyRepository
from lisa.services.policies import PolicyService
from policy_cases import list_policies
from lisa.repositories.work_items import (
    DeploymentBindingError,
    PostgresWorkItemStore,
    StaleLeaseError,
)

WORKSPACE_ID = "8cd71112-21e0-4d45-a8cc-9ec553879df2"


def _test_sql(name: str) -> str:
    return (Path(__file__).parent / "sql" / "work_items" / name).read_text()


@pytest.fixture
def store() -> PostgresWorkItemStore:
    pytest.importorskip("psycopg")
    provision_dsn = os.getenv("LISA_TEST_PROVISION_URL") or os.getenv("LISA_TEST_DATABASE_URL")
    if not provision_dsn:
        pytest.skip("LISA_TEST_PROVISION_URL must point to a disposable PostgreSQL test database")
    if "test" not in urlparse(provision_dsn).path.lower():
        pytest.fail("LISA_TEST_DATABASE_URL must name a disposable database containing 'test'")
    provision_lisa_schema(provision_dsn, WORKSPACE_ID)
    runtime_dsn = os.getenv("LISA_TEST_WRITER_URL") or provision_dsn
    return PostgresWorkItemStore(runtime_dsn, WORKSPACE_ID)


def _create_evaluation(store: PostgresWorkItemStore, request_key: str | None = None):
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


def _test_policy_id(store: PostgresWorkItemStore) -> str:
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


def test_provision_binds_one_database_to_its_workspace(store: PostgresWorkItemStore):
    with pytest.raises(DeploymentBindingError):
        PostgresWorkItemStore(
            os.getenv("LISA_TEST_WRITER_URL")
            or os.getenv("LISA_TEST_PROVISION_URL")
            or os.environ["LISA_TEST_DATABASE_URL"],
            "1480b512-0b71-44e3-90a1-25b5a4f90d72",
        )


def test_on_disk_policy_markdown_is_not_loaded_into_postgres(
    store: PostgresWorkItemStore, tmp_path: Path
):
    writer_dsn = os.getenv("LISA_TEST_WRITER_URL") or os.getenv("LISA_TEST_PROVISION_URL")
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
    store: PostgresWorkItemStore,
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


def test_concurrent_retries_create_one_evaluation(store: PostgresWorkItemStore):
    request_key = str(uuid4())
    session_id = f"session-{uuid4()}"

    def create():
        return store.create_evaluation(
            title="Concurrent evaluation",
            source="test",
            target_agent_name="quickstart-refund-agent",
            session_id=session_id,
            policy_name="refund eligibility",
            policy_id=_test_policy_id(store),
            policy_hash="policy-sha256-concurrent",
            judge_config={"model": "test-judge"},
            request_key=request_key,
        )

    with ThreadPoolExecutor(max_workers=6) as workers:
        results = list(workers.map(lambda _: create(), range(6)))

    assert len({result.id for result in results}) == 1
    assert sum(result.created for result in results) == 1


def test_concurrent_claims_have_one_owner(store: PostgresWorkItemStore):
    work_item = _create_evaluation(store)

    def claim(index: int):
        return store.claim_work_item(work_item.id, f"worker-{index}", lease_seconds=60)

    with ThreadPoolExecutor(max_workers=6) as workers:
        claims = list(workers.map(claim, range(6)))

    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1


def test_concurrent_event_appends_allocate_unique_ordered_sequences(
    store: PostgresWorkItemStore,
):
    work_item = _create_evaluation(store)
    claim = store.claim_work_item(work_item.id, "worker-a", lease_seconds=60)
    assert claim is not None

    def append(index: int):
        return store.append_event(
            work_item.id,
            "progress",
            title=f"Progress {index}",
            actor="worker-a",
            fencing_token=claim.fencing_token,
        )

    with ThreadPoolExecutor(max_workers=8) as workers:
        sequences = list(workers.map(append, range(20)))

    assert sorted(sequences) == list(range(3, 23))


def test_claim_fencing_rejects_expired_worker_writes(store: PostgresWorkItemStore):
    work_item = _create_evaluation(store)
    stale_claim = store.claim_work_item(work_item.id, "worker-a", lease_seconds=1)
    assert stale_claim is not None
    time.sleep(1.05)
    current_claim = store.claim_work_item(work_item.id, "worker-b", lease_seconds=60)

    assert current_claim is not None
    assert current_claim.fencing_token > stale_claim.fencing_token
    with pytest.raises(StaleLeaseError):
        store.append_event(
            work_item.id,
            "progress",
            title="Old worker update",
            actor="worker-a",
            fencing_token=stale_claim.fencing_token,
        )


def test_expired_lease_is_rejected_after_waiting_for_row_lock(
    store: PostgresWorkItemStore,
):
    work_item = _create_evaluation(store)
    claim = store.claim_work_item(work_item.id, "worker-a", lease_seconds=1)
    assert claim is not None
    blocker = store._connect()
    blocker.execute("BEGIN")
    blocker.execute(_test_sql("hold_work_item_row.sql"), (work_item.id,))
    try:
        with ThreadPoolExecutor(max_workers=1) as workers:
            pending = workers.submit(
                store.append_event,
                work_item.id,
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


def test_runtime_roles_are_separated_from_provision(store: PostgresWorkItemStore):
    import psycopg

    reader_dsn = os.getenv("LISA_TEST_READER_URL")
    provision_dsn = os.getenv("LISA_TEST_PROVISION_URL") or os.getenv("LISA_TEST_DATABASE_URL")
    if not reader_dsn or not provision_dsn:
        pytest.skip("test provision and reader role URLs are required")

    with psycopg.connect(reader_dsn, autocommit=True) as reader:
        assert reader.execute(_test_sql("count_readable_work_items.sql")).fetchone()
        reader.execute(_test_sql("read_policies.sql"))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute(_test_sql("write_policies.sql"))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute(_test_sql("read_base_work_items.sql"))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute(_test_sql("reader_ddl.sql"))

    writer_dsn = os.getenv("LISA_TEST_WRITER_URL")
    if not writer_dsn:
        pytest.skip("LISA_TEST_WRITER_URL is required")
    with psycopg.connect(writer_dsn, autocommit=True) as writer:
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
    listing = policies.list_policies()
    workspace_text = policies.read_policy()
    agent_text = policies.read_policy("agent", agent_name)
    policies.update_policy(
        f"## {title}\napproved: yes\n\nCheck the changed requirement.",
        policies.read_policy("agent", agent_name),
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
    assert any(f"{first.id} | {title} | Applies to: {agent_name} only" in row for row in listing)
    assert f"## {workspace_rule.id} | {title}" in workspace_text
    assert "Workspace requirement." in workspace_text
    assert f"## {first.id} | {title}" in agent_text
    assert "Agent requirement." in agent_text
    assert updated.id == first.id
    assert updated.criteria == "Check the changed requirement."


def test_policy_primary_key_column_uses_postgres_text(store):
    import psycopg

    provision_dsn = os.getenv("LISA_TEST_PROVISION_URL") or os.getenv("LISA_TEST_DATABASE_URL")
    with psycopg.connect(provision_dsn) as connection:
        row = connection.execute(_test_sql("policy_id_column_type.sql")).fetchone()
    assert row == ("text",)


def test_claim_can_be_renewed_and_still_rejects_old_fencing_tokens(
    store: PostgresWorkItemStore,
):
    work_item = _create_evaluation(store)
    claim = store.claim_work_item(work_item.id, "worker-a", lease_seconds=1)
    assert claim is not None
    renewed = store.renew_lease(work_item.id, "worker-a", claim.fencing_token, lease_seconds=60)
    assert renewed is not None
    assert renewed.fencing_token == claim.fencing_token
    assert renewed.lease_expires_at > claim.lease_expires_at
    store.append_event(
        work_item.id,
        "progress",
        title="still active",
        actor="worker-a",
        fencing_token=renewed.fencing_token,
    )


def test_events_and_context_are_ordered_and_require_current_claim(
    store: PostgresWorkItemStore,
):
    work_item = _create_evaluation(store)
    claim = store.claim_work_item(work_item.id, "worker-a", lease_seconds=60)
    assert claim is not None

    first_seq = store.append_event(
        work_item.id,
        "session_detail_requested",
        title="Retrieving session detail",
        actor="worker-a",
        fencing_token=claim.fencing_token,
    )
    second_seq = store.append_event(
        work_item.id,
        "evaluation_inputs_saved",
        title="Evaluation inputs saved",
        actor="worker-a",
        fencing_token=claim.fencing_token,
    )
    context_id = store.save_context_item(
        work_item.id,
        "evaluation_inputs",
        {
            "session_id": work_item.session_id,
            "policy_id": "P1.1",
            "policy_name": "Rule",
            "target_agent_name": "agent",
            "spans": ["span-1"],
        },
        actor="worker-a",
        fencing_token=claim.fencing_token,
    )

    assert (first_seq, second_seq) == (3, 4)
    assert [event.title for event in store.list_events(work_item.id)] == [
        "Evaluation accepted",
        "Evaluation started",
        "Retrieving session detail",
        "Evaluation inputs saved",
        "Saved evaluation inputs",
    ]
    context = store.get_context_item(context_id)
    assert context.kind == "evaluation_inputs"
    assert context.content["session_id"] == work_item.session_id


def test_work_item_cannot_finish_before_score_acknowledgement(store: PostgresWorkItemStore):
    work_item = _create_evaluation(store)
    claim = store.claim_work_item(work_item.id, "worker-a", lease_seconds=60)
    assert claim is not None

    with pytest.raises(ValueError, match="score outbox"):
        store.finish_work_item(
            work_item.id,
            status="succeeded",
            actor="worker-a",
            fencing_token=claim.fencing_token,
        )


def test_verdict_constraint_accepts_current_evaluation_outcomes(
    store: PostgresWorkItemStore,
):
    import psycopg

    work_item = _create_evaluation(store)
    with psycopg.connect(store.dsn) as connection:
        for verdict in ("pass", "fail", "uncertain", "insufficient_evidence"):
            connection.execute(
                _test_sql("set_evaluation_verdict.sql"),
                (verdict, work_item.id),
            )
