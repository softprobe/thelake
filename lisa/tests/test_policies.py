import hashlib
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from pathlib import Path
from threading import Barrier, Event

import pytest
from policy_cases import list_policies, read_policy

from benchmark.policy_seed import seed_policy_document
from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.models.policies import PolicyRecord
from lisa.repositories.policies import InMemoryPolicyRepository, _sql
from lisa.services.policies import PolicyService
from lisa.views.policies import render_policy_list, render_policy_records


@pytest.fixture(autouse=True)
def isolated_data_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_TURN_SOURCE", "web")


@pytest.fixture
def policy_database(monkeypatch, lisa_test_database):
    database = lisa_test_database()
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATABASE_READ_URL", database.reader_url)
    return database


def test_policy_tools_scope_records_per_workspace(monkeypatch, lisa_test_database, policy_database):
    second = "22222222-2222-4222-8222-222222222222"
    services = build_services(WorkspaceContext.from_environment())
    current = read_policy(services)
    draft = "# Workspace policy\n\n## Refund Eligibility Policy\n\nCheck eligibility before refunding.\n"

    seed_policy_document(services.policy_service, draft, current)

    saved = read_policy(services)
    assert "# Workspace policies" in saved
    assert "## P1.1 | Refund Eligibility Policy" in saved
    assert "approved: no" in saved
    assert "Check eligibility before refunding." in saved
    monkeypatch.setenv("LISA_WORKSPACE_ID", second)
    second_database = lisa_test_database(second)
    monkeypatch.setenv("LISA_DATABASE_URL", second_database.writer_url)
    monkeypatch.setenv("LISA_DATABASE_READ_URL", second_database.reader_url)
    services = build_services(WorkspaceContext.from_environment())
    assert read_policy(services) == "No policy exists yet."


def test_policy_records_have_stable_string_ids_and_database_shaped_fields(
    policy_database
):
    context = WorkspaceContext.from_environment()
    services = build_services(context)
    seed_policy_document(services.policy_service,
        "## Refund eligibility\napproved: yes\n\nCheck eligibility before refunding.", ""
    )
    rule = services.policy_service.approved_rules("refund-agent")[0]
    assert rule.id == "P1.1"
    listing = list_policies(services)[0]
    assert "Refund eligibility" in listing
    assert "P1.1 | Refund eligibility" in listing
    assert "Applies to: all agents by default" in listing
    assert "approved: yes" in listing
    assert rule.id in listing

    seed_policy_document(services.policy_service,
        "## Refund eligibility\napproved: yes\n\nCheck the refund before issuing it.",
        read_policy(services),
    )
    updated = services.policy_service.approved_rules("refund-agent")[0]
    assert updated.id == rule.id
    assert updated.criteria == "Check the refund before issuing it."
    assert len(services.policy_service.list_policy_records()) == 1


def test_policy_ids_use_paragraph_style_ordinals(policy_database):
    context = WorkspaceContext.from_environment()
    repository = build_services(context).policy_service.repository
    rules = [
        {"name": f"Rule {number}", "criteria": f"Rule {number}.", "approved": True}
        for number in range(1, 11)
    ]

    saved = repository.seed_scope(
        "workspace",
        "",
        [],
        rules,
    )

    by_name = {rule.name: rule.id for rule in saved}
    assert by_name["Rule 1"] == "P1.1"
    assert by_name["Rule 9"] == "P1.9"
    assert by_name["Rule 10"] == "P1.10"

    rendered = render_policy_records(saved, "workspace")
    assert rendered.index("## P1.1 | Rule 1") < rendered.index("## P1.10 | Rule 10")


def test_policy_approval_serializes_with_scope_writes(policy_database):
    context = WorkspaceContext.from_environment()
    repository = build_services(context).policy_service.repository
    pending = repository.write_policy("workspace", "", "Refund review", "Review first.")
    executor = ThreadPoolExecutor(max_workers=1)
    approval_started = Event()

    try:
        with repository._connect() as lock_connection:
            lock_connection.execute(
                _sql("lock_policy_scope.sql"), {"lock_key": "workspace:"}
            )
            def approve():
                approval_started.set()
                return repository.approve_policy(pending.id)

            approval = executor.submit(approve)
            assert approval_started.wait(timeout=5)
            with pytest.raises(FutureTimeoutError):
                approval.result(timeout=0.1)
        assert approval.result(timeout=5).approved
    finally:
        executor.shutdown(wait=True)


def test_write_policy_updates_approved_row_in_place_and_requires_reapproval(policy_database):
    context = WorkspaceContext.from_environment()
    repository = build_services(context).policy_service.repository
    pending = repository.write_policy("workspace", "", "Refund review", "Review first.")
    approved = repository.approve_policy(pending.id)

    updated = repository.write_policy(
        "workspace", "", approved.name, "Changed after approval.", policy_id=approved.id
    )

    assert updated.id == approved.id
    assert updated.approved is False
    assert updated.criteria == "Changed after approval."
    assert repository.list_all() == [updated]
    assert repository.approve_policy(updated.id).approved is True


def test_policy_ids_continue_from_the_greatest_stored_ordinal(policy_database):
    context = WorkspaceContext.from_environment()
    repository = build_services(context).policy_service.repository
    first = repository.write_policy("workspace", "", "Existing one", "First.")
    second = repository.write_policy("agent", "agent-a", "Existing two", "Second.")
    assert (first.id, second.id) == ("P1.1", "P1.2")

    created = repository.write_policy("workspace", "", "Customer greeting", "Greet the customer.")

    assert created.id == "P1.3"
    assert created.name == "Customer greeting"


def test_policy_id_allocation_serializes_first_writes_across_scopes(policy_database):
    context = WorkspaceContext.from_environment()
    repository = build_services(context).policy_service.repository
    start = Barrier(2)

    def create(scope, agent_name, name):
        start.wait(timeout=5)
        return repository.write_policy(scope, agent_name, name, f"{name} criteria.")

    with ThreadPoolExecutor(max_workers=2) as executor:
        workspace = executor.submit(create, "workspace", "", "Workspace greeting")
        agent = executor.submit(create, "agent", "billing-agent", "Billing greeting")
        rows = [workspace.result(timeout=10), agent.result(timeout=10)]

    assert {row.id for row in rows} == {"P1.1", "P1.2"}
    assert {row.name for row in repository.list_all()} == {
        "Workspace greeting", "Billing greeting",
    }


def test_write_policy_updates_the_selected_row(policy_database):
    context = WorkspaceContext.from_environment()
    repository = build_services(context).policy_service.repository
    created = repository.write_policy(
        "agent", "conversation-agent", "Conversation greeting", "Greet the customer."
    )
    approved = repository.approve_policy(created.id)

    updated = repository.write_policy(
        "agent",
        "conversation-agent",
        "Conversation greeting",
        "Greet the customer. Ask a clarifying question when needed.",
        policy_id=approved.id,
    )

    assert updated.id == created.id == approved.id == "P1.1"
    assert updated.approved is False
    assert updated.criteria == "Greet the customer. Ask a clarifying question when needed."
    assert repository.list_all() == [updated]


def test_dotted_policy_ids_identify_separate_policies():
    from lisa.models.policies import is_policy_id

    assert is_policy_id("P1.1")
    assert is_policy_id("P1.1.1")


def test_write_policy_sql_only_updates_the_selected_row():
    statement = _sql("write_policy.sql")

    assert statement.lstrip().startswith("UPDATE lisa.policies")
    assert "WHERE id = %(id)s" in statement
    assert "AND scope = %(scope)s" in statement
    assert "AND target_agent_name = %(target_agent_name)s" in statement
    assert "INSERT" + " INTO" not in statement


def test_policy_update_clears_cached_triage_for_same_policy_id(policy_database):
    context = WorkspaceContext.from_environment()
    repository = build_services(context).policy_service.repository
    pending = repository.write_policy("workspace", "", "Refund review", "Review first.")
    repository.approve_policy(pending.id)
    with repository._connect() as connection:
        connection.execute(
            (Path(__file__).parent / "sql/policies/record_policy_triage.sql")
            .read_text(encoding="utf-8"),
            {"session_id": "session-before-policy-update", "policy_id": pending.id},
        )

    updated = repository.write_policy(
        "workspace", "", pending.name, "Review the reason first.", policy_id=pending.id
    )

    assert updated.id == pending.id
    assert not updated.approved
    with repository._connect() as connection:
        assert connection.execute(
            (Path(__file__).parent / "sql/policies/list_policy_triage.sql")
            .read_text(encoding="utf-8"),
            {"policy_id": pending.id},
        ).fetchall() == []


def test_delete_policy_removes_catalog_row_and_cached_triage(policy_database):
    context = WorkspaceContext.from_environment()
    repository = build_services(context).policy_service.repository
    policy = repository.write_policy("workspace", "", "Temporary rule", "Retire this rule.")
    with repository._connect() as connection:
        connection.execute(
            (Path(__file__).parent / "sql/policies/record_policy_triage.sql")
            .read_text(encoding="utf-8"),
            {"session_id": "session-before-policy-delete", "policy_id": policy.id},
        )

    repository.delete_policy(policy.id)

    with repository._connect() as connection:
        assert connection.execute(
            (Path(__file__).parent / "sql/policies/read_policy_id.sql")
            .read_text(encoding="utf-8"),
            {"policy_id": policy.id},
        ).fetchall() == []
        assert connection.execute(
            (Path(__file__).parent / "sql/policies/list_policy_triage.sql")
            .read_text(encoding="utf-8"),
            {"policy_id": policy.id},
        ).fetchall() == []


def test_delete_policy_preserves_evaluation_snapshot_without_policy_row(policy_database):
    context = WorkspaceContext.from_environment()
    services = build_services(context)
    repository = services.policy_service.repository
    policy = repository.write_policy("workspace", "", "Evaluated rule", "Keep its history.")
    activity = services.activity_store.create_activity(
        title="Historical evaluation", source="test", kind="policy_evaluation"
    )
    with repository._connect() as connection:
        connection.execute(
            (Path(__file__).parent / "sql/policies/seed_evaluation_run.sql")
            .read_text(encoding="utf-8"),
            {
                "evaluation_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "activity_id": activity.id,
                "policy_id": policy.id,
                "target_agent_name": "agent-a",
                "session_id": "historical-session",
                "policy_name": policy.name,
                "policy_hash": hashlib.sha256(policy.criteria.encode()).hexdigest(),
                "judge_config": "{}",
                "request_key": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            },
        )

    repository.delete_policy(policy.id)
    assert repository.list_all() == []
    with repository._connect() as connection:
        history = connection.execute(
            (Path(__file__).parent / "sql/policies/read_evaluation_snapshot.sql")
            .read_text(encoding="utf-8")
        ).fetchone()
    assert history == {
        "policy_id": policy.id,
        "policy_name": policy.name,
        "policy_hash": hashlib.sha256(policy.criteria.encode()).hexdigest(),
    }


def test_policy_page_at_maximum_offset_has_no_unreachable_successor():
    class Repository:
        def list_all_page(self, *, limit, offset):
            assert limit == 101
            assert offset == 10_000
            return [
                PolicyRecord(
                    id=f"P1.{number}", scope="workspace", target_agent_name=None,
                    name=f"Rule {number}", criteria=f"Rule {number}.", approved=True,
                )
                for number in range(10_001, 10_102)
            ]

    service = PolicyService(
        WorkspaceContext.from_environment(), Repository()
    )
    page, next_offset = service.list_policy_records_page(offset=10_000)

    assert len(page) == 100
    assert next_offset is None


def test_policy_listing_explains_agent_specific_scope(policy_database):
    context = WorkspaceContext.from_environment()
    services = build_services(context)
    seed_policy_document(services.policy_service,
        "## Refund eligibility\napproved: no\n\nDo not issue the refund.",
        "No policy exists yet.",
        scope="agent",
        agent_name="dogfood-refund-agent",
    )

    listing = list_policies(services)[0]

    assert "Refund eligibility" in listing
    assert "P1.1 | Refund eligibility" in listing
    assert "Applies to: dogfood-refund-agent only" in listing
    assert "approved: no" in listing
    assert "P1." in listing


def test_policy_listing_orders_paragraph_ids_numerically(tmp_path):
    context = WorkspaceContext(
        workspace="11111111-1111-4111-8111-111111111111",
        data_dir=tmp_path,
    )
    repository = InMemoryPolicyRepository()
    repository.seed_scope(
        "workspace",
        "",
        [],
        [
            {"name": f"Rule {index}", "criteria": f"Criteria {index}", "approved": False}
            for index in (10, 2, 1)
        ],
    )
    rows = repository.list_all()
    repository.list_all = lambda: list(reversed(rows))
    policy_service = PolicyService(context, repository=repository)

    listing = render_policy_list(policy_service.list_policy_records())

    assert [item.split(" | ", 1)[0] for item in listing] == ["P1.1", "P1.2", "P1.3"]


def test_policy_tools_redact_secrets_before_persisting_markdown(policy_database):

    services = build_services(WorkspaceContext.from_environment())
    current = read_policy(services)
    draft = '# Workspace policy\n\n## Support credential\n\nKeep credential data private. api_key="policy-secret-value"\n'

    seed_policy_document(services.policy_service, draft, current)
    saved = read_policy(services)

    assert "policy-secret-value" not in saved
    assert "[REDACTED]" in saved


def test_owner_approves_the_policy_named_by_the_tool(monkeypatch, policy_database):
    monkeypatch.setenv("LISA_ACTOR_ID", "owner-1")
    monkeypatch.setenv("LISA_OWNER_USER_IDS", "owner-1")
    monkeypatch.setenv("LISA_TURN_SOURCE", "web")
    service = build_services(WorkspaceContext.from_environment()).policy_service
    first = service.write_policy(
        scope="workspace", agent_name="", name="First rule", criteria="Follow the first rule."
    )
    second = service.write_policy(
        scope="workspace", agent_name="", name="Second rule", criteria="Follow the second rule."
    )

    assert first.id != second.id
    approved_first = service.approve_policy(first.id)
    approved_second = service.approve_policy(second.id)
    assert (approved_first.id, approved_first.approved) == (first.id, True)
    assert (approved_second.id, approved_second.approved) == (second.id, True)
    stored = {row.id: row.approved for row in service.list_policy_records()}
    assert stored[first.id] is True and stored[second.id] is True
    with pytest.raises(ValueError, match="human-readable policy ID"):
        service.approve_policy("not-a-policy")
    with pytest.raises(ValueError, match="Policy ID was not found"):
        service.approve_policy("P1.9")

    monkeypatch.setenv("LISA_TURN_SOURCE", "worker")
    worker = build_services(WorkspaceContext.from_environment()).policy_service
    with pytest.raises(ValueError, match="owner conversation"):
        worker.approve_policy(first.id)

    monkeypatch.setenv("LISA_TURN_SOURCE", "slack")
    monkeypatch.setenv("LISA_ACTOR_ID", "guest")
    guest = build_services(WorkspaceContext.from_environment()).policy_service
    with pytest.raises(ValueError, match="authorized Lisa owner"):
        guest.approve_policy(second.id)


def test_policy_update_rejects_empty_rule_markdown(policy_database):
    with pytest.raises(ValueError, match="readable Markdown"):
        services = build_services(WorkspaceContext.from_environment())
        seed_policy_document(services.policy_service,
            "# Workspace policy\n\n## Inferred rule\n\n",
            "",
        )
