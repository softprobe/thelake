from benchmark.policy_seed import seed_policy_document

import hashlib
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from threading import Event

import pytest

from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.models.policies import PolicyRecord
from lisa.repositories.policies import InMemoryPolicyRepository, PostgresPolicyRepository, _sql
from lisa.services.policies import PolicyService
from policy_cases import list_policies, read_policy
from lisa.views.policies import render_policy_list, render_policy_records


@pytest.fixture(autouse=True)
def isolated_data_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_TURN_SOURCE", "web")
    monkeypatch.delenv("LISA_CONFIRMED_POLICY_ID", raising=False)


@pytest.fixture
def policy_database(monkeypatch, lisa_test_database):
    database = lisa_test_database()
    monkeypatch.setenv("LISA_DATABASE_URL", database.writer_url)
    monkeypatch.setenv("LISA_DATABASE_READ_URL", database.reader_url)


def test_policy_tools_scope_records_per_workspace(monkeypatch, lisa_test_database, policy_database):
    first = "11111111-1111-4111-8111-111111111111"
    second = "22222222-2222-4222-8222-222222222222"
    services = build_services(WorkspaceContext.from_environment())
    current = read_policy(services)
    draft = "# Workspace policy\n\n## Refund Eligibility Policy\n\nCheck eligibility before refunding.\n"
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_ID", "Refund Eligibility Policy")
    services = build_services(WorkspaceContext.from_environment())

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
    monkeypatch, policy_database
):
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_ID", "Refund eligibility")
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
    assert not (context.home() / "POLICY.md").exists()

    seed_policy_document(services.policy_service,
        "## Refund eligibility\napproved: yes\n\nCheck the refund before issuing it.",
        read_policy(services),
    )
    updated = services.policy_service.approved_rules("refund-agent")[0]
    assert updated.id == f"{rule.id}.1"
    assert updated.criteria == "Check the refund before issuing it."
    assert next(
        row for row in services.policy_service.repository.list_all_history() if row.id == rule.id
    ).archived_at is not None


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


def test_stale_write_cannot_change_approved_policy_row(policy_database):
    context = WorkspaceContext.from_environment()
    repository = build_services(context).policy_service.repository
    pending = repository.write_policy("workspace", "", "Refund review", "Review first.")
    approved = repository.approve_policy(pending.id)

    with repository._connect() as connection, connection.transaction():
        stale_write = connection.execute(_sql("write_policy.sql"), {
            "id": approved.id,
            "scope": approved.scope,
            "target_agent_name": approved.target_agent_name or "",
            "name": approved.name,
            "criteria": "Changed after approval.",
        }).fetchone()

    assert stale_write is None
    current = next(row for row in repository.list_all_history() if row.id == approved.id)
    assert current.approved is True
    assert current.criteria == "Review first."


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


def test_policy_listing_explains_agent_specific_scope(monkeypatch, policy_database):
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_ID", "Refund eligibility")
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


def test_policy_listing_ignores_on_disk_markdown_files(tmp_path, policy_database):
    context = WorkspaceContext.from_environment()
    agent_name = "disk-refund-agent"
    agent_hash = hashlib.sha256(agent_name.encode("utf-8")).hexdigest()
    disk_path = context.home() / "agents" / agent_hash / "POLICY.md"
    disk_path.parent.mkdir(parents=True)
    disk_path.write_text(
        "## Refund exception\napproved: no\n\nEscalate before refunding.",
        encoding="utf-8",
    )
    services = build_services(context)

    listing = list_policies(services)

    assert listing == "No policies exist yet."


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


def test_policy_reads_do_not_import_on_disk_markdown(tmp_path, policy_database):
    context = WorkspaceContext.from_environment()
    disk_path = context.home() / "POLICY.md"
    disk_path.parent.mkdir(parents=True)
    disk_path.write_text(
        "## Refund eligibility\napproved: yes\n\nCheck eligibility.", encoding="utf-8"
    )
    services = build_services(context)

    assert read_policy(services) == "No policy exists yet."
    assert services.policy_service.approved_rules("refund-agent") == []


def test_policy_tools_redact_secrets_before_persisting_markdown(monkeypatch, policy_database):

    services = build_services(WorkspaceContext.from_environment())
    current = read_policy(services)
    draft = '# Workspace policy\n\n## Support credential\n\nKeep credential data private. api_key="policy-secret-value"\n'
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_ID", "Support credential")
    services = build_services(WorkspaceContext.from_environment())

    seed_policy_document(services.policy_service, draft, current)
    saved = read_policy(services)

    assert "policy-secret-value" not in saved
    assert "[REDACTED]" in saved


def test_policy_update_rejects_empty_rule_markdown(policy_database):
    with pytest.raises(ValueError, match="readable Markdown"):
        services = build_services(WorkspaceContext.from_environment())
        seed_policy_document(services.policy_service,
            "# Workspace policy\n\n## Inferred rule\n\n",
            "",
        )
