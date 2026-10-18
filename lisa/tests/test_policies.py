import hashlib

import pytest

from lisa.bootstrap import build_services
from lisa.context import WorkspaceContext
from lisa.repositories.policies import InMemoryPolicyRepository
from lisa.services.policies import PolicyService
from policy_cases import list_policies, read_policy
from lisa.views.policies import render_policy_list, render_policy_records


@pytest.fixture(autouse=True)
def isolated_data_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    monkeypatch.setenv("LISA_TURN_SOURCE", "web")
    monkeypatch.delenv("LISA_CONFIRMED_POLICY_TITLE", raising=False)


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
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund Eligibility Policy")
    services = build_services(WorkspaceContext.from_environment())

    services.policy_service.update_policy(draft, current)

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
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund eligibility")
    context = WorkspaceContext.from_environment()
    services = build_services(context)
    services.policy_service.update_policy(
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

    services.policy_service.update_policy(
        "## Refund eligibility\napproved: yes\n\nCheck the refund before issuing it.",
        read_policy(services),
    )
    updated = services.policy_service.approved_rules("refund-agent")[0]
    assert updated.id == rule.id
    assert updated.criteria == "Check the refund before issuing it."


def test_policy_ids_use_paragraph_style_ordinals(policy_database):
    context = WorkspaceContext.from_environment()
    repository = build_services(context).policy_service.repository
    rules = [
        {"name": f"Rule {number}", "criteria": f"Rule {number}.", "approved": True}
        for number in range(1, 11)
    ]

    saved = repository.replace_scope(
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


def test_policy_listing_explains_agent_specific_scope(monkeypatch, policy_database):
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund eligibility")
    context = WorkspaceContext.from_environment()
    services = build_services(context)
    services.policy_service.update_policy(
        "## Refund eligibility\napproved: no\n\nDo not issue the refund.",
        "No policy exists yet.",
        scope="agent",
        agent_name="quickstart-refund-agent",
    )

    listing = list_policies(services)[0]

    assert "Refund eligibility" in listing
    assert "P1.1 | Refund eligibility" in listing
    assert "Applies to: quickstart-refund-agent only" in listing
    assert "approved: no" in listing
    assert "pol_" not in listing


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
    repository.replace_scope(
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


def test_policy_update_requires_owner_confirmation_to_add_rule(monkeypatch, policy_database):

    draft = "# Workspace policy\n\n## Refund eligibility\n\nCheck eligibility before refunding.\n"

    with pytest.raises(ValueError, match="explicit confirmation"):
        services = build_services(WorkspaceContext.from_environment())
        services.policy_service.update_policy(draft, "")

    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund eligibility")
    services = build_services(WorkspaceContext.from_environment())
    services.policy_service.update_policy(draft, "")


def test_policy_update_requires_confirmation_to_change_a_confirmed_rule(
    monkeypatch, policy_database
):
    current = "# Workspace policy\n\n## Refund eligibility\n\nCheck eligibility before refunding.\n"
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund eligibility")
    services = build_services(WorkspaceContext.from_environment())
    services.policy_service.update_policy(current, "")

    current = read_policy(services)
    changed = current.replace(
        "Check eligibility before refunding", "Check the ticket before refunding"
    )
    monkeypatch.delenv("LISA_CONFIRMED_POLICY_TITLE")
    services = build_services(WorkspaceContext.from_environment())
    with pytest.raises(ValueError, match="explicit confirmation"):
        services.policy_service.update_policy(changed, current)

    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund eligibility")
    services = build_services(WorkspaceContext.from_environment())
    services.policy_service.update_policy(changed, current)


def test_policy_tools_redact_secrets_before_persisting_markdown(monkeypatch, policy_database):

    services = build_services(WorkspaceContext.from_environment())
    current = read_policy(services)
    draft = '# Workspace policy\n\n## Support credential\n\nKeep credential data private. api_key="policy-secret-value"\n'
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Support credential")
    services = build_services(WorkspaceContext.from_environment())

    services.policy_service.update_policy(draft, current)
    saved = read_policy(services)

    assert "policy-secret-value" not in saved
    assert "[REDACTED]" in saved


def test_policy_update_rejects_empty_rule_markdown(policy_database):
    with pytest.raises(ValueError, match="readable Markdown"):
        services = build_services(WorkspaceContext.from_environment())
        services.policy_service.update_policy(
            "# Workspace policy\n\n## Inferred rule\n\n",
            "",
        )
