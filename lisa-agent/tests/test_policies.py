import pytest

from thelake_lisa.context import WorkspaceContext
from thelake_lisa.services import build_services


@pytest.fixture(autouse=True)
def isolated_data_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))


def test_policy_tools_scope_files_per_workspace(tmp_path, monkeypatch):

    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    first = "11111111-1111-4111-8111-111111111111"
    second = "22222222-2222-4222-8222-222222222222"
    monkeypatch.setenv("LISA_WORKSPACE_ID", first)
    services = build_services(WorkspaceContext.from_environment())
    current = services.policies.read_policy()
    draft = "# Workspace policy\n\n## Refund Eligibility Policy\n\nCheck eligibility before refunding.\n"
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund Eligibility Policy")
    services = build_services(WorkspaceContext.from_environment())

    services.policies.update_policy(draft, current)

    assert services.policies.read_policy() == draft
    monkeypatch.setenv("LISA_WORKSPACE_ID", second)
    services = build_services(WorkspaceContext.from_environment())
    assert services.policies.read_policy() == "No policy exists yet."


def test_policy_update_requires_owner_confirmation_to_add_rule(tmp_path, monkeypatch):

    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    current = "# Workspace policy\n\n## Refund eligibility\n\nCheck eligibility before refunding.\n"

    with pytest.raises(ValueError, match="explicit confirmation"):
        services = build_services(WorkspaceContext.from_environment())
        services.policies.update_policy(current, "")

    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund eligibility")
    services = build_services(WorkspaceContext.from_environment())
    services.policies.update_policy(current, "")


def test_policy_update_requires_confirmation_to_change_a_confirmed_rule(tmp_path, monkeypatch):
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    current = "# Workspace policy\n\n## Refund eligibility\n\nCheck eligibility before refunding.\n"
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund eligibility")
    services = build_services(WorkspaceContext.from_environment())
    services.policies.update_policy(current, "")

    changed = current.replace(
        "Check eligibility before refunding", "Check the ticket before refunding"
    )
    monkeypatch.delenv("LISA_CONFIRMED_POLICY_TITLE")
    services = build_services(WorkspaceContext.from_environment())
    with pytest.raises(ValueError, match="explicit confirmation"):
        services.policies.update_policy(changed, current)

    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Refund eligibility")
    services = build_services(WorkspaceContext.from_environment())
    services.policies.update_policy(changed, current)


def test_policy_tools_redact_secrets_before_persisting_markdown(tmp_path, monkeypatch):

    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    services = build_services(WorkspaceContext.from_environment())
    current = services.policies.read_policy()
    draft = '# Workspace policy\n\n## Support credential\n\nKeep credential data private. api_key="policy-secret-value"\n'
    monkeypatch.setenv("LISA_CONFIRMED_POLICY_TITLE", "Support credential")
    services = build_services(WorkspaceContext.from_environment())

    services.policies.update_policy(draft, current)
    saved = services.policies.read_policy()

    assert "policy-secret-value" not in saved
    assert "[REDACTED]" in saved


def test_policy_update_rejects_empty_rule_markdown(tmp_path, monkeypatch):
    monkeypatch.setenv("LISA_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("LISA_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111")
    with pytest.raises(ValueError, match="readable Markdown"):
        services = build_services(WorkspaceContext.from_environment())
        services.policies.update_policy(
            "# Workspace policy\n\n## Inferred rule\n\n",
            "",
        )


@pytest.mark.parametrize("target", ["POLICY.md", "POLICY.lock"])
def test_policy_storage_rejects_symlink_files_and_locks(tmp_path, target):
    context = WorkspaceContext(
        workspace="11111111-1111-4111-8111-111111111111",
        data_dir=tmp_path,
        confirmed_policy_title="Rule",
    )
    services = build_services(context)
    home = context.home()
    home.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.write_text("Do not change")
    (home / target).symlink_to(outside)
    with pytest.raises(ValueError, match="symlink"):
        services.policies.update_policy("## Rule\n\nConfirmed rule", "")
    assert outside.read_text() == "Do not change"
