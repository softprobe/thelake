from __future__ import annotations

import ast
from pathlib import Path

PACKAGE = Path(__file__).parents[1] / "src" / "lisa"


def _imported_modules(path: Path) -> list[str]:
    relative = path.relative_to(PACKAGE).with_suffix("")
    package_parts = ["lisa", *relative.parts[:-1]]
    if relative.name == "__init__":
        package_parts = ["lisa", *relative.parts[:-1]]
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level:
                base = package_parts[: len(package_parts) - node.level + 1]
                suffix = (node.module or "").split(".") if node.module else []
                modules.append(".".join([*base, *suffix]))
            elif node.module:
                modules.append(node.module)
        elif isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
    return modules


def test_core_lisa_components_have_mvc_owners():
    expected_modules = {
        "controllers/http.py",
        "controllers/mcp.py",
        "models/contracts.py",
        "models/work_items.py",
        "repositories/conversations.py",
        "repositories/lisa_database.py",
        "repositories/work_items.py",
        "repositories/failures.py",
        "repositories/notifications.py",
        "models/policies.py",
        "repositories/policies.py",
        "repositories/slack_events.py",
        "services/conversations.py",
        "services/policies.py",
        "services/evaluations.py",
        "services/slack_events.py",
        "views/mcp.py",
        "views/conversations.py",
        "views/policies.py",
        "views/tools.py",
    }

    assert {
        path.relative_to(PACKAGE).as_posix() for path in PACKAGE.rglob("*.py")
    } >= expected_modules


def test_root_package_has_no_obsolete_mvc_facades():
    obsolete_facades = {
        "api.py",
        "mcp_tools.py",
        "notifications.py",
        "policies.py",
        "policy_repository.py",
        "presentation.py",
        "evaluation/service.py",
        "evaluation/failures.py",
        "controllers/policies.py",
        "work_items/__init__.py",
        "work_items/query.py",
        "work_items/repository.py",
    }

    present = {
        path.relative_to(PACKAGE).as_posix()
        for path in PACKAGE.rglob("*.py")
        if path.relative_to(PACKAGE).as_posix() in obsolete_facades
    }

    assert not present


def test_work_item_models_and_persistence_have_distinct_mvc_owners():
    assert (PACKAGE / "models" / "work_items.py").is_file()
    assert (PACKAGE / "repositories" / "work_items.py").is_file()
    assert (PACKAGE / "repositories" / "lisa_database.py").is_file()
    assert not list((PACKAGE / "work_items").glob("*.py"))


def test_controllers_delegate_use_cases_and_app_does_not_reach_into_controller_internals():
    controller_sources = "\n".join(
        path.read_text(encoding="utf-8") for path in (PACKAGE / "controllers").glob("*.py")
    )
    app_source = (PACKAGE / "app.py").read_text(encoding="utf-8")

    assert "services.client.post(" not in controller_sources
    assert "services.database_reader.query(" not in controller_sources
    assert "_slack_event_loop" not in app_source
    assert "_slack_tasks" not in app_source


def test_root_app_assembles_fastapi_while_http_controller_defines_routes():
    app_path = PACKAGE / "app.py"
    controller_path = PACKAGE / "controllers" / "http.py"
    app_tree = ast.parse(app_path.read_text(encoding="utf-8"))
    controller_tree = ast.parse(controller_path.read_text(encoding="utf-8"))

    def called_names(tree: ast.AST) -> set[str]:
        return {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        } | {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }

    assert "FastAPI" in called_names(app_tree)
    assert "FastAPI" not in called_names(controller_tree)
    assert "APIRouter" in called_names(controller_tree)
    assert "include_router" in called_names(app_tree)
    assert any(
        isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        and node.decorator_list
        and any(isinstance(decorator, ast.Call) for decorator in node.decorator_list)
        for node in controller_tree.body
    )


def test_models_do_not_depend_on_transport_or_presentation_layers():
    for path in (PACKAGE / "models").rglob("*.py"):
        imports = _imported_modules(path)
        assert not any(
            name.startswith(("lisa.controllers", "lisa.views")) for name in imports
        ), f"Model module {path} must not depend on controllers or views"


def test_conversation_storage_and_http_contracts_have_mvc_owners():
    repository = ast.parse(
        (PACKAGE / "repositories" / "conversations.py").read_text(encoding="utf-8")
    )
    file_repository = next(
        node
        for node in repository.body
        if isinstance(node, ast.ClassDef) and node.name == "FileConversationRepository"
    )
    repository_functions = {
        node.name for node in file_repository.body if isinstance(node, ast.FunctionDef)
    }
    assert {
        "workspace_directory",
        "conversation_directory",
        "load_metadata",
        "save_metadata",
    } <= repository_functions

    controller = ast.parse((PACKAGE / "controllers" / "http.py").read_text(encoding="utf-8"))
    controller_models = {
        node.name
        for node in controller.body
        if isinstance(node, ast.ClassDef)
        and any(isinstance(base, ast.Name) and base.id == "BaseModel" for base in node.bases)
    }
    assert not controller_models, "HTTP request contracts belong in the models layer"


def test_mvc_layers_do_not_import_inward_from_lower_layers():
    forbidden = {
        "repositories": ("lisa.controllers", "lisa.services", "lisa.views"),
        "views": ("lisa.controllers", "lisa.services", "lisa.repositories"),
        "services": ("lisa.controllers", "lisa.views"),
    }
    for layer, prefixes in forbidden.items():
        for path in (PACKAGE / layer).rglob("*.py"):
            imported_modules = _imported_modules(path)
            assert not any(
                module.startswith(prefix) for module in imported_modules for prefix in prefixes
            ), f"{layer.title()} module {path} crosses an inward MVC dependency"


def test_services_receive_repositories_and_other_services_from_composition_root():
    sources = "\n".join(
        path.read_text(encoding="utf-8") for path in (PACKAGE / "services").glob("*.py")
    )

    # Constructors belong in bootstrap, where the runtime's concrete choices
    # are visible together. A use case may call an injected collaborator, but
    # should not quietly choose its own persistence or another service.
    for constructor in (
        "FailureRepository(",
        "NotificationReceiptRepository(",
        "PostgresPolicyRepository(",
        "InMemoryPolicyRepository(",
        "SessionService(",
        "ConversationService(",
    ):
        assert constructor not in sources, f"Service layer constructs {constructor[:-1]}"


def test_http_controller_contains_endpoints_without_constructing_services_or_storage():
    source = (PACKAGE / "controllers" / "http.py").read_text(encoding="utf-8")
    assert "ConversationService(" not in source
    assert "FileConversationRepository(" not in source
    assert "_slack_event_path" not in source


def test_fastapi_composes_services_even_before_workspace_is_configured():
    source = (PACKAGE / "app.py").read_text(encoding="utf-8")
    assert "services = build_services(WorkspaceContext.from_environment())" in source
    assert "if workspace_id:\n        services = build_services" not in source
