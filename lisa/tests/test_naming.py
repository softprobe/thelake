from __future__ import annotations

import re
import tomllib
from pathlib import Path


PROJECT = Path(__file__).parents[1]
REPOSITORY = PROJECT.parent


def test_lisa_has_one_name_for_project_package_and_runtime():
    metadata = tomllib.loads((PROJECT / "pyproject.toml").read_text(encoding="utf-8"))
    dockerfile = (PROJECT / "Dockerfile").read_text(encoding="utf-8")
    backend = (PROJECT / "src" / "lisa" / "backends" / "openhands.py").read_text(
        encoding="utf-8"
    )

    assert PROJECT.name == "lisa"
    assert metadata["project"]["name"] == "lisa"
    assert (PROJECT / "src" / "lisa" / "app.py").is_file()
    assert '"lisa.app:app"' in dockerfile
    assert '"lisa.mcp_app"' in backend


def test_compose_uses_the_lisa_service_name():
    root_compose = (REPOSITORY / "docker-compose.yml").read_text(encoding="utf-8")
    dogfood_compose = (REPOSITORY / "examples" / "lisa-dogfood" / "compose.yaml").read_text(
        encoding="utf-8"
    )

    assert "  lisa:" in root_compose
    assert "  lisa:" in dogfood_compose


def test_documentation_pages_use_lowercase_kebab_case():
    roots = (
        REPOSITORY / "docs",
        REPOSITORY / "tests" / "compat" / "grafana" / "browser",
    )
    pages = [page for root in roots for page in root.rglob("*.md")]
    exceptions = {"README.md", "POLICY.md", "SKILL.md", "AGENTS.md", "CLAUDE.md"}
    pattern = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*\.md")

    invalid = [
        page.relative_to(REPOSITORY).as_posix()
        for page in pages
        if "node_modules" not in page.parts
        if page.name not in exceptions and not pattern.fullmatch(page.name)
    ]

    assert invalid == []


def test_playwright_files_use_kebab_case_and_typescript_modules_use_camel_case():
    browser_tests = REPOSITORY / "tests" / "compat" / "grafana" / "browser"
    expected = {
        "e2e-all-query-features.spec.ts",
        "e2e-explorer-ui.spec.ts",
        "e2e-grafana-dashboards.spec.ts",
        "e2e-grafana-explore.spec.ts",
        "e2e-ingestion.spec.ts",
        "queryFeatures.ts",
    }

    assert {path.name for path in browser_tests.iterdir() if path.is_file()} >= expected


def test_lisa_python_modules_and_packages_use_snake_case():
    source = PROJECT / "src" / "lisa"
    modules = [*source.rglob("*.py"), *(PROJECT / "tests").rglob("*.py")]
    module_pattern = re.compile(r"[a-z][a-z0-9_]*\.py")
    package_pattern = re.compile(r"[a-z][a-z0-9_]*")

    invalid_modules = [
        path.relative_to(PROJECT).as_posix()
        for path in modules
        if path.name != "__init__.py" and not module_pattern.fullmatch(path.name)
    ]
    invalid_packages = [
        path.relative_to(source).as_posix()
        for path in source.rglob("*")
        if path.is_dir()
        and "__pycache__" not in path.parts
        and not package_pattern.fullmatch(path.name)
    ]

    assert invalid_modules == []
    assert invalid_packages == []
