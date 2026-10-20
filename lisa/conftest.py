"""Sole owner of Lisa disposable test and evaluation environment lifecycle.

Pytest fixtures, activity benchmarks, and policy-evaluation harnesses must use
these helpers. Do not start/stop Compose, create Lisa databases/roles, provision
schemas, or bind/dispose dogfood stacks anywhere else.
"""

from __future__ import annotations

import atexit
import json
import os
import shutil
import subprocess
import sys
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse
from uuid import uuid4

import psycopg
from psycopg import sql

from lisa.context import WorkspaceContext

try:
    import pytest
except ImportError:  # Live evaluation runners import helpers without the test extra.
    pytest = None

WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
TEST_STACK = REPOSITORY_ROOT / "scripts" / "test_stack.py"
TEST_COMPOSE = REPOSITORY_ROOT / "tests" / "compose.test.yaml"
_PROVISION_SCRIPT = Path(__file__).resolve().parent / "scripts" / "provision_lisa.py"
_SQL_DIR = Path(__file__).resolve().parent / "tests" / "sql"

_ACTIVE_POSTGRES_LOCK = threading.Lock()
_EXIT_HOOKS_INSTALLED = False


@dataclass(frozen=True)
class LisaTestDatabase:
    provision_url: str
    writer_url: str
    reader_url: str


@dataclass(frozen=True)
class LisaPostgresStack:
    server_url: str
    state_file: Path


@dataclass(frozen=True)
class LisaDogfoodTarget:
    project_name: str
    instance_id: str
    lake_data: Path
    env_file: Path


_ACTIVE_POSTGRES: dict[str, LisaPostgresStack] = {}


def provision_lisa_schema(
    dsn: str,
    workspace_id: str,
    *,
    writer_role: str = "lisa_writer",
    reader_role: str = "lisa_reader",
) -> None:
    """Recreate the Lisa schema via the one-shot deploy script."""
    spec = spec_from_file_location("lisa_provision", _PROVISION_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load Lisa's database provisioner")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    module.provision(
        dsn,
        workspace_id,
        writer_role=writer_role,
        reader_role=reader_role,
        recreate=True,
    )


def start_lisa_postgres(
    *,
    suite: str,
    artifact_dir: Path | None = None,
) -> LisaPostgresStack:
    """Start the shared transient ``lisa-postgres`` Compose service.

    The stack is tracked until ``stop_lisa_postgres`` runs. Process exit and
    pytest session finish also stop any still-active stacks so interrupted runs
    do not leave orphan containers.
    """
    command = [
        sys.executable,
        str(TEST_STACK),
        "start-compose",
        "--suite",
        suite,
        "--compose-file",
        str(TEST_COMPOSE),
        "--service",
        "lisa-postgres",
        "--published-port",
        "lisa-postgres:5432",
        "--wait",
    ]
    if artifact_dir is not None:
        command.extend(["--artifact-dir", str(artifact_dir)])
    started = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    state = json.loads(started.stdout)
    port = state["ports"]["lisa-postgres:5432"][0].rsplit(":", 1)[1]
    server_url = f"postgresql://lisa_test:lisa_test@127.0.0.1:{port}/postgres"
    stack = LisaPostgresStack(server_url=server_url, state_file=Path(state["state_file"]))
    _track_postgres_stack(stack)
    try:
        with psycopg.connect(server_url):
            pass
    except Exception:
        stop_lisa_postgres(stack)
        raise
    return stack


def stop_lisa_postgres(stack: LisaPostgresStack) -> None:
    """Stop the run-owned ``lisa-postgres`` stack recorded in ``stack.state_file``.

    The stack stays tracked until stop succeeds so process/session exit hooks can
    retry a failed teardown instead of leaving an orphan container.
    """
    subprocess.run(
        [
            sys.executable,
            str(TEST_STACK),
            "stop",
            "--state-file",
            str(stack.state_file),
        ],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    _untrack_postgres_stack(stack)


def stop_all_lisa_postgres() -> None:
    """Stop every still-tracked Lisa Postgres stack (process/session exit path).

    Soft stop failures are collected so every stack is attempted; the first
    failure is re-raised afterward. ``KeyboardInterrupt`` / ``SystemExit`` are
    not swallowed.
    """
    with _ACTIVE_POSTGRES_LOCK:
        stacks = list(_ACTIVE_POSTGRES.values())
    errors: list[Exception] = []
    for stack in stacks:
        try:
            stop_lisa_postgres(stack)
        except Exception as error:
            errors.append(error)
    if errors:
        raise errors[0]


def _track_postgres_stack(stack: LisaPostgresStack) -> None:
    with _ACTIVE_POSTGRES_LOCK:
        _ACTIVE_POSTGRES[str(stack.state_file)] = stack
    _ensure_exit_hooks()


def _untrack_postgres_stack(stack: LisaPostgresStack) -> None:
    with _ACTIVE_POSTGRES_LOCK:
        _ACTIVE_POSTGRES.pop(str(stack.state_file), None)


def _ensure_exit_hooks() -> None:
    global _EXIT_HOOKS_INSTALLED
    if _EXIT_HOOKS_INSTALLED:
        return
    _EXIT_HOOKS_INSTALLED = True
    atexit.register(stop_all_lisa_postgres)


def create_lisa_test_database(
    server_url: str,
    workspace_id: str = WORKSPACE_ID,
) -> LisaTestDatabase:
    """Create an isolated Lisa database with writer/reader roles and schema."""
    base = urlparse(server_url)
    maintenance_url = _maintenance_url(server_url)
    name = f"lisa_test_{uuid4().hex}"
    writer_role = f"{name}_writer"
    reader_role = f"{name}_reader"
    writer_password = uuid4().hex
    reader_password = uuid4().hex
    with psycopg.connect(maintenance_url, autocommit=True) as connection:
        statement = (_SQL_DIR / "create_database.sql").read_text(encoding="utf-8")
        connection.execute(sql.SQL(statement).format(database=sql.Identifier(name)))
        role_template = (_SQL_DIR / "create_role.sql").read_text(encoding="utf-8")
        for role, password in ((writer_role, writer_password), (reader_role, reader_password)):
            connection.execute(
                sql.SQL(role_template).format(
                    role=sql.Identifier(role), password=sql.Literal(password)
                )
            )
    provision_url = urlunparse(base._replace(path=f"/{name}"))
    provision_lisa_schema(
        provision_url,
        workspace_id,
        writer_role=writer_role,
        reader_role=reader_role,
    )
    return LisaTestDatabase(
        provision_url=provision_url,
        writer_url=_runtime_url(base, name, writer_role, writer_password),
        reader_url=_runtime_url(base, name, reader_role, reader_password),
    )


def lisa_dogfood_target(path: Path, repository_root: Path) -> LisaDogfoodTarget:
    """Validate a run-owned dogfood env file and resolve its disposable data path."""
    repository_root = repository_root.resolve(strict=True)
    warehouse_root = repository_root / "warehouse"
    dogfood_root = warehouse_root / "dogfood"
    if warehouse_root.is_symlink() or dogfood_root.is_symlink():
        raise RuntimeError("Refusing cleanup through a symlinked dogfood directory")
    data_root = dogfood_root.resolve(strict=True)
    if data_root != dogfood_root or repository_root not in data_root.parents:
        raise RuntimeError("Dogfood cleanup data path must remain inside the repository")
    values = _read_environment_file(path)
    project_name = values.get("THELAKE_DOGFOOD_PROJECT_NAME", "")
    instance_id = values.get("THELAKE_DOGFOOD_INSTANCE_ID", "")
    data_path = values.get("THELAKE_DOGFOOD_DATA_PATH", "")
    relative_data_path = Path(data_path)
    raw_lake_data = repository_root / relative_data_path
    lake_data = raw_lake_data.resolve()
    if (
        not project_name.startswith("lisa-policy-eval-")
        or not instance_id
        or path.is_symlink()
        or path.resolve().parent != data_root
        or path.stem != project_name
        or relative_data_path.is_absolute()
        or relative_data_path.parts != ("warehouse", "dogfood", project_name)
        or not relative_data_path.name.startswith("lisa-policy-eval-")
        or lake_data.parent != data_root
        or relative_data_path.name != project_name
    ):
        raise RuntimeError("Cleanup requires a run-owned Lisa policy evaluation stack and data path")
    if raw_lake_data.is_symlink():
        raise RuntimeError("Refusing to remove a symlinked dogfood data directory")
    return LisaDogfoodTarget(project_name, instance_id, lake_data, path)


def configure_lisa_dogfood(
    path: Path,
    data_dir: Path,
    repository_root: Path | None = None,
) -> LisaDogfoodTarget:
    """Bind process env to a local dogfood stack without printing credentials."""
    root = repository_root or REPOSITORY_ROOT
    target = lisa_dogfood_target(path, root)
    values = _read_environment_file(path)
    workspace_id = values.get("THELAKE_DEFAULT_WORKSPACE_ID")
    database_port = values.get("THELAKE_DOGFOOD_DB_PORT")
    api_port = values.get("THELAKE_DOGFOOD_PORT")
    writer_password = values.get("LISA_DATABASE_WRITER_PASSWORD")
    reader_password = values.get("LISA_DATABASE_READER_PASSWORD")
    service_token = values.get("LISA_THELAKE_TOKEN")
    if not all(
        (workspace_id, database_port, api_port, writer_password, reader_password, service_token)
    ):
        raise RuntimeError("Dogfood connection metadata is incomplete; start an isolated stack")
    os.environ["LISA_WORKSPACE_ID"] = workspace_id
    safe_writer_password = quote(writer_password, safe="")
    os.environ["LISA_DATABASE_URL"] = (
        f"postgresql://lisa_writer:{safe_writer_password}@127.0.0.1:{database_port}/lisa"
    )
    safe_reader_password = quote(reader_password, safe="")
    os.environ["LISA_DATABASE_READ_URL"] = (
        f"postgresql://lisa_reader:{safe_reader_password}@127.0.0.1:{database_port}/lisa"
    )
    os.environ["LISA_THELAKE_API_URL"] = f"http://127.0.0.1:{api_port}"
    os.environ["LISA_THELAKE_TOKEN"] = service_token
    os.environ["LISA_DATA_DIR"] = str(data_dir.resolve())
    os.environ["LISA_LLM_TRACE_ENABLED"] = "1"
    os.environ["SOFTPROBE_PUBLIC_KEY"] = "dogfood-local"
    return target


def dispose_lisa_dogfood(
    target: LisaDogfoodTarget,
    repository_root: Path,
    *,
    report_path: Path,
    data_dir: Path,
) -> None:
    """Remove only this evaluation's disposable stack and DuckLake data."""
    for retained_path in (report_path.resolve(), data_dir.resolve()):
        if retained_path == target.lake_data or target.lake_data in retained_path.parents:
            raise RuntimeError(
                "Keep the evaluation report and LLM traces outside disposable lake data"
            )

    for resource, options in (
        ("container", ["ps", "--all", "--quiet"]),
        ("volume", ["volume", "ls", "--quiet"]),
        ("network", ["network", "ls", "--quiet"]),
    ):
        project_filter = f"label=com.docker.compose.project={target.project_name}"
        owner_filter = f"label=io.arex.lisa.dogfood-instance={target.instance_id}"
        project_resources = subprocess.run(
            ["docker", *options, "--filter", project_filter],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        owned_resources = subprocess.run(
            ["docker", *options, "--filter", project_filter, "--filter", owner_filter],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        if set(project_resources) != set(owned_resources):
            raise RuntimeError(f"Refusing to remove {resource}s not owned by this evaluation run")

    command = [
        "docker",
        "compose",
        "--env-file",
        str(repository_root / ".env"),
        "--env-file",
        str(target.env_file.resolve()),
        "--project-name",
        target.project_name,
        "--file",
        str(repository_root / "examples" / "lisa-dogfood" / "compose.yaml"),
        "down",
        "--volumes",
        "--remove-orphans",
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
    except (OSError, subprocess.CalledProcessError) as error:
        raise RuntimeError("Failed to remove the disposable Lisa policy evaluation stack") from error
    if target.lake_data.exists():
        shutil.rmtree(target.lake_data)
    target.env_file.unlink(missing_ok=True)
    for suffix in (".yaml", ".api_url", ".lisa_api_url"):
        metadata_file = repository_root / "warehouse" / "dogfood" / f"{target.project_name}{suffix}"
        if metadata_file.is_symlink():
            raise RuntimeError("Refusing to remove symlinked dogfood metadata")
        metadata_file.unlink(missing_ok=True)


def _maintenance_url(database_url: str) -> str:
    parsed = urlparse(database_url)
    return urlunparse(parsed._replace(path="/postgres"))


def _runtime_url(base, database: str, role: str, password: str) -> str:
    host = f"[{base.hostname}]" if base.hostname and ":" in base.hostname else base.hostname
    authority = f"{quote(role, safe='')}:{quote(password, safe='')}@{host}"
    if base.port:
        authority += f":{base.port}"
    return urlunparse(base._replace(netloc=authority, path=f"/{database}"))


def _read_environment_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key.strip()] = value
    return values


if pytest is not None:

    def pytest_sessionfinish(session, exitstatus) -> None:  # noqa: ARG001
        """Last-resort teardown if a fixture finally did not run."""
        stop_all_lisa_postgres()

    @pytest.fixture(scope="session")
    def postgres_test_server() -> Iterator[str]:
        """Own one test-stack PostgreSQL service for the Lisa test run."""
        stack = start_lisa_postgres(suite="lisa-pytest")
        try:
            yield stack.server_url
        finally:
            stop_lisa_postgres(stack)

    @pytest.fixture
    def lisa_test_database(
        postgres_test_server: str,
    ) -> Callable[[str], LisaTestDatabase]:
        """Create a separately provisioned Lisa database for each test workspace."""

        def create(workspace_id: str = WORKSPACE_ID) -> LisaTestDatabase:
            return create_lisa_test_database(postgres_test_server, workspace_id)

        return create

    @pytest.fixture
    def lisa_test_db(lisa_test_database) -> LisaTestDatabase:
        return lisa_test_database()

    @pytest.fixture
    def lisa_test_context(lisa_test_database, tmp_path) -> Callable[..., WorkspaceContext]:
        """Build production WorkspaceContext values backed by isolated Lisa databases."""

        def create(
            workspace: str = WORKSPACE_ID, *, data_dir: Path | None = None, **values
        ) -> WorkspaceContext:
            database = lisa_test_database(workspace)
            return WorkspaceContext(
                workspace=workspace,
                data_dir=data_dir or tmp_path / uuid4().hex,
                database_url=database.writer_url,
                database_read_url=database.reader_url,
                **values,
            )

        return create
