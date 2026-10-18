"""Shared Lisa test infrastructure: disposable Postgres and production contexts."""

from __future__ import annotations

import subprocess
import json
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from lisa.context import WorkspaceContext
from sql_cases import provision_lisa_schema

WORKSPACE_ID = "11111111-1111-4111-8111-111111111111"
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
TEST_STACK = REPOSITORY_ROOT / "scripts" / "test_stack.py"
TEST_COMPOSE = REPOSITORY_ROOT / "tests" / "compose.test.yaml"


@dataclass(frozen=True)
class LisaTestDatabase:
    provision_url: str
    writer_url: str
    reader_url: str


def _maintenance_url(database_url: str) -> str:
    parsed = urlparse(database_url)
    return urlunparse(parsed._replace(path="/postgres"))


def _runtime_url(base, database: str, role: str, password: str) -> str:
    host = f"[{base.hostname}]" if base.hostname and ":" in base.hostname else base.hostname
    authority = f"{quote(role, safe='')}:{quote(password, safe='')}@{host}"
    if base.port:
        authority += f":{base.port}"
    return urlunparse(base._replace(netloc=authority, path=f"/{database}"))


@pytest.fixture(scope="session")
def postgres_test_server() -> Iterator[str]:
    """Own one test-stack PostgreSQL service for the Lisa test run."""
    started = subprocess.run(
        [
            sys.executable,
            str(TEST_STACK),
            "start-compose",
            "--suite",
            "lisa-pytest",
            "--compose-file",
            str(TEST_COMPOSE),
            "--service",
            "lisa-postgres",
            "--published-port",
            "lisa-postgres:5432",
            "--wait",
        ],
        cwd=REPOSITORY_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    state = json.loads(started.stdout)
    port = state["ports"]["lisa-postgres:5432"][0].rsplit(":", 1)[1]
    server_url = f"postgresql://lisa_test:lisa_test@127.0.0.1:{port}/postgres"
    try:
        with psycopg.connect(server_url):
            pass
        yield server_url
    finally:
        subprocess.run(
            [
                sys.executable,
                str(TEST_STACK),
                "stop",
                "--state-file",
                state["state_file"],
            ],
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )


@pytest.fixture
def lisa_test_database(
    postgres_test_server: str,
) -> Callable[[str], LisaTestDatabase]:
    """Create a separately provisioned Lisa database for each test workspace."""
    base = urlparse(postgres_test_server)
    maintenance_url = _maintenance_url(postgres_test_server)

    def create(workspace_id: str = WORKSPACE_ID) -> LisaTestDatabase:
        name = f"lisa_test_{uuid4().hex}"
        writer_role = f"{name}_writer"
        reader_role = f"{name}_reader"
        writer_password = uuid4().hex
        reader_password = uuid4().hex
        with psycopg.connect(maintenance_url, autocommit=True) as connection:
            statement = (Path(__file__).parent / "sql" / "create_database.sql").read_text()
            connection.execute(sql.SQL(statement).format(database=sql.Identifier(name)))
            role_template = (Path(__file__).parent / "sql" / "create_role.sql").read_text()
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

    return create


@pytest.fixture
def lisa_test_db(lisa_test_database) -> LisaTestDatabase:
    return lisa_test_database()


@pytest.fixture
def lisa_test_context(lisa_test_database, tmp_path) -> Callable[..., WorkspaceContext]:
    """Build production WorkspaceContext values backed by isolated Lisa databases."""

    def create(workspace: str = WORKSPACE_ID, *, data_dir: Path | None = None, **values) -> WorkspaceContext:
        database = lisa_test_database(workspace)
        return WorkspaceContext(
            workspace=workspace,
            data_dir=data_dir or tmp_path / uuid4().hex,
            database_url=database.writer_url,
            database_read_url=database.reader_url,
            **values,
        )

    return create
