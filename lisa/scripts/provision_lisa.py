"""One-shot Lisa Postgres setup: optional DB/roles, then the operational schema.

Not part of the agent runtime. Give Lisa only writer/reader URLs afterward.

When ``LISA_DATABASE_WRITER_PASSWORD`` and ``LISA_DATABASE_READER_PASSWORD`` are
set, this also creates the ``lisa`` database and those roles (dogfood /
local bootstrap). Schema shape changes require ``LISA_DATABASE_RECREATE=1``.
"""

from __future__ import annotations

import os
from hashlib import sha256
from importlib.resources import files
from urllib.parse import urlparse, urlunparse
from uuid import UUID

import psycopg
from psycopg import ClientCursor
from psycopg import sql as pg_sql
from psycopg.rows import dict_row


def _sql(name: str) -> str:
    return files("lisa").joinpath("sql", "activities", name).read_text(encoding="utf-8")


def _workspace_fingerprint(workspace_id: str) -> str:
    return sha256(str(UUID(workspace_id)).encode("utf-8")).hexdigest()


def _admin_dsn(provision_dsn: str) -> str:
    """Connect to the maintenance DB so CREATE DATABASE is allowed."""
    parts = urlparse(provision_dsn)
    return urlunparse(parts._replace(path="/postgres"))


def bootstrap_database(
    provision_dsn: str,
    *,
    writer_password: str,
    reader_password: str,
    writer_role: str = "lisa_writer",
    reader_role: str = "lisa_reader",
) -> None:
    with psycopg.connect(
        _admin_dsn(provision_dsn),
        autocommit=True,
        row_factory=dict_row,
        cursor_factory=ClientCursor,
    ) as connection:
        for role, password in ((writer_role, writer_password), (reader_role, reader_password)):
            exists = connection.execute(
                _sql("role_exists.sql"), {"role": role}
            ).fetchone()
            statement = (
                "ALTER ROLE {} WITH LOGIN PASSWORD {}"
                if exists
                else "CREATE ROLE {} LOGIN PASSWORD {}"
            )
            connection.execute(
                pg_sql.SQL(statement).format(
                    pg_sql.Identifier(role),
                    pg_sql.Literal(password),
                )
            )
        if not connection.execute(_sql("database_exists.sql")).fetchone():
            connection.execute(_sql("create_lisa_database.sql"))
        connection.execute(
            pg_sql.SQL(_sql("grant_lisa_connect.sql")).format(
                writer=pg_sql.Identifier(writer_role),
                reader=pg_sql.Identifier(reader_role),
            )
        )


def _schema_exists(connection: psycopg.Connection) -> bool:
    return connection.execute(_sql("schema_exists.sql")).fetchone() is not None


def _bound_fingerprint(connection: psycopg.Connection) -> str | None:
    if not _schema_exists(connection):
        return None
    try:
        row = connection.execute(_sql("get_deployment.sql")).fetchone()
    except psycopg.Error:
        return None
    return None if row is None else row["workspace_fingerprint"]


def _apply_schema(
    connection: psycopg.Connection,
    fingerprint: str,
    *,
    writer_role: str,
    reader_role: str,
) -> None:
    connection.execute(_sql("schema.sql"))
    grants = pg_sql.SQL(_sql("grant_runtime_roles.sql")).format(
        writer=pg_sql.Identifier(writer_role),
        reader=pg_sql.Identifier(reader_role),
    )
    connection.execute(grants)
    connection.execute(
        _sql("bind_deployment.sql"),
        {"workspace_fingerprint": fingerprint},
    )
    binding = connection.execute(_sql("get_deployment.sql")).fetchone()
    if not binding or binding["workspace_fingerprint"] != fingerprint:
        raise SystemExit("Lisa database is already bound to a different workspace")


def provision(
    dsn: str,
    workspace_id: str,
    *,
    writer_role: str = "lisa_writer",
    reader_role: str = "lisa_reader",
    recreate: bool = False,
) -> None:
    fingerprint = _workspace_fingerprint(workspace_id)
    with psycopg.connect(
        dsn, row_factory=dict_row, cursor_factory=ClientCursor
    ) as connection, connection.transaction():
        existing = _bound_fingerprint(connection)
        if recreate:
            connection.execute(_sql("drop_schema.sql"))
            _apply_schema(
                connection, fingerprint, writer_role=writer_role, reader_role=reader_role
            )
            return
        if existing == fingerprint:
            return
        if _schema_exists(connection):
            raise SystemExit(
                "Lisa schema already exists with a different shape or workspace binding. "
                "Set LISA_DATABASE_RECREATE=1 to DROP SCHEMA lisa CASCADE and provision again."
            )
        _apply_schema(
            connection, fingerprint, writer_role=writer_role, reader_role=reader_role
        )


def main() -> None:
    dsn = os.environ.get("LISA_DATABASE_PROVISION_URL", "").strip()
    workspace_id = os.environ.get("LISA_WORKSPACE_ID", "").strip()
    if not dsn:
        raise SystemExit("LISA_DATABASE_PROVISION_URL is required")
    if not workspace_id:
        raise SystemExit("LISA_WORKSPACE_ID is required")
    writer_role = os.environ.get("LISA_DATABASE_WRITER_ROLE", "lisa_writer").strip()
    reader_role = os.environ.get("LISA_DATABASE_READER_ROLE", "lisa_reader").strip()
    writer_password = os.environ.get("LISA_DATABASE_WRITER_PASSWORD", "").strip()
    reader_password = os.environ.get("LISA_DATABASE_READER_PASSWORD", "").strip()
    if writer_password or reader_password:
        if not writer_password or not reader_password:
            raise SystemExit(
                "Set both LISA_DATABASE_WRITER_PASSWORD and LISA_DATABASE_READER_PASSWORD "
                "to bootstrap the lisa database and roles"
            )
        bootstrap_database(
            dsn,
            writer_password=writer_password,
            reader_password=reader_password,
            writer_role=writer_role,
            reader_role=reader_role,
        )
    recreate = os.environ.get("LISA_DATABASE_RECREATE", "").strip() in {"1", "true", "yes"}
    provision(
        dsn,
        workspace_id,
        writer_role=writer_role,
        reader_role=reader_role,
        recreate=recreate,
    )


if __name__ == "__main__":
    main()
