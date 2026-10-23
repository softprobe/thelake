"""Bounded read-only SQL access to Lisa's approved views and policy table."""

from __future__ import annotations

import json
from hashlib import sha256
from importlib.resources import files
from typing import Any
from uuid import UUID

import psycopg
import sqlglot
from psycopg.rows import dict_row
from sqlglot import exp

from ..models.database import MAX_ROWS, POLICY_COLUMNS, RELATION_COLUMNS

__all__ = ["MAX_ROWS", "POLICY_COLUMNS", "RELATION_COLUMNS"]

MAX_QUERY_LENGTH = 5_000
MAX_RESPONSE_BYTES = 32_000
STATEMENT_TIMEOUT_MS = 1_500

# The view used to bind a reader connection is granted to the reader role but
# deliberately unavailable to user-authored lisa_db queries.
INTERNAL_READER_RELATIONS = {"readable_deployment"}
VIEW_COLUMNS = {name: set(columns) for name, columns in RELATION_COLUMNS.items()}

ALLOWED_FUNCTIONS = {
    "avg",
    "coalesce",
    "count",
    "date_trunc",
    "length",
    "lower",
    "max",
    "min",
    "nullif",
    "sum",
    "upper",
}


def validate_read_query(query: str) -> str:
    """Parse one SELECT, restrict it to approved Lisa relations, and cap rows."""
    if not query.strip() or len(query) > MAX_QUERY_LENGTH:
        raise ValueError(f"Query must contain at most {MAX_QUERY_LENGTH} characters")
    try:
        statements = sqlglot.parse(query, read="postgres")
    except sqlglot.errors.ParseError as error:
        raise ValueError("Query is not valid PostgreSQL SELECT syntax") from error
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise ValueError("Only one SELECT statement is allowed")
    tree = statements[0]
    if any(isinstance(node.expression, exp.Func) for node in tree.find_all(exp.Dot)):
        raise ValueError("Schema-qualified function calls are not allowed")
    if any(
        tree.find(kind) is not None
        for kind in (exp.With, exp.Subquery, exp.Union, exp.Intersect, exp.Except, exp.Lock)
    ):
        raise ValueError(
            "CTEs, nested queries, set operations, and locking clauses are not allowed"
        )
    if tree.args.get("into"):
        raise ValueError("SELECT INTO is not allowed")

    tables = list(tree.find_all(exp.Table))
    if not tables:
        raise ValueError("Query must read an approved Lisa view or table")
    aliases: dict[str, str] = {}
    relation_names: set[str] = set()
    for table in tables:
        name = table.name.lower()
        schema = table.db.lower()
        if table.catalog or schema != "lisa" or name not in VIEW_COLUMNS:
            raise ValueError("Query may read only approved Lisa views or tables")
        relation_names.add(name)
        aliases[(table.alias or table.name).lower()] = name

    available_columns = set().union(*(VIEW_COLUMNS[name] for name in relation_names))
    if tree.find(exp.Star) is not None:
        raise ValueError("Wildcard projections are not allowed; name approved columns explicitly")
    for column in tree.find_all(exp.Column):
        name = column.name.lower()
        if column.table:
            relation = aliases.get(column.table.lower())
            if relation is None or name not in VIEW_COLUMNS[relation]:
                raise ValueError(f"Column {column.sql()} is not an approved Lisa column")
        elif name not in available_columns:
            raise ValueError(f"Column {name} is not an approved Lisa column")

    for function in tree.find_all(exp.Func):
        # sqlglot models boolean predicates such as AND and OR as Func nodes too.
        # They are SQL operators, not callable functions; validating them against
        # the scalar function allowlist rejects ordinary filtered reads.
        if isinstance(function, exp.Condition) and not isinstance(function, exp.Anonymous):
            continue
        name = function.sql_name().lower()
        if isinstance(function, exp.Anonymous):
            name = function.name.lower()
        if name not in ALLOWED_FUNCTIONS:
            raise ValueError(f"Function {name} is not allowed")

    limit = tree.args.get("limit")
    if limit:
        count = _integer_limit(limit.expression, "LIMIT")
        tree.limit(min(count, MAX_ROWS), copy=False)
    else:
        tree.limit(MAX_ROWS, copy=False)
    offset = tree.args.get("offset")
    if offset and _integer_limit(offset.expression, "OFFSET") > 1_000:
        raise ValueError("OFFSET cannot exceed 1000 rows")

    return tree.sql(dialect="postgres")


def _integer_limit(expression: exp.Expression, name: str) -> int:
    if not isinstance(expression, exp.Literal) or expression.is_string:
        raise ValueError(f"{name} must be a non-negative integer literal")
    try:
        value = int(expression.this)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be a non-negative integer literal") from error
    if value < 0:
        raise ValueError(f"{name} must be a non-negative integer literal")
    return value


def _workspace_fingerprint(workspace_id: str) -> str:
    return sha256(str(UUID(workspace_id)).encode("utf-8")).hexdigest()


def _sql(name: str) -> str:
    return files("lisa").joinpath("sql", "activities", name).read_text(encoding="utf-8")


class LisaDatabaseReader:
    """Query approved Lisa Postgres relations through a bounded, read-only role."""

    def __init__(self, dsn: str, workspace_id: str):
        self.dsn = dsn
        self.workspace_fingerprint = _workspace_fingerprint(workspace_id)
        with self._connect() as connection:
            binding = connection.execute(_sql("get_readable_deployment.sql")).fetchone()
        if not binding or binding["workspace_fingerprint"] != self.workspace_fingerprint:
            raise RuntimeError("Lisa read database is not bound to the configured workspace")

    def _connect(self):
        return psycopg.connect(
            self.dsn,
            row_factory=dict_row,
            options=(
                "-c default_transaction_read_only=on "
                f"-c statement_timeout={STATEMENT_TIMEOUT_MS} "
                "-c idle_in_transaction_session_timeout=3000"
            ),
        )

    def query(self, query: str) -> dict[str, Any]:
        statement = validate_read_query(query)
        try:
            with self._connect() as connection:
                cursor = connection.execute(statement)
                columns = [column.name for column in cursor.description]
                rows = []
                response_bytes = 0
                truncated = False
                for row in cursor:
                    item = dict(row)
                    size = len(json.dumps(item, ensure_ascii=False, default=str).encode("utf-8"))
                    if response_bytes + size > MAX_RESPONSE_BYTES:
                        truncated = True
                        break
                    response_bytes += size
                    rows.append(item)
                if len(rows) == MAX_ROWS:
                    truncated = True
                return {
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows),
                    "truncated": truncated,
                }
        except psycopg.Error as error:
            raise ValueError(
                f"Lisa database query failed (SQLSTATE {error.sqlstate or 'unknown'})"
            ) from None
