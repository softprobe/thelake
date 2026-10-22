from __future__ import annotations

import re
from pathlib import Path

import pytest
import sqlglot
from sqlglot import exp
from sql_cases import read_sql_case, read_sql_file

from lisa.repositories.lisa_database import (
    INTERNAL_READER_RELATIONS,
    POLICY_COLUMNS,
    RELATION_COLUMNS,
    VIEW_COLUMNS,
    validate_read_query,
)
from lisa.views.database import LISA_DATABASE_SCHEMA


@pytest.mark.parametrize(
    "case",
    [
        "reject_update",
        "reject_delete",
        "reject_catalog",
        "reject_sleep",
        "reject_qualified_function",
        "reject_quoted_qualified_function",
        "reject_fully_quoted_qualified_function",
        "reject_sequence",
        "reject_cte",
        "reject_column",
        "reject_multiple",
    ],
)
def test_read_query_rejects_writes_hidden_relations_functions_and_columns(case):
    with pytest.raises(ValueError):
        validate_read_query(read_sql_case(case))


def test_read_query_allows_approved_views_and_caps_rows():
    sql = validate_read_query(read_sql_case("allowed_bounded"))

    assert "FROM lisa.readable_activities" in sql
    assert "LIMIT 100" in sql


def test_read_query_allows_the_policy_table_but_not_other_base_tables():
    sql = validate_read_query(read_sql_file("read_policy_columns"))

    assert "FROM lisa.policies" in sql
    assert "LIMIT 100" in sql
    with pytest.raises(ValueError, match="approved Lisa views or tables"):
        validate_read_query(read_sql_file("reject_activity_base_table"))


def test_database_allowlist_matches_every_exposed_view_projection():
    schema_path = Path(__file__).resolve().parents[1] / "src/lisa/sql/activities/schema.sql"
    schema = schema_path.read_text(encoding="utf-8")
    declared_views = {
        statement.this.name: tuple(statement.expression.named_selects)
        for statement in sqlglot.parse(schema, read="postgres")
        if isinstance(statement, exp.Create)
        and statement.args.get("kind") == "VIEW"
        and statement.this.name not in INTERNAL_READER_RELATIONS
    }

    assert declared_views == {
        relation: tuple(columns)
        for relation, columns in RELATION_COLUMNS.items()
        if relation != "policies"
    }


def test_policy_database_allowlist_matches_the_actual_table_columns():
    schema_path = Path(__file__).resolve().parents[1] / "src/lisa/sql/activities/schema.sql"
    schema = schema_path.read_text(encoding="utf-8")
    declaration = next(
        statement
        for statement in sqlglot.parse(schema, read="postgres")
        if isinstance(statement, exp.Create)
        and statement.args.get("kind") == "TABLE"
        and isinstance(statement.this, exp.Schema)
        and statement.this.this.name == "policies"
    )
    database_columns = tuple(
        column.this.name
        for column in declaration.this.expressions
        if isinstance(column, exp.ColumnDef)
    )

    assert POLICY_COLUMNS == RELATION_COLUMNS["policies"]
    assert database_columns == POLICY_COLUMNS
    assert "archived_at" not in VIEW_COLUMNS["policies"]


def test_database_allowlist_matches_reader_role_grants():
    grant_path = Path(__file__).resolve().parents[1] / "src/lisa/sql/activities/grant_runtime_roles.sql"
    grants = grant_path.read_text(encoding="utf-8")
    reader_grants = {
        " ".join(grant.split())
        for grant in re.findall(r"(?m)^GRANT\b[^;]*\bTO \{reader\};", grants)
    }
    expected_grants = {
        f"GRANT SELECT ON lisa.{relation} TO {{reader}};"
        for relation in VIEW_COLUMNS
        if relation != "policies"
    }
    expected_grants.update(
        {
            "GRANT SELECT (id, scope, target_agent_name, name, criteria, approved, created_at, updated_at) "
            "ON lisa.policies TO {reader};",
            "GRANT SELECT ON lisa.readable_deployment TO {reader};",
        }
    )

    assert reader_grants == expected_grants


def test_database_schema_resource_lists_the_same_relation_columns_as_validator():
    for relation, columns in RELATION_COLUMNS.items():
        assert f"lisa.{relation}:" in LISA_DATABASE_SCHEMA
        for column in columns:
            assert re.search(rf"\b{re.escape(column)}\b", LISA_DATABASE_SCHEMA)


def test_policy_database_query_rejects_unmodeled_archive_column():
    with pytest.raises(ValueError, match="not an approved Lisa column"):
        validate_read_query(
            "SELECT id, archived_at FROM lisa.policies"
        )


@pytest.mark.parametrize(
    "query",
    [
        "SELECT reference_type, reference_id FROM lisa.readable_activity_events",
        "SELECT reference_type, reference_id FROM lisa.readable_activity_monitor_thread_actions",
    ],
)
def test_read_query_allows_columns_exposed_by_every_activity_event_view(query):
    sql = validate_read_query(query)

    assert "reference_type" in sql
    assert "reference_id" in sql


@pytest.mark.parametrize(
    "case", ["reject_context_wildcard", "reject_qualified_context_wildcard"]
)
def test_read_query_rejects_wildcards_that_could_expose_hidden_columns(case):
    with pytest.raises(ValueError, match="Wildcard projections"):
        validate_read_query(read_sql_case(case))


def test_read_query_applies_limit_when_select_already_has_a_large_limit():
    sql = validate_read_query(read_sql_case("allowed_large_limit"))

    assert sql.endswith("LIMIT 100")
