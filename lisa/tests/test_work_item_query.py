from __future__ import annotations

import pytest
from sql_cases import read_sql_case, read_sql_file

from lisa.repositories.lisa_database import validate_read_query


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

    assert "FROM lisa.readable_work_items" in sql
    assert "LIMIT 100" in sql


def test_read_query_allows_the_policy_table_but_not_other_base_tables():
    sql = validate_read_query(read_sql_file("read_policy_columns"))

    assert "FROM lisa.policies" in sql
    assert "LIMIT 100" in sql
    with pytest.raises(ValueError, match="approved Lisa views or tables"):
        validate_read_query(read_sql_file("reject_work_item_base_table"))


@pytest.mark.parametrize(
    "case", ["reject_context_wildcard", "reject_qualified_context_wildcard"]
)
def test_read_query_rejects_wildcards_that_could_expose_hidden_columns(case):
    with pytest.raises(ValueError, match="Wildcard projections"):
        validate_read_query(read_sql_case(case))


def test_read_query_applies_limit_when_select_already_has_a_large_limit():
    sql = validate_read_query(read_sql_case("allowed_large_limit"))

    assert sql.endswith("LIMIT 100")
