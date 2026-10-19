"""Static checks for Lisa's one-shot operational schema."""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path


def _schema_sql() -> str:
    return files("lisa").joinpath("sql", "activities", "schema.sql").read_text(encoding="utf-8")


def test_schema_is_a_clean_create_not_an_upgrade_migration():
    schema = _schema_sql()
    assert "CREATE SCHEMA lisa" in schema
    assert "IF NOT EXISTS" not in schema
    assert "schema_migrations" not in schema
    assert "ALTER TABLE" not in schema
    assert "evidence_window" not in schema


def test_evaluation_runs_require_policy_id():
    schema = _schema_sql()
    assert "policy_id text NOT NULL REFERENCES lisa.policies(id)" in schema
    assert "CREATE TABLE lisa.evaluation_runs" in schema


def test_activities_are_generic_records_and_evaluations_link_to_them():
    schema = _schema_sql()
    assert "lisa.activities" in schema
    assert "lisa.activity_events" in schema
    assert "activity_id uuid NOT NULL REFERENCES lisa.activities(id)" in schema
    assert "readable_evaluations" in schema


def test_provision_script_requires_explicit_recreate_for_existing_schema():
    script = (Path(__file__).resolve().parents[1] / "scripts" / "provision_lisa.py").read_text(
        encoding="utf-8"
    )
    assert "LISA_DATABASE_RECREATE" in script
    assert "DROP SCHEMA IF EXISTS lisa CASCADE" in (
        files("lisa").joinpath("sql", "activities", "drop_schema.sql").read_text()
    )


def test_database_creation_uses_the_provisioning_connection_owner():
    statement = (
        files("lisa")
        .joinpath("sql", "activities", "create_lisa_database.sql")
        .read_text(encoding="utf-8")
        .strip()
    )
    assert statement == "CREATE DATABASE lisa"


def test_policy_insert_escapes_postgres_format_percent_for_psycopg():
    statement = (
        files("lisa").joinpath("sql", "policies", "save_policy.sql").read_text(encoding="utf-8")
    )
    assert "format('P1.%%s', nextval" in statement
