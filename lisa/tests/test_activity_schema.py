"""Static checks for Lisa's one-shot operational schema."""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path


def _schema_sql() -> str:
    return files("lisa").joinpath("sql", "activities", "schema.sql").read_text(encoding="utf-8")


def test_evaluation_runs_keep_policy_id_as_a_historical_snapshot():
    schema = _schema_sql()
    evaluation_runs = schema.split("CREATE TABLE lisa.evaluation_runs (", 1)[1].split(");", 1)[0]
    assert "policy_id text NOT NULL," in evaluation_runs
    assert "REFERENCES lisa.policies(id)" not in evaluation_runs
    assert "CREATE TABLE lisa.evaluation_runs" in schema


def test_activities_are_generic_records_and_evaluations_link_to_them():
    schema = _schema_sql()
    assert "lisa.activities" in schema
    assert "lisa.activity_events" in schema
    assert "activity_id uuid NOT NULL REFERENCES lisa.activities(id)" in schema
    assert "readable_evaluations" in schema


def test_schema_has_durable_followups_and_direct_policy_approval():
    schema = _schema_sql()
    assert "lisa.activity_followups" in schema
    assert "trigger_event_type text" in schema
    assert "approved boolean NOT NULL DEFAULT false" in schema
    assert "^P1[.][1-9][0-9]*([.][1-9][0-9]*)?$" in schema
    assert "UNIQUE (scope, target_agent_name, name)" in schema
    assert "origin_slack_channel_id text" in schema
    assert "next_action_at timestamptz" in schema


def test_activity_timeline_actions_can_link_ordered_source_evidence():
    schema = _schema_sql()
    assert "reference_type text" in schema
    assert "reference_id text" in schema
    assert "activity_events_reference_idx" in schema
    assert "activity_events_runtime_reference_unique" in schema
    assert "activity_monitor_runtime_event_global_idx" in schema
    assert "readable_activity_monitor_thread_actions" in schema


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


def test_policy_insert_uses_the_caller_allocated_readable_id():
    statement = (
        files("lisa").joinpath("sql", "policies", "save_policy.sql").read_text(encoding="utf-8")
    )
    assert "VALUES (%(id)s" in statement
    assert "format('P1." not in statement
