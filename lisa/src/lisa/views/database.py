"""Tool-facing description of Lisa's read-only operational database."""

from textwrap import fill

from lisa.models.database import MAX_ROWS, RELATION_COLUMNS

_RELATION_NOTES = {
    "readable_evaluations": (
        "These rows are Lisa's actual judge runs and link to an activity by activity_id."
    ),
    "readable_evaluation_triage": "Join to lisa.policies when the user specifies a policy name.",
    "readable_activity_monitor_links": (
        "Types are agent, message, runtime_event, runtime_work, and carrier_message."
    ),
    "policies": "Policy deletion removes the row; there is no archived_at column.",
}


def _relation_description(name: str, columns: tuple[str, ...]) -> str:
    description = f"- lisa.{name}: {', '.join(columns)}"
    note = _RELATION_NOTES.get(name)
    if note:
        description += f". {note}"
    return fill(description, width=88, subsequent_indent="  ")


_RELATION_DESCRIPTIONS = "\n".join(
    _relation_description(name, columns) for name, columns in RELATION_COLUMNS.items()
)

LISA_DATABASE_SCHEMA = f"""Use lisa_db for explicit read-only database diagnostics
or to retrieve Lisa's recorded evaluation history. For activity work, use Lisa's activity tools;
for policy work, use read_policy, list_policies, write_policy, approve_policy,
and delete_policy.
If the user asks which columns the live database stores, verify against the
database with an explicit zero-row projection from the named relation. Do not
answer only from remembered schema or return row data when only the schema was
requested.
Do not query lisa_db to find, create, update, or inspect activities. For a
historical triage lookup by policy name, join lisa.policies on policy_id and
filter by its name.

When explicit database diagnostics are requested, these approved relations and
columns are available:

{_RELATION_DESCRIPTIONS}

Use each relation with its full ``lisa.<relation>`` name exactly as listed.
Never retry a query with an unqualified relation name.

Lisa's activity tables store activity history and outcomes, not workspace
session telemetry. Keep diagnostic queries bounded. Queries must name columns
explicitly; wildcard projections are rejected. They return at most {MAX_ROWS}
rows and include a ``truncated`` flag; report when a result is truncated instead
of saying it contains every row.
Do not probe absent columns by selecting them. This schema is the authoritative
column list: if a requested field is not listed, report that it is unavailable
without issuing a query that names it.
Use Lisa's ``search_sessions`` and ``get_session`` tools when the user asks for
telemetry evidence. Policy changes still go through owner-confirmed tools; the
SQL role is read-only. Other Lisa base tables and hidden columns are not exposed.
"""
