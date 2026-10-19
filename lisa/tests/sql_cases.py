from __future__ import annotations

import re
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

_PROVISION_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "provision_lisa.py"


def provision_lisa_schema(
    dsn: str,
    workspace_id: str,
    *,
    writer_role: str = "lisa_writer",
    reader_role: str = "lisa_reader",
) -> None:
    """Recreate the Lisa schema via the one-shot deploy script."""
    spec = spec_from_file_location("lisa_provision", _PROVISION_SCRIPT)
    module = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    module.provision(
        dsn,
        workspace_id,
        writer_role=writer_role,
        reader_role=reader_role,
        recreate=True,
    )


def read_sql_case(name: str) -> str:
    source = Path(__file__).parent / "sql" / "query_safety_cases.sql"
    cases = re.findall(
        r"(?ms)^-- case: ([a-z_]+)\s*\n(.*?)(?=^-- case: |\Z)",
        source.read_text(encoding="utf-8"),
    )
    try:
        return next(statement.strip().rstrip(";").strip() for case, statement in cases if case == name)
    except StopIteration as error:
        raise KeyError(name) from error


def read_sql_file(name: str) -> str:
    return (Path(__file__).parent / "sql" / "activities" / f"{name}.sql").read_text(
        encoding="utf-8"
    )
