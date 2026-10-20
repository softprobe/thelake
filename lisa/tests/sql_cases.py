from __future__ import annotations

import re
from pathlib import Path


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
