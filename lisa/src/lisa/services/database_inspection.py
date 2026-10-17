"""Application boundary for Lisa's bounded, read-only SQL tool."""

from typing import Any

from ..repositories.lisa_database import LisaDatabaseReader


class DatabaseInspectionService:
    def __init__(self, reader: LisaDatabaseReader | None):
        self.reader = reader

    def query(self, statement: str) -> dict[str, Any]:
        if self.reader is None:
            raise RuntimeError("LISA_DATABASE_READ_URL is not configured")
        return self.reader.query(statement)
