"""Unit tests for scripts/copy_traces_workspace_uuid.py (stdlib unittest)."""

from __future__ import annotations

import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "copy_traces_workspace_uuid.py"
SPEC = importlib.util.spec_from_file_location("copy_traces_workspace_uuid", SCRIPT)
MOD = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MOD
assert SPEC.loader is not None
SPEC.loader.exec_module(MOD)

UUID_A = "550e8400-e29b-41d4-a716-446655440000"
UUID_B = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
SLUG_A = "ws-acme-mtyxusmz-2t77yn"
SLUG_B = "ws-beta-aaaa-bbbb"


class LoadMappingTests(unittest.TestCase):
    def test_json_object(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            json.dump({SLUG_A: UUID_A, SLUG_B: UUID_B.upper()}, fh)
            path = pathlib.Path(fh.name)
        mapping = MOD.load_mapping(path)
        self.assertEqual(mapping[SLUG_A], UUID_A)
        self.assertEqual(mapping[SLUG_B], UUID_B)

    def test_json_array_tenant_id(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            json.dump(
                [
                    {"tenant_id": SLUG_A, "workspace_id": UUID_A},
                    {"lake_scope_id": SLUG_B, "workspace_id": UUID_B},
                ],
                fh,
            )
            path = pathlib.Path(fh.name)
        self.assertEqual(MOD.load_mapping(path), {SLUG_A: UUID_A, SLUG_B: UUID_B})

    def test_csv_lake_scope_id(self):
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as fh:
            fh.write("lake_scope_id,workspace_id\n")
            fh.write(f"{SLUG_A},{UUID_A}\n")
            path = pathlib.Path(fh.name)
        self.assertEqual(MOD.load_mapping(path), {SLUG_A: UUID_A})

    def test_rejects_empty(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            fh.write("{}")
            path = pathlib.Path(fh.name)
        with self.assertRaises(ValueError):
            MOD.load_mapping(path)

    def test_rejects_bad_uuid(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            json.dump({SLUG_A: "not-a-uuid"}, fh)
            path = pathlib.Path(fh.name)
        with self.assertRaises(ValueError):
            MOD.load_mapping(path)

    def test_rejects_conflicting_duplicate(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            json.dump(
                [
                    {"tenant_id": SLUG_A, "workspace_id": UUID_A},
                    {"tenant_id": SLUG_A, "workspace_id": UUID_B},
                ],
                fh,
            )
            path = pathlib.Path(fh.name)
        with self.assertRaises(ValueError):
            MOD.load_mapping(path)


class SqlBuilderTests(unittest.TestCase):
    def test_rewrites_tenant_id_to_workspace_id(self):
        sql = MOD.build_insert_select_sql(
            source_table='"old"."softprobe".traces',
            target_table='"softprobe"."thelake".traces',
            source_columns=["session_id", "trace_id", "tenant_id", "timestamp"],
            target_columns=["session_id", "trace_id", "workspace_id", "timestamp"],
            source_tenancy="tenant_id",
            workspace_id=UUID_A,
            old_slug=SLUG_A,
        )
        self.assertIn(f"'{UUID_A}' AS \"workspace_id\"", sql)
        self.assertNotIn('"tenant_id"', sql.split("SELECT", 1)[1].split("FROM", 1)[0])
        self.assertIn(f"WHERE \"tenant_id\" = '{SLUG_A}'", sql)
        self.assertIn('INSERT INTO "softprobe"."thelake".traces', sql)

    def test_drops_source_only_promotions(self):
        sql = MOD.build_insert_select_sql(
            source_table="src.traces",
            target_table="dst.traces",
            source_columns=["session_id", "tenant_id", "promo_col"],
            target_columns=["session_id", "workspace_id"],
            source_tenancy="tenant_id",
            workspace_id=UUID_A,
            old_slug=SLUG_A,
        )
        self.assertNotIn("promo_col", sql)

    def test_requires_workspace_id_on_target(self):
        with self.assertRaises(ValueError):
            MOD.build_insert_select_sql(
                source_table="src.traces",
                target_table="dst.traces",
                source_columns=["session_id", "tenant_id"],
                target_columns=["session_id", "tenant_id"],
                source_tenancy="tenant_id",
                workspace_id=UUID_A,
                old_slug=SLUG_A,
            )

    def test_source_tenancy_prefers_tenant_id(self):
        self.assertEqual(
            MOD.source_tenancy_column(["session_id", "tenant_id", "lake_scope_id"]),
            "tenant_id",
        )

    def test_source_tenancy_falls_back_to_lake_scope_id(self):
        self.assertEqual(
            MOD.source_tenancy_column(["session_id", "lake_scope_id"]),
            "lake_scope_id",
        )

    def test_excluded_signals_are_not_traces(self):
        self.assertNotIn("traces", MOD.EXCLUDED_SIGNAL_TABLES)
        self.assertEqual(
            set(MOD.EXCLUDED_SIGNAL_TABLES),
            {"logs", "scores", "session_summary"},
        )


class AttachSqlTests(unittest.TestCase):
    def test_prefixes_postgres_dsn(self):
        sql = MOD.attach_sql(
            alias="old",
            metadata_path="host=db user=u",
            data_path="gs://bucket/path/",
            metadata_schema="softprobe",
        )
        self.assertIn("ATTACH 'ducklake:postgres:host=db user=u' AS old", sql)
        self.assertIn("DATA_PATH 'gs://bucket/path/'", sql)
        self.assertIn("CREATE_IF_NOT_EXISTS false", sql)

    def test_rejects_bad_alias(self):
        with self.assertRaises(ValueError):
            MOD.attach_sql(
                alias="old-lake",
                metadata_path="postgres:host=db",
                data_path="/tmp/data",
                metadata_schema="main",
            )


class QuoteTests(unittest.TestCase):
    def test_literal_escapes_quotes(self):
        self.assertEqual(MOD.quote_literal("a'b"), "'a''b'")

    def test_ident_escapes_quotes(self):
        self.assertEqual(MOD.quote_ident('a"b'), '"a""b"')


if __name__ == "__main__":
    unittest.main()
