#!/usr/bin/env python3
"""Tests for scripts/one_clock_parquet_backup.py (synthetic old → one-clock parquet)."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "one_clock_parquet_backup.py"
sys.path.insert(0, str(ROOT / "scripts"))

import one_clock_parquet_backup as backup  # noqa: E402


class OneClockParquetBackupTest(unittest.TestCase):
    def test_schema_to_workspace_name(self) -> None:
        self.assertEqual(backup.schema_to_workspace_name("sp_llm"), "sp-llm")
        self.assertEqual(
            backup.schema_to_workspace_name("ws_myworkspace_mtyxusmz_2t77yn"),
            "ws-myworkspace-mtyxusmz-2t77yn",
        )

    def test_export_selects_match_copy_script_intent(self) -> None:
        self.assertIn("EXCLUDE (record_date)", backup.EXPORT_SELECTS["traces"])
        self.assertIn("EXCLUDE (record_date)", backup.EXPORT_SELECTS["logs"])
        self.assertIn("EXCLUDE (record_date)", backup.EXPORT_SELECTS["scores"])
        copy_sql = (ROOT / "scripts" / "one_clock_catalog_copy.sql").read_text()
        for table in ("traces", "logs", "scores"):
            self.assertIn(table, copy_sql)
            self.assertIn(table, backup.EXPORT_SELECTS)
        self.assertNotIn("metric_samples", copy_sql)
        self.assertNotIn("metric_samples", backup.EXPORT_SELECTS)

    def test_fixture_export_drops_forbidden_columns(self) -> None:
        duckdb = backup.find_duckdb(None)
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            manifest = backup.export_workspace(
                duckdb=duckdb,
                preamble=backup.fixture_bootstrap_sql(),
                workspace="fixture-demo",
                out_root=out,
                src_prefix=backup.ATTACH_ALIAS,
                table_schema=backup.ATTACH_ALIAS,
            )
            self.assertTrue(manifest["ok"], manifest)
            exported = [
                t for t in manifest["tables"] if t["status"] == "exported"
            ]
            self.assertEqual(len(exported), len(backup.EXPORT_SELECTS), manifest)

            traces = out / "fixture-demo" / "traces" / "data.parquet"
            self.assertTrue(traces.is_file())
            cols = backup.parquet_columns(duckdb, traces)
            self.assertNotIn("record_date", cols)
            self.assertIn("timestamp", cols)
            self.assertIn("session_id", cols)
            self.assertEqual(backup.parquet_row_count(duckdb, traces), 2)

            # CLI entrypoint
            cli_out = out / "cli"
            proc = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--mode",
                    "fixture",
                    "--workspace",
                    "cli-demo",
                    "--out-dir",
                    str(cli_out),
                    "--duckdb",
                    duckdb,
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr + proc.stdout)
            cli_manifest = json.loads((cli_out / "cli-demo" / "MANIFEST.json").read_text())
            self.assertTrue(cli_manifest["ok"])


if __name__ == "__main__":
    unittest.main()
