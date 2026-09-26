"""Unit tests for scripts/perf helpers (stdlib unittest)."""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from agent_otlp import (
    GEN_AI_IN_TOKENS,
    GEN_AI_MODEL,
    GEN_AI_OUT_TOKENS,
    GEN_AI_PROVIDER,
    GEN_AI_TOTAL_TOKENS,
    SERVICE_NAME,
    SP_AGENT_NAME,
    SP_COST_TOTAL,
    SP_OBSERVATION_TYPE,
    SP_SESSION_ID,
    SP_USER_ID,
    build_agent_session,
)
from percentiles import percentile_ms, summarize
from seed_sql import ONE_CLOCK_PARTITION_BY, load_table_sql

ROOT = Path(__file__).resolve().parents[2]
ATTR_KEYS = ROOT / "src" / "models" / "attr_keys.rs"


class PercentileTests(unittest.TestCase):
    def test_empty(self):
        self.assertIsNone(percentile_ms([], 50))

    def test_p50_odd(self):
        self.assertEqual(percentile_ms([1, 2, 3], 50), 2.0)

    def test_summarize_keys(self):
        s = summarize([10.0, 20.0, 30.0, 40.0, 100.0])
        self.assertEqual(s["count"], 5)
        self.assertIsNotNone(s["p50_ms"])
        self.assertIsNotNone(s["p95_ms"])
        self.assertIsNotNone(s["p99_ms"])


class Rfc3339Tests(unittest.TestCase):
    def test_duckdb_timestamp_normalized(self):
        from bench_llm_load import to_rfc3339

        self.assertEqual(
            to_rfc3339("2026-07-22 03:57:57.199+00"),
            "2026-07-22T03:57:57.199Z",
        )
        self.assertEqual(
            to_rfc3339("2026-09-26T06:15:34.902Z"),
            "2026-09-26T06:15:34.902Z",
        )


class ProcessCpuTests(unittest.TestCase):
    def test_cpu_ratio_one_core(self):
        from process_cpu import USER_HZ, cpu_ratio

        # 100 jiffies over 1s at USER_HZ=100 → 1.0 core
        self.assertAlmostEqual(cpu_ratio(0, int(USER_HZ), 1.0), 1.0)
        self.assertAlmostEqual(cpu_ratio(0, int(USER_HZ / 2), 1.0), 0.5)

    def test_summarize_cpu_empty(self):
        from process_cpu import summarize_cpu

        s = summarize_cpu([])
        self.assertEqual(s["count"], 0)
        self.assertIsNone(s["mean_cores"])

    def test_summarize_cpu_mean(self):
        from process_cpu import summarize_cpu

        s = summarize_cpu([0.1, 0.2, 0.3])
        self.assertEqual(s["count"], 3)
        self.assertAlmostEqual(s["mean_cores"], 0.2)

    def test_read_jiffies_self(self):
        from process_cpu import read_jiffies
        import os

        j = read_jiffies(os.getpid())
        self.assertIsNotNone(j)
        self.assertGreaterEqual(j, 0)

class AgentOtlpTests(unittest.TestCase):
    def test_session_tree_shape(self):
        body, sid, n = build_agent_session(
            session_id="sess-test",
            traces_per_session=2,
            spans_per_trace=4,
        )
        self.assertEqual(sid, "sess-test")
        self.assertGreaterEqual(n, 2 * 4)
        spans = body["resourceSpans"][0]["scopeSpans"][0]["spans"]
        self.assertEqual(len(spans), n)
        session_ids = {
            a["value"]["stringValue"]
            for s in spans
            for a in s["attributes"]
            if a["key"] == SP_SESSION_ID
        }
        self.assertEqual(session_ids, {"sess-test"})
        obs = {
            a["value"]["stringValue"]
            for s in spans
            for a in s["attributes"]
            if a["key"] == SP_OBSERVATION_TYPE
        }
        self.assertIn("agent", obs)
        self.assertIn("generation", obs)
        trace_ids = {s["traceId"] for s in spans}
        self.assertEqual(len(trace_ids), 2)
        roots = [s for s in spans if not s["parentSpanId"]]
        children = [s for s in spans if s["parentSpanId"]]
        self.assertEqual(len(roots), 2)
        self.assertTrue(all(c["parentSpanId"] for c in children))

    def test_attr_keys_lockstep_with_rust(self):
        src = ATTR_KEYS.read_text()
        # Extract string literals assigned in attr_keys.rs
        rust_vals = set(re.findall(r'pub const \w+: &str = "([^"]+)";', src))
        py_vals = {
            SP_SESSION_ID,
            SP_OBSERVATION_TYPE,
            SP_USER_ID,
            SP_AGENT_NAME,
            SP_COST_TOTAL,
            GEN_AI_MODEL,
            GEN_AI_PROVIDER,
            GEN_AI_IN_TOKENS,
            GEN_AI_OUT_TOKENS,
            GEN_AI_TOTAL_TOKENS,
            SERVICE_NAME,
        }
        missing = py_vals - rust_vals
        self.assertFalse(
            missing,
            f"agent_otlp keys missing from attr_keys.rs: {missing}",
        )


class SeedSqlTests(unittest.TestCase):
    def test_load_sql_is_schema_then_insert_not_ctas_data(self):
        sql = load_table_sql(
            alias="seedlake",
            table="traces",
            parquet_path="/tmp/seed/traces/data.parquet",
            force_drop=True,
        )
        self.assertIn("DROP TABLE IF EXISTS seedlake.traces;", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS seedlake.traces AS SELECT * FROM read_parquet", sql)
        self.assertIn("LIMIT 0;", sql)
        self.assertIn(f"SET PARTITIONED BY ({ONE_CLOCK_PARTITION_BY})", sql)
        self.assertIn("SET SORTED BY (session_id, trace_id, timestamp)", sql)
        self.assertIn("INSERT INTO seedlake.traces BY NAME SELECT * FROM read_parquet", sql)
        # Must not load data via CTAS (no CTAS without LIMIT 0)
        create_lines = [ln for ln in sql.splitlines() if "CREATE TABLE" in ln]
        self.assertEqual(len(create_lines), 1)
        self.assertIn("LIMIT 0", create_lines[0])

    def test_force_drop_optional(self):
        sql = load_table_sql(
            alias="x",
            table="logs",
            parquet_path="/p.parquet",
            force_drop=False,
        )
        self.assertNotIn("DROP TABLE", sql)
        self.assertIn("INSERT INTO x.logs BY NAME", sql)


if __name__ == "__main__":
    unittest.main()
