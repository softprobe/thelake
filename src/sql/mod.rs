//! Production lake SQL — recipes, bounds, literals, schema registry.
//!
//! Design: [`docs/design-sql-and-schema.md`](../../docs/design-sql-and-schema.md).
//! Callers outside this package must not embed SQL verbs.

pub mod bounds;
pub mod literal;
pub mod llm;
pub mod logs;
pub mod maintenance;
pub mod promotion;
pub mod query;
pub mod schema;
pub mod session_summary;
pub mod telemetry;
pub mod tempo;
// The typed boundary is introduced before its first engine consumer so the
// contract can be reviewed independently of the query migration.
#[allow(dead_code)]
pub(crate) mod trusted;
pub mod writer;

pub(crate) use bounds::{ensure_fact_scan_bound, execute_batch_checked, prepare_checked};
pub use bounds::{query_window_from_exclusive_ns, BoundLakeSql, QueryWindow};
pub use literal::{sql_string_literal, timestamp_ns_literal, timestamptz_literal};

/// UTC calendar-day expression for a `TIMESTAMPTZ` index timestamp.
/// DuckDB otherwise applies `date_trunc` in the session timezone.
pub fn utc_calendar_day_expr(column: &str) -> String {
    format!("date_trunc('day', {column} AT TIME ZONE 'UTC')")
}

/// Equality predicate for two rows that belong to the same UTC day.
pub fn same_utc_calendar_day(left: &str, right: &str) -> String {
    format!(
        "{} = {}",
        utc_calendar_day_expr(left),
        utc_calendar_day_expr(right)
    )
}

pub use schema::{
    fact_table_specs, insert_order_by, is_otlp_table, qualified_table_name, table_spec, TableSpec,
    LOGS, ONE_CLOCK_PARTITION_BY, OTLP_TABLES, SCORES, SCORE_CONFIGS, TRACES,
};

#[cfg(test)]
mod locality_tests {
    use std::fs;
    use std::path::Path;

    /// SQL verb tokens that must not appear as string literals outside `src/sql/` (+ tests).
    fn looks_like_sql_verb_literal(line: &str) -> bool {
        let t = line.trim();
        // Heuristic: assignment or format! / concat building SELECT/INSERT/…
        let upper = t.to_ascii_uppercase();
        let has_verb = [
            "SELECT ",
            "INSERT ",
            "UPDATE ",
            "DELETE ",
            "CREATE TABLE",
            "ALTER TABLE",
            "ATTACH ",
            "COPY ",
            "EXPLAIN ",
        ]
        .iter()
        .any(|v| upper.contains(v));
        if !has_verb {
            return false;
        }
        // Ignore comments and this test module's own needles.
        if t.starts_with("//") || t.starts_with("///") || t.contains("looks_like_sql_verb") {
            return false;
        }
        // String literal containing SQL
        t.contains('"') || t.contains('\'')
    }

    /// Drop `#[cfg(test)] mod … { … }` bodies so embedded unit-test SQL is not flagged.
    fn without_cfg_test_modules(text: &str) -> String {
        let lines: Vec<&str> = text.lines().collect();
        let mut out = Vec::with_capacity(lines.len());
        let mut i = 0;
        while i < lines.len() {
            let trimmed = lines[i].trim();
            if trimmed.starts_with("#[cfg(test)]") {
                // Skip attribute + following `mod name { ... }` (or inline attrs).
                i += 1;
                while i < lines.len() && lines[i].trim().starts_with("#[") {
                    i += 1;
                }
                if i < lines.len() && lines[i].trim().starts_with("mod ") {
                    let mut depth = 0i32;
                    let mut started = false;
                    while i < lines.len() {
                        for ch in lines[i].chars() {
                            if ch == '{' {
                                depth += 1;
                                started = true;
                            } else if ch == '}' {
                                depth -= 1;
                            }
                        }
                        i += 1;
                        if started && depth <= 0 {
                            break;
                        }
                    }
                    continue;
                }
                continue;
            }
            out.push(lines[i]);
            i += 1;
        }
        out.join("\n")
    }

    fn walk_rs(dir: &Path, hits: &mut Vec<String>) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().and_then(|n| n.to_str()) == Some("sql") {
                    continue; // allowed
                }
                walk_rs(&path, hits);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                let text = without_cfg_test_modules(&fs::read_to_string(&path).unwrap_or_default());
                for (i, line) in text.lines().enumerate() {
                    if looks_like_sql_verb_literal(line) {
                        // Allow re-exports / thin wrappers that only mention verbs in docs —
                        // still flag real string literals with SQL.
                        if line.contains("format!(")
                            || line.contains("concat!(")
                            || (line.contains('"') && line.to_ascii_uppercase().contains("SELECT "))
                            || (line.contains('"') && line.to_ascii_uppercase().contains("INSERT "))
                            || (line.contains('"')
                                && line.to_ascii_uppercase().contains("ALTER TABLE"))
                            || (line.contains('"')
                                && line.to_ascii_uppercase().contains("CREATE TABLE"))
                        {
                            hits.push(format!("{}:{}:{}", path.display(), i + 1, line.trim()));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn production_sql_only_under_src_sql() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut hits = Vec::new();
        walk_rs(&root, &mut hits);
        // Test fixtures and explicitly classified connection/bootstrap SQL are
        // the only exceptions. Keep this file-level list narrow: directory-wide
        // exemptions hide production fact SQL regressions.
        //
        // STOP: make sure you fully understand this list before adding to it. You must be careful to add
        // an exemption only when the SQL is absolutely safe to skip the gate check!!!
        let allowed = [
            "/bin/",
            "_tests.rs",
            "/tests.rs",
            "/unit_tests.rs",
            "/promotion.rs",
            "/runtime_engine.rs",
            "/async_jobs/tests.rs",
            "/session_summary/ddl.rs",
            "/session_summary/dirty.rs",
            "/session_summary/list.rs",
            "/session_summary/reduce.rs",
            "/session_summary/tests.rs",
            "/api/health.rs",
            "/storage/schema/ducklake_partition.rs",
            "/storage/schema/variant.rs",
            "/storage/ducklake/attach.rs",
            "/storage/ducklake/promotion.rs",
            "/storage/duckdb/cache.rs",
            "/storage/duckdb/engine.rs",
            "/storage/ducklake/workspace_views.rs",
        ];
        let hard: Vec<_> = hits
            .into_iter()
            .filter(|h| !allowed.iter().any(|t| h.contains(t)))
            .collect();
        assert!(
            hard.is_empty(),
            "SQL verb literals outside src/sql/:\n{}",
            hard.join("\n")
        );
    }

    #[test]
    fn representative_trace_log_recipes_are_gate_checked() {
        let bounded_trace = crate::sql::telemetry::details_spans_sql(
            "*",
            "timestamp >= '2026-09-10'::TIMESTAMP_NS",
            10,
        );
        let bounded_log = crate::sql::logs::scan_sql(
            " AND timestamp <= '2026-09-11'::TIMESTAMP_NS",
            "NULL::VARCHAR AS promoted",
            10,
        );
        for sql in [bounded_trace, bounded_log] {
            assert!(
                crate::sql::ensure_fact_scan_bound(&sql).is_ok(),
                "recipe must carry its timestamp bound:\n{sql}"
            );
        }
        assert!(
            crate::sql::ensure_fact_scan_bound(&crate::sql::telemetry::details_spans_sql(
                "*",
                "trace_id = 'x'",
                10
            ))
            .is_err()
        );
    }

    #[test]
    fn domain_duckdb_connections_stay_inside_approved_engines() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut violations = Vec::new();
        let approved = [
            "/ingest_engine/",
            "/query/",
            "/compaction/",
            "/storage/",
            "/sql/bounds/",
            "/sql/mod.rs",
        ];

        fn walk(path: &Path, approved: &[&str], violations: &mut Vec<String>) {
            let Ok(entries) = fs::read_dir(path) else {
                return;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, approved, violations);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                let file_name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("");
                if file_name.ends_with("_tests.rs") || file_name == "tests.rs" {
                    continue;
                }
                let text = without_cfg_test_modules(&fs::read_to_string(&path).unwrap_or_default());
                let uses_connection = text.lines().any(|line| {
                    line.contains("duckdb::Connection")
                        || line.contains("use duckdb::Connection")
                        || line.contains("use duckdb::{Connection")
                });
                let uses_writer = text.contains("DuckLakeWriter");
                if (uses_connection || uses_writer)
                    && !approved
                        .iter()
                        .any(|fragment| path.to_string_lossy().contains(fragment))
                {
                    violations.push(path.display().to_string());
                }
            }
        }

        walk(&root, &approved, &mut violations);
        assert!(
            violations.is_empty(),
            "domain DuckDB connection access outside approved engines:\n{}",
            violations.join("\n")
        );
    }
}
