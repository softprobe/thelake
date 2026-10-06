//! Production lake SQL — recipes, bounds, literals, schema registry.
//!
//! Design: [`docs/design-sql-and-schema.md`](../../docs/design-sql-and-schema.md).
//! Callers outside this package must not embed SQL verbs.

pub mod bounds;
pub mod health;
pub mod lake_reads;
pub mod literal;
pub mod llm;
pub mod logs;
pub mod maintenance;
pub(crate) mod paging;
pub mod promotion;
pub mod schema;
pub mod session_summary;
pub mod telemetry;
pub mod tempo;
// The typed boundary is introduced before its first engine consumer so the
// contract can be reviewed independently of the query migration.
#[allow(dead_code)]
pub(crate) mod trusted;
pub mod writer;

#[cfg(test)]
pub(crate) use bounds::assert_sql_has_otlp_time_predicates;
pub(crate) use bounds::ensure_fact_scan_uses_timestamp_pruning;
#[cfg(test)]
pub(crate) use bounds::ensure_sql_has_bare_timestamp_predicate;
pub(crate) use bounds::push_otlp_ns_window_predicates;
pub(crate) use bounds::{
    execute_batch_checked, execute_batch_for_parquet_ingest, execute_maintenance_script,
    prepare_checked,
};
pub use bounds::{
    push_otlp_time_predicates, query_window_from_exclusive_ns, QueryWindow, TimestampFilteredSql,
};
pub use literal::{sql_string_literal, timestamp_ns_literal, timestamptz_literal};

pub use schema::{
    fact_table_specs, insert_order_by, is_otlp_table, qualified_table_name, table_spec, TableSpec,
    LOGS, OTLP_TABLES, SCORES, SCORE_CONFIGS, TRACES,
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
            "/session_summary/reduce.rs",
            "/session_summary/tests.rs",
            "/api/health.rs",
            "/storage/schema/ducklake_partition.rs",
            "/storage/schema/otlp_layout.rs",
            "/storage/schema/attribute_map.rs",
            "/storage/ducklake/attach.rs",
            "/storage/ducklake/layout.rs",
            "/storage/ducklake/promotion.rs",
            "/storage/ducklake/writer.rs",
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
                crate::sql::ensure_sql_has_bare_timestamp_predicate(&sql).is_ok(),
                "recipe must carry its timestamp predicate:\n{sql}"
            );
        }
        assert!(crate::sql::ensure_sql_has_bare_timestamp_predicate(
            &crate::sql::telemetry::details_spans_sql("*", "trace_id = 'x'", 10)
        )
        .is_err());
    }

    #[test]
    fn lower_layers_must_not_depend_on_api() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut violations = Vec::new();
        for subtree in ["sql", "session_summary", "query"] {
            let dir = root.join(subtree);
            walk_api_refs(&dir, &mut violations);
        }
        assert!(
            violations.is_empty(),
            "sql/session_summary/query must not reference crate::api:\n{}",
            violations.join("\n")
        );
    }

    fn walk_api_refs(path: &Path, violations: &mut Vec<String>) {
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk_api_refs(&path, violations);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            if name.ends_with("_tests.rs") || name == "tests.rs" || name == "unit_tests.rs" {
                continue;
            }
            let text = without_cfg_test_modules(&fs::read_to_string(&path).unwrap_or_default());
            for (i, line) in text.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.starts_with("//") {
                    continue;
                }
                if trimmed.contains("crate::api::") || trimmed.contains("use crate::api") {
                    violations.push(format!("{}:{}:{}", path.display(), i + 1, trimmed));
                }
            }
        }
    }

    #[test]
    fn query_engine_must_not_embed_sql_verb_literals() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/query");
        let mut hits = Vec::new();
        walk_rs(&root, &mut hits);
        assert!(
            hits.is_empty(),
            "QueryEngine is execute-only; SQL verbs belong in sql/:\n{}",
            hits.join("\n")
        );
    }

    #[test]
    fn appstate_trusted_sql_helper_must_not_exist() {
        let api_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/api");
        let mut violations = Vec::new();
        walk_forbidden_token(
            &api_root,
            "execute_tenant_scoped_trusted_sql",
            &mut violations,
        );
        assert!(
            violations.is_empty(),
            "AppState::execute_tenant_scoped_trusted_sql must be removed; product handlers use RuntimeEngine::execute_trusted / QueryEngine typed methods:\n{}",
            violations.join("\n")
        );
    }

    fn walk_forbidden_token(path: &Path, token: &str, violations: &mut Vec<String>) {
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk_forbidden_token(&path, token, violations);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            if name.ends_with("_tests.rs") || name == "tests.rs" || name == "unit_tests.rs" {
                continue;
            }
            let text = without_cfg_test_modules(&fs::read_to_string(&path).unwrap_or_default());
            for (i, line) in text.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.starts_with("//") {
                    continue;
                }
                if trimmed.contains(token) {
                    violations.push(format!("{}:{}:{}", path.display(), i + 1, trimmed));
                }
            }
        }
    }

    #[test]
    fn approved_query_minting_stays_inside_sql() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut violations = Vec::new();
        walk_approved_query(&root, &mut violations);
        assert!(
            violations.is_empty(),
            "approved_query may only appear under src/sql/:\n{}",
            violations.join("\n")
        );
    }

    fn walk_approved_query(path: &Path, violations: &mut Vec<String>) {
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().and_then(|n| n.to_str()) == Some("sql") {
                    continue;
                }
                walk_approved_query(&path, violations);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            if name.ends_with("_tests.rs") || name == "tests.rs" || name == "unit_tests.rs" {
                continue;
            }
            let text = without_cfg_test_modules(&fs::read_to_string(&path).unwrap_or_default());
            for (i, line) in text.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.starts_with("//") {
                    continue;
                }
                if trimmed.contains("approved_query") {
                    violations.push(format!("{}:{}:{}", path.display(), i + 1, trimmed));
                }
            }
        }
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
