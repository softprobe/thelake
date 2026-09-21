//! D12: reject unbound fact scans and forbidden time column names.

const FORBIDDEN_TIME_COLUMNS: &[&str] = &["record_date", "event_date", "window_ts"];

/// Replace string literals and comments with spaces while preserving quoted
/// identifiers and statement punctuation. Gate matching must inspect SQL code,
/// not example text or a timestamp-looking value in a string.
fn code_view(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = bytes.to_vec();
    let mut i = 0;
    let mut single = false;
    let mut line_comment = false;
    let mut block_comment = false;
    while i < bytes.len() {
        if line_comment {
            if bytes[i] == b'\n' {
                line_comment = false;
            } else {
                out[i] = b' ';
            }
            i += 1;
            continue;
        }
        if block_comment {
            if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                out[i] = b' ';
                out[i + 1] = b' ';
                i += 2;
                block_comment = false;
            } else {
                out[i] = b' ';
                i += 1;
            }
            continue;
        }
        if single {
            out[i] = b' ';
            if bytes[i] == b'\'' {
                if bytes.get(i + 1) == Some(&b'\'') {
                    out[i + 1] = b' ';
                    i += 2;
                    continue;
                }
                single = false;
            }
            i += 1;
            continue;
        }
        if bytes[i] == b'\'' {
            out[i] = b' ';
            single = true;
            i += 1;
        } else if bytes[i] == b'-' && bytes.get(i + 1) == Some(&b'-') {
            out[i] = b' ';
            out[i + 1] = b' ';
            line_comment = true;
            i += 2;
        } else if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            out[i] = b' ';
            out[i + 1] = b' ';
            block_comment = true;
            i += 2;
        } else {
            i += 1;
        }
    }
    String::from_utf8(out).expect("SQL is UTF-8")
}

fn statements(sql: &str) -> Vec<&str> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    let mut single = false;
    let mut double = false;
    let mut line_comment = false;
    let mut block_comment = false;
    while i < bytes.len() {
        if line_comment {
            if bytes[i] == b'\n' {
                line_comment = false;
            }
            i += 1;
            continue;
        }
        if block_comment {
            if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                block_comment = false;
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }
        if single {
            if bytes[i] == b'\'' {
                if bytes.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                single = false;
            }
            i += 1;
            continue;
        }
        if double {
            if bytes[i] == b'"' {
                if bytes.get(i + 1) == Some(&b'"') {
                    i += 2;
                    continue;
                }
                double = false;
            }
            i += 1;
            continue;
        }
        match bytes[i] {
            b'\'' => single = true,
            b'"' => double = true,
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                line_comment = true;
                i += 1;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                block_comment = true;
                i += 1;
            }
            b';' => {
                out.push(&sql[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    out.push(&sql[start..]);
    out
}

/// Allowlisted infra / DDL that is not a fact scan.
pub fn is_allowlisted_infra(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("SELECT 1")
        || upper.starts_with("SELECT 1;")
        || upper == "SELECT 1"
        || upper.starts_with("ATTACH ")
        || upper.starts_with("DETACH ")
        || upper.starts_with("INSTALL ")
        || upper.starts_with("LOAD ")
        || upper.starts_with("CALL ")
        || upper.starts_with("SET ")
        || upper.starts_with("PRAGMA ")
        || upper.starts_with("CREATE ")
        || upper.starts_with("ALTER ")
        || upper.starts_with("DROP ")
        || upper.starts_with("COPY ")
        || upper.starts_with("USE ")
        || upper.starts_with("BEGIN")
        || upper.starts_with("COMMIT")
        || upper.starts_with("ROLLBACK")
        || upper.starts_with("CHECKPOINT")
        || upper.starts_with("EXPORT ")
        || upper.starts_with("IMPORT ")
        || upper.starts_with("ANALYZE")
        || upper.starts_with("EXPLAIN")
    {
        return true;
    }
    // DuckLake maintenance helpers
    if upper.contains("DUCKLAKE_") && (upper.starts_with("CALL ") || upper.starts_with("SELECT ")) {
        return true;
    }
    false
}

pub fn names_fact_table(sql: &str) -> bool {
    let lower = code_view(sql).to_ascii_lowercase();
    crate::sql::schema::fact_table_specs().any(|table| {
        let t = table.name;
        lower.contains(&format!(".{t}"))
            || lower.contains(&format!(" {t} "))
            || lower.contains(&format!(" {t}\n"))
            || lower.contains(&format!("from {t}"))
            || lower.contains(&format!("into {t}"))
            || lower.contains(&format!("join {t}"))
            || lower.contains(&format!("update {t}"))
            || lower.contains(&format!("table {t}"))
    })
}

fn has_timestamp_bound(sql: &str) -> bool {
    let lower = code_view(sql).to_ascii_lowercase();
    // Require a real timestamp *predicate*: column (optionally CAST) compared via
    // >= / <= / > / < / BETWEEN. Rejects "SELECT timestamp … WHERE value > 0".
    // Matches both `timestamp >=` and `CAST(timestamp AS TIMESTAMP_NS) >=`.
    let bytes = lower.as_bytes();
    let needle = b"timestamp";
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] != needle {
            i += 1;
            continue;
        }
        // Skip if this is a longer identifier (e.g. end_timestamp, observed_timestamp).
        let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
        let after = i + needle.len();
        let after_ok =
            after >= bytes.len() || !bytes[after].is_ascii_alphanumeric() && bytes[after] != b'_';
        if !(before_ok && after_ok) {
            i += 1;
            continue;
        }
        let rest = &lower[after..];
        let rest = rest.trim_start();
        let rest = if let Some(r) = rest.strip_prefix("as ") {
            // CAST(timestamp AS TIMESTAMP_NS)
            let r = r.trim_start();
            let r = r.trim_start_matches(|c: char| c.is_ascii_alphanumeric() || c == '_');
            r.trim_start().trim_start_matches(')').trim_start()
        } else {
            rest
        };
        if rest.starts_with(">=")
            || rest.starts_with("<=")
            || rest.starts_with('>')
            || rest.starts_with('<')
            || rest.starts_with("between ")
        {
            return true;
        }
        i += 1;
    }
    false
}

/// Reject fact SQL missing a `timestamp` bound; reject forbidden day/clock column names.
pub fn ensure_fact_scan_bound(sql: &str) -> Result<(), String> {
    // Multi-statement batches: check each non-empty statement.
    for stmt in statements(sql) {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        ensure_one(stmt)?;
    }
    Ok(())
}

/// Writer path: `INSERT … FROM read_parquet` / `VALUES` is not an unbound scan.
fn is_external_fact_write(sql: &str) -> bool {
    let upper = code_view(sql).to_ascii_uppercase();
    if !upper.contains("INSERT ") {
        return false;
    }
    if upper.contains("READ_PARQUET") {
        return true;
    }
    // `VALUES (` / `VALUES\n(` / `FROM (VALUES`
    let mut search = upper.as_str();
    while let Some(pos) = search.find("VALUES") {
        let after = &search[pos + "VALUES".len()..];
        if after.trim_start().starts_with('(') {
            return true;
        }
        search = &search[pos + "VALUES".len()..];
    }
    false
}

fn ensure_one(sql: &str) -> Result<(), String> {
    let lower = code_view(sql).to_ascii_lowercase();
    if is_allowlisted_infra(sql) {
        // Still forbid legacy column names even in DDL that shouldn't use them for scans —
        // but CREATE/ALTER defining columns historically might mention them during cutover.
        // After cutover, DDL must not name forbidden columns either when creating fact tables.
        if sql_is_mutating_fact_ddl(sql) {
            for bad in FORBIDDEN_TIME_COLUMNS {
                if lower.contains(bad) {
                    return Err(format!("forbidden time column name `{bad}`"));
                }
            }
        }
        return Ok(());
    }
    for bad in FORBIDDEN_TIME_COLUMNS {
        if lower.contains(bad) {
            return Err(format!("forbidden time column name `{bad}`"));
        }
    }
    if names_fact_table(sql) && !has_timestamp_bound(sql) && !is_external_fact_write(sql) {
        return Err("fact-table SQL missing timestamp bound".into());
    }
    Ok(())
}

fn sql_is_mutating_fact_ddl(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    (upper.starts_with("CREATE ") || upper.starts_with("ALTER ")) && names_fact_table(sql)
}

/// Shared checked `execute_batch` for paths that hold a raw `Connection`.
pub fn execute_batch_checked(conn: &duckdb::Connection, sql: &str) -> anyhow::Result<()> {
    ensure_fact_scan_bound(sql).map_err(anyhow::Error::msg)?;
    conn.execute_batch(sql)
        .map_err(|e| anyhow::anyhow!("execute_batch failed: {e}"))
}

/// Shared checked `prepare` so raw `Connection` callers cannot bypass D12.
pub fn prepare_checked<'a>(
    conn: &'a duckdb::Connection,
    sql: &str,
) -> anyhow::Result<duckdb::Statement<'a>> {
    ensure_fact_scan_bound(sql).map_err(anyhow::Error::msg)?;
    conn.prepare(sql)
        .map_err(|e| anyhow::anyhow!("prepare failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_select_one_and_attach() {
        assert!(ensure_fact_scan_bound("SELECT 1").is_ok());
        assert!(ensure_fact_scan_bound("ATTACH 'x' AS y").is_ok());
        assert!(ensure_fact_scan_bound("CALL ducklake_checkpoint('c')").is_ok());
    }

    #[test]
    fn rejects_unbound_fact_scan() {
        let err = ensure_fact_scan_bound("SELECT * FROM softprobe.traces WHERE session_id = 's'")
            .unwrap_err();
        assert!(err.contains("timestamp bound"), "{err}");
    }

    #[test]
    fn accepts_bound_fact_scan() {
        assert!(ensure_fact_scan_bound(
            "SELECT * FROM softprobe.traces WHERE CAST(timestamp AS TIMESTAMP_NS) >= '2026-09-10'::TIMESTAMP_NS AND CAST(timestamp AS TIMESTAMP_NS) <= '2026-09-11'::TIMESTAMP_NS"
        )
        .is_ok());
    }

    #[test]
    fn rejects_forbidden_time_columns() {
        for bad in ["record_date", "event_date", "window_ts"] {
            let sql = format!(
                "SELECT * FROM softprobe.traces WHERE {bad} = DATE '2026-09-10' AND timestamp >= 'x'"
            );
            let err = ensure_fact_scan_bound(&sql).unwrap_err();
            assert!(err.contains(bad), "{err}");
        }
    }

    #[test]
    fn rejects_uppercase_and_quoted_forbidden_time_columns() {
        for bad in ["RECORD_DATE", "\"Event_Date\"", "\"WINDOW_TS\""] {
            let err = ensure_fact_scan_bound(&format!(
                "SELECT * FROM softprobe.traces WHERE {bad} = DATE '2026-09-10'"
            ))
            .unwrap_err();
            assert!(
                err.to_ascii_lowercase()
                    .contains(&bad.to_ascii_lowercase().replace('"', "")),
                "{err}"
            );
        }
    }

    #[test]
    fn does_not_treat_end_timestamp_as_the_event_time_bound() {
        let err = ensure_fact_scan_bound(
            "SELECT * FROM softprobe.traces WHERE end_timestamp >= '2026-09-10'::TIMESTAMP_NS",
        )
        .unwrap_err();
        assert!(err.contains("timestamp bound"), "{err}");
    }

    #[test]
    fn accepts_one_sided_event_time_windows() {
        assert!(ensure_fact_scan_bound(
            "SELECT * FROM softprobe.logs WHERE timestamp >= '2026-09-10'::TIMESTAMP_NS"
        )
        .is_ok());
        assert!(ensure_fact_scan_bound(
            "SELECT * FROM softprobe.logs WHERE timestamp <= TIMESTAMPTZ '2026-09-11'"
        )
        .is_ok());
    }

    #[test]
    fn ignores_fake_tables_bounds_and_semicolons_inside_literals_or_comments() {
        assert!(
            ensure_fact_scan_bound("SELECT 'FROM softprobe.traces WHERE timestamp >= 0;';").is_ok()
        );
        assert!(ensure_fact_scan_bound(
            "SELECT 1 /* FROM softprobe.traces WHERE timestamp >= 0; */;"
        )
        .is_ok());
        let err = ensure_fact_scan_bound(
            "SELECT * FROM softprobe.traces WHERE note = 'timestamp >= 0;';\
             SELECT * FROM softprobe.logs",
        )
        .unwrap_err();
        assert!(err.contains("timestamp bound"), "{err}");
    }

    #[test]
    fn allows_external_parquet_insert_without_timestamp_bound() {
        assert!(ensure_fact_scan_bound(
            "INSERT INTO softprobe.traces BY NAME SELECT * FROM read_parquet('/tmp/x.parquet');"
        )
        .is_ok());
    }

    #[test]
    fn allows_values_insert_without_timestamp_predicate() {
        assert!(ensure_fact_scan_bound(
            "INSERT INTO softprobe.scores (score_id, name, timestamp)\n\
             SELECT * FROM (VALUES ('s1', 'n', '2026-01-01'::TIMESTAMPTZ));"
        )
        .is_ok());
        assert!(ensure_fact_scan_bound(
            "INSERT INTO softprobe.logs (session_id, timestamp, body)\n\
             VALUES\n('sess', TIMESTAMPTZ '2026-01-01', 'hello');"
        )
        .is_ok());
    }

    #[test]
    fn rejects_timestamp_column_without_time_predicate() {
        let err =
            ensure_fact_scan_bound("SELECT timestamp FROM softprobe.logs WHERE body IS NOT NULL")
                .unwrap_err();
        assert!(err.contains("timestamp bound"), "{err}");
    }

    #[test]
    fn accepts_bare_timestamp_comparison() {
        assert!(ensure_fact_scan_bound(
            "SELECT count(*) FROM softprobe.traces WHERE timestamp >= '2026-01-01' AND timestamp <= '2026-01-02'"
        )
        .is_ok());
    }

    #[test]
    fn gate_detects_every_registry_fact_table() {
        for table in crate::sql::schema::fact_table_specs() {
            assert!(
                names_fact_table(&format!("SELECT * FROM softprobe.{}", table.name)),
                "{} is missing from execute-gate detection",
                table.name
            );
        }
    }

    #[test]
    fn gate_and_registry_have_no_extra_fact_table_owners() {
        for name in ["traces", "logs", "scores"] {
            assert!(crate::sql::schema::table_spec(name).is_some(), "{name}");
        }
        assert!(!names_fact_table("SELECT * FROM softprobe.score_configs"));
    }
}
