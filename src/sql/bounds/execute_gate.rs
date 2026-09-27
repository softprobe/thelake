//! D12: require partition-prunable timestamp predicates on fact scans.

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
    let simple_select_one = code_view(sql)
        .trim()
        .trim_end_matches(';')
        .trim()
        .eq_ignore_ascii_case("SELECT 1");
    if simple_select_one
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
    let words = code_view(sql)
        .replace('"', "")
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    crate::sql::schema::fact_table_specs().any(|table| {
        words
            .iter()
            .any(|word| word.eq_ignore_ascii_case(table.name))
    })
}

/// Check DuckDB's planned physical scans before running a fact-table read.
/// A timestamp filter must reach every traces/logs/scores scan, which catches
/// nested scans, disjunctions, joins, and quoted table names without trying to
/// parse SQL ourselves.
pub(crate) fn ensure_fact_scan_uses_timestamp_pruning(
    conn: &duckdb::Connection,
    sql: &str,
) -> anyhow::Result<()> {
    ensure_fact_scan_uses_timestamp_pruning_with_file_ingest(conn, sql, false)
}

pub(crate) fn ensure_fact_scan_uses_timestamp_pruning_for_ingest(
    conn: &duckdb::Connection,
    sql: &str,
) -> anyhow::Result<()> {
    ensure_fact_scan_uses_timestamp_pruning_with_file_ingest(conn, sql, true)
}

fn ensure_fact_scan_uses_timestamp_pruning_with_file_ingest(
    conn: &duckdb::Connection,
    sql: &str,
    allow_parquet_ingest: bool,
) -> anyhow::Result<()> {
    let statements = statements(sql)
        .into_iter()
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .collect::<Vec<_>>();
    if statements.len() > 1 {
        anyhow::bail!("multiple SQL statements are not supported; send one statement per call");
    }
    for statement in statements {
        ensure_one_fact_scan_uses_timestamp_pruning(conn, statement, allow_parquet_ingest)?;
    }
    Ok(())
}

fn ensure_one_fact_scan_uses_timestamp_pruning(
    conn: &duckdb::Connection,
    sql: &str,
    allow_parquet_ingest: bool,
) -> anyhow::Result<()> {
    let code = code_view(sql);
    let upper = code.to_ascii_uppercase();
    if FORBIDDEN_TIME_COLUMNS
        .iter()
        .any(|forbidden| upper.contains(&forbidden.to_ascii_uppercase()))
    {
        anyhow::bail!("forbidden legacy time column in SQL");
    }
    if upper.trim_start().starts_with("EXPORT ") {
        anyhow::bail!("EXPORT can read every table and is not allowed by the fact-scan gate");
    }
    if upper.trim_start().starts_with("CALL ") {
        if is_allowlisted_ducklake_call(&upper) {
            return Ok(());
        }
        anyhow::bail!("unrecognized CALL cannot be verified for fact-table scans");
    }
    let reads_parquet = upper.contains("READ_PARQUET") || upper.contains("PARQUET_SCAN");
    if reads_parquet && !allow_parquet_ingest {
        anyhow::bail!("direct Parquet scans are not allowed through the query API");
    }
    let Some(query) = query_for_explain(sql) else {
        if names_fact_table(sql) && !is_non_scanning_statement(&upper) {
            anyhow::bail!(
                "unsupported query form references a fact table and cannot be plan-checked"
            );
        }
        return Ok(());
    };

    // EXPLAIN (FORMAT JSON) exposes each physical scan and its pushed filters
    // as separate structured nodes, avoiding fragile associations in the
    // human-readable plan text.
    let explain = format!("EXPLAIN (FORMAT JSON) {query}");
    let mut stmt = conn
        .prepare(&explain)
        .map_err(|e| anyhow::anyhow!("could not plan fact scan: {e}"))?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|e| anyhow::anyhow!("could not read fact scan plan: {e}"))?;
    let plan = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("could not read fact scan plan: {e}"))?
        .join("\n");

    let plan: serde_json::Value = serde_json::from_str(&plan)
        .map_err(|e| anyhow::anyhow!("DuckDB returned an unreadable physical plan: {e}"))?;
    let mut fact_scans = Vec::new();
    collect_fact_scans(&plan, &mut fact_scans);
    for (table, filters) in &fact_scans {
        if !filters
            .as_deref()
            .map(filter_guarantees_timestamp_pruning)
            .unwrap_or(false)
        {
            anyhow::bail!(
                "DuckDB plan scans fact table `{table}` without a conjunctive pushed timestamp filter"
            );
        }
    }
    if has_fact_table_source(sql) && fact_scans.is_empty() {
        anyhow::bail!("DuckDB plan did not expose a fact-table scan to verify");
    }
    Ok(())
}

fn is_allowlisted_ducklake_call(upper: &str) -> bool {
    let Some(call) = upper.trim_start().strip_prefix("CALL ") else {
        return false;
    };
    let function = call
        .split('(')
        .next()
        .unwrap_or_default()
        .trim()
        .rsplit('.')
        .next()
        .unwrap_or_default();
    [
        "DUCKLAKE_MERGE_ADJACENT_FILES",
        "DUCKLAKE_EXPIRE_SNAPSHOTS",
        "DUCKLAKE_CLEANUP_OLD_FILES",
        "DUCKLAKE_DELETE_ORPHANED_FILES",
        "DUCKLAKE_CHECKPOINT",
        "SET_OPTION",
    ]
    .contains(&function)
}

fn is_non_scanning_statement(upper: &str) -> bool {
    [
        "ALTER ",
        "ATTACH ",
        "BEGIN",
        "CHECKPOINT",
        "COMMIT",
        "CREATE ",
        "DETACH ",
        "DESCRIBE ",
        "DROP ",
        "IMPORT ",
        "INSTALL ",
        "LOAD ",
        "PRAGMA ",
        "ROLLBACK",
        "SET ",
        "SHOW ",
        "USE ",
    ]
    .iter()
    .any(|prefix| upper.trim_start().starts_with(prefix))
}

fn query_for_explain(sql: &str) -> Option<&str> {
    let trimmed = sql.trim();
    let code = code_view(trimmed);
    let upper = code.to_ascii_uppercase();
    let query = if upper.trim_start().starts_with("EXPLAIN ") {
        let offset = upper.find("EXPLAIN")? + "EXPLAIN".len();
        let mut rest = trimmed[offset..].trim_start();
        if rest.starts_with('(') {
            let close = rest.find(')')?;
            rest = rest[close + 1..].trim_start();
        } else if rest.to_ascii_uppercase().starts_with("ANALYZE ") {
            rest = rest["ANALYZE".len()..].trim_start();
        }
        rest
    } else {
        trimmed
    };
    let query_code = code_view(query);
    let first_keyword = query_code.split_whitespace().next()?.to_ascii_uppercase();
    if matches!(first_keyword.as_str(), "COPY" | "CREATE" | "ANALYZE") {
        return Some(query);
    }
    [
        "SELECT",
        "WITH",
        "TABLE",
        "FROM",
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "SUMMARIZE",
        "PIVOT",
        "ANALYZE",
    ]
    .contains(&first_keyword.as_str())
    .then_some(query)
}

fn has_fact_table_source(sql: &str) -> bool {
    let lower = code_view(sql).replace('"', "").to_ascii_lowercase();
    crate::sql::schema::fact_table_specs().any(|table| {
        ["from", "join"].iter().any(|keyword| {
            lower.match_indices(keyword).any(|(start, _)| {
                let before_ok = start == 0
                    || !lower.as_bytes()[start - 1].is_ascii_alphanumeric()
                        && lower.as_bytes()[start - 1] != b'_';
                let after_keyword = start + keyword.len();
                let keyword_boundary = after_keyword == lower.len()
                    || !lower.as_bytes()[after_keyword].is_ascii_alphanumeric()
                        && lower.as_bytes()[after_keyword] != b'_';
                if !(before_ok && keyword_boundary) {
                    return false;
                }
                let table_ref = lower[after_keyword..]
                    .trim_start()
                    .split(|c: char| c.is_ascii_whitespace() || matches!(c, ',' | '(' | ')'))
                    .next()
                    .unwrap_or_default();
                table_ref
                    .rsplit('.')
                    .next()
                    .is_some_and(|name| name.eq_ignore_ascii_case(table.name))
            })
        })
    })
}

fn collect_fact_scans<'a>(node: &'a serde_json::Value, out: &mut Vec<(String, Option<&'a str>)>) {
    let Some(object) = node.as_object() else {
        if let Some(children) = node.as_array() {
            for child in children {
                collect_fact_scans(child, out);
            }
        }
        return;
    };
    let name = object
        .get("name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    let extra = object
        .get("extra_info")
        .and_then(serde_json::Value::as_object);
    if name.contains("SCAN") {
        if let Some(table) = extra
            .and_then(|extra| extra.get("Table"))
            .and_then(serde_json::Value::as_str)
        {
            let table = table.rsplit('.').next().unwrap_or(table).trim_matches('"');
            if crate::sql::schema::fact_table_specs()
                .any(|spec| spec.name.eq_ignore_ascii_case(table))
            {
                out.push((
                    table.to_string(),
                    extra
                        .and_then(|extra| extra.get("Filters"))
                        .and_then(serde_json::Value::as_str),
                ));
            }
        }
    }
    if let Some(children) = object.get("children") {
        collect_fact_scans(children, out);
    }
}

fn filter_guarantees_timestamp_pruning(filter: &str) -> bool {
    filter_timestamp_bounds(filter) == 0b11
}

fn filter_timestamp_bounds(filter: &str) -> u8 {
    let filter = strip_outer_parentheses(filter.trim());
    let upper = filter.to_ascii_uppercase();
    if upper.starts_with("NOT ") || upper.starts_with("NOT(") || upper.starts_with('!') {
        return 0;
    }
    if split_top_level_boolean(filter, "OR").is_some() {
        // Any timestamp term in an OR branch can escape the selected window.
        // An identity-only OR remains safe only when another AND adds both bounds.
        return 0;
    }
    if let Some(parts) = split_top_level_boolean(filter, "AND") {
        return parts
            .iter()
            .fold(0, |bounds, part| bounds | filter_timestamp_bounds(part));
    }
    expression_timestamp_bounds(filter)
}

fn expression_timestamp_bounds(expression: &str) -> u8 {
    let lower = code_view(expression).to_ascii_lowercase();
    lower
        .match_indices("timestamp")
        .fold(0, |bounds, (start, _)| {
            let before_ok = start == 0
                || !lower.as_bytes()[start - 1].is_ascii_alphanumeric()
                    && lower.as_bytes()[start - 1] != b'_';
            let end = start + "timestamp".len();
            let after_ok = end == lower.len()
                || !lower.as_bytes()[end].is_ascii_alphanumeric() && lower.as_bytes()[end] != b'_';
            let comparison = lower[end..].trim_start();
            if !(before_ok && after_ok) {
                return bounds;
            }
            if comparison.starts_with("<>") {
                bounds
            } else if comparison.starts_with(">=") || comparison.starts_with('>') {
                bounds | 0b01
            } else if comparison.starts_with("<=") || comparison.starts_with('<') {
                bounds | 0b10
            } else if comparison.starts_with("between ") {
                bounds | 0b11
            } else {
                bounds
            }
        })
}

fn split_top_level_boolean<'a>(expression: &'a str, operator: &str) -> Option<Vec<&'a str>> {
    let code = code_view(expression).to_ascii_uppercase();
    let bytes = code.as_bytes();
    let word = operator.as_bytes();
    let mut depth = 0usize;
    let mut start = 0usize;
    let mut parts = Vec::new();
    let mut i = 0usize;
    while i + word.len() <= bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
        let end = i + word.len();
        let after_ok =
            end == bytes.len() || !bytes[end].is_ascii_alphanumeric() && bytes[end] != b'_';
        if depth == 0 && before_ok && after_ok && &bytes[i..end] == word {
            parts.push(&expression[start..i]);
            start = end;
            i = end;
            continue;
        }
        i += 1;
    }
    if parts.is_empty() {
        None
    } else {
        parts.push(&expression[start..]);
        Some(parts)
    }
}

fn strip_outer_parentheses(mut expression: &str) -> &str {
    loop {
        let trimmed = expression.trim();
        if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
            return trimmed;
        }
        let bytes = code_view(trimmed);
        let mut depth = 0usize;
        let mut wraps_all = true;
        for (index, byte) in bytes.bytes().enumerate() {
            match byte {
                b'(' => depth += 1,
                b')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 && index + 1 < bytes.len() {
                        wraps_all = false;
                        break;
                    }
                }
                _ => {}
            }
        }
        if !wraps_all {
            return trimmed;
        }
        expression = &trimmed[1..trimmed.len() - 1];
    }
}

fn has_bare_timestamp_predicate(sql: &str) -> bool {
    let lower = code_view(sql).to_ascii_lowercase();
    // Partition pruning requires a bare timestamp column comparison. Reject
    // projected timestamps, casts, and function-wrapped forms that scan more files.
    let Some(where_clause_start) = lower
        .match_indices("where")
        .find(|(start, keyword)| {
            let end = start + keyword.len();
            (*start == 0
                || !lower.as_bytes()[start - 1].is_ascii_alphanumeric()
                    && lower.as_bytes()[start - 1] != b'_')
                && (end == lower.len()
                    || !lower.as_bytes()[end].is_ascii_alphanumeric()
                        && lower.as_bytes()[end] != b'_')
        })
        .map(|(start, keyword)| start + keyword.len())
    else {
        return false;
    };
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
        if !(before_ok && after_ok) || i < where_clause_start {
            i += 1;
            continue;
        }
        let rest = lower[after..].trim_start();
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

/// Source-level check that a SQL string contains a bare timestamp comparison.
/// Runtime execution uses [`ensure_fact_scan_uses_timestamp_pruning`] to verify
/// that both sides of the range reach each physical fact-table scan.
pub fn ensure_sql_has_bare_timestamp_predicate(sql: &str) -> Result<(), String> {
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
    if names_fact_table(sql) && !has_bare_timestamp_predicate(sql) && !is_external_fact_write(sql) {
        return Err("fact-table SQL missing bare timestamp predicate for partition pruning".into());
    }
    Ok(())
}

fn sql_is_mutating_fact_ddl(sql: &str) -> bool {
    let upper = sql.trim_start().to_ascii_uppercase();
    (upper.starts_with("CREATE ") || upper.starts_with("ALTER ")) && names_fact_table(sql)
}

/// Shared checked `execute_batch` for paths that hold a raw `Connection`.
pub(crate) fn execute_batch_checked(conn: &duckdb::Connection, sql: &str) -> anyhow::Result<()> {
    ensure_fact_scan_uses_timestamp_pruning(conn, sql)?;
    conn.execute_batch(sql)
        .map_err(|e| anyhow::anyhow!("execute_batch failed: {e}"))
}

/// Writer-only path for its temporary Parquet input. Query APIs never receive
/// this exemption, and logical DuckLake fact scans remain plan-checked.
pub(crate) fn execute_batch_for_parquet_ingest(
    conn: &duckdb::Connection,
    sql: &str,
) -> anyhow::Result<()> {
    ensure_fact_scan_uses_timestamp_pruning_for_ingest(conn, sql)?;
    conn.execute_batch(sql)
        .map_err(|e| anyhow::anyhow!("execute_batch failed: {e}"))
}

/// Shared checked `prepare` so raw `Connection` callers cannot bypass D12.
pub(crate) fn prepare_checked<'a>(
    conn: &'a duckdb::Connection,
    sql: &str,
) -> anyhow::Result<duckdb::Statement<'a>> {
    ensure_fact_scan_uses_timestamp_pruning(conn, sql)?;
    conn.prepare(sql)
        .map_err(|e| anyhow::anyhow!("prepare failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_connection() -> duckdb::Connection {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE traces (timestamp TIMESTAMP_NS, value INTEGER);\
             CREATE TABLE logs (timestamp TIMESTAMP_NS, value INTEGER);\
             CREATE TABLE scores (timestamp TIMESTAMPTZ, value INTEGER);",
        )
        .unwrap();
        conn
    }

    #[test]
    fn explain_gate_accepts_timestamp_filter_pushed_to_fact_scan() {
        let conn = plan_connection();
        let sql = "SELECT * FROM traces WHERE timestamp >= TIMESTAMP_NS '2026-01-01' \
                   AND timestamp < TIMESTAMP_NS '2026-01-02'";
        ensure_fact_scan_uses_timestamp_pruning(&conn, sql).unwrap();
    }

    #[test]
    fn explain_gate_rejects_fact_scan_without_its_own_timestamp_filter() {
        let conn = plan_connection();
        let sql = "SELECT * FROM traces t WHERE EXISTS (\
                   SELECT 1 FROM logs l WHERE l.timestamp >= TIMESTAMP_NS '2026-01-01')";
        let err = ensure_fact_scan_uses_timestamp_pruning(&conn, sql).unwrap_err();
        assert!(err.to_string().contains("traces"), "{err}");
    }

    #[test]
    fn explain_gate_rejects_timestamp_predicate_under_or() {
        let conn = plan_connection();
        let sql = "SELECT * FROM traces WHERE timestamp >= TIMESTAMP_NS '2026-01-01' OR value = 1";
        assert!(ensure_fact_scan_uses_timestamp_pruning(&conn, sql).is_err());
    }

    #[test]
    fn explain_gate_checks_each_fact_scan_in_a_join() {
        let conn = plan_connection();
        let sql = "SELECT * FROM traces t JOIN logs l ON t.value = l.value \
                   WHERE t.timestamp >= TIMESTAMP_NS '2026-01-01'";
        let err = ensure_fact_scan_uses_timestamp_pruning(&conn, sql).unwrap_err();
        assert!(err.to_string().contains("logs"), "{err}");
    }

    #[test]
    fn explain_gate_accepts_filters_on_each_joined_fact_scan() {
        let conn = plan_connection();
        let sql = "SELECT * FROM traces t JOIN logs l ON t.value = l.value \
                   WHERE t.timestamp >= TIMESTAMP_NS '2026-01-01' \
                     AND t.timestamp < TIMESTAMP_NS '2026-01-02' \
                     AND l.timestamp >= TIMESTAMP_NS '2026-01-01' \
                     AND l.timestamp < TIMESTAMP_NS '2026-01-02'";
        ensure_fact_scan_uses_timestamp_pruning(&conn, sql).unwrap();
    }

    #[test]
    fn end_timestamp_does_not_satisfy_partition_window() {
        assert_eq!(expression_timestamp_bounds("end_timestamp >= 'x'"), 0);
    }

    #[test]
    fn timestamp_range_must_be_conjunctive_and_two_sided() {
        assert!(filter_guarantees_timestamp_pruning(
            "timestamp >= 'from' AND timestamp < 'to'"
        ));
        assert!(!filter_guarantees_timestamp_pruning(
            "timestamp >= 'from' OR value = 1"
        ));
        assert!(!filter_guarantees_timestamp_pruning(
            "timestamp >= 'from' AND value = 1"
        ));
        assert!(!filter_guarantees_timestamp_pruning(
            "timestamp >= 'from' AND timestamp <> 'sentinel'"
        ));
        assert!(filter_guarantees_timestamp_pruning(
            "(value = 1 OR value = 2) AND timestamp >= 'from' AND timestamp < 'to'"
        ));
    }

    #[test]
    fn explain_gate_finds_quoted_fact_tables() {
        let conn = plan_connection();
        let err =
            ensure_fact_scan_uses_timestamp_pruning(&conn, "SELECT * FROM \"traces\"").unwrap_err();
        assert!(err.to_string().contains("traces"), "{err}");
    }

    #[test]
    fn explain_gate_checks_fact_scans_in_query_bearing_ddl() {
        let conn = plan_connection();
        let err = ensure_fact_scan_uses_timestamp_pruning(
            &conn,
            "CREATE TABLE copy AS SELECT * FROM traces",
        )
        .unwrap_err();
        assert!(err.to_string().contains("traces"), "{err}");
    }

    #[test]
    fn explain_gate_rejects_multi_statement_batches() {
        let conn = plan_connection();
        let sql = "SELECT * FROM traces WHERE timestamp >= TIMESTAMP_NS '2026-01-01' \
                   AND timestamp < TIMESTAMP_NS '2026-01-02'; \
                   SELECT * FROM logs";
        let err = ensure_fact_scan_uses_timestamp_pruning(&conn, sql).unwrap_err();
        assert!(err.to_string().contains("multiple SQL statements"), "{err}");
    }

    #[test]
    fn explain_gate_rejects_direct_parquet_reads() {
        let conn = plan_connection();
        let err = ensure_fact_scan_uses_timestamp_pruning(
            &conn,
            "SELECT * FROM read_parquet('/warehouse/traces/year=2026/*.parquet')",
        )
        .unwrap_err();
        assert!(err.to_string().contains("direct Parquet scans"), "{err}");
    }

    #[test]
    fn explain_gate_checks_table_relation_in_ctas() {
        let conn = plan_connection();
        let err =
            ensure_fact_scan_uses_timestamp_pruning(&conn, "CREATE TABLE copied AS TABLE traces")
                .unwrap_err();
        assert!(err.to_string().contains("traces"), "{err}");
    }

    #[test]
    fn explain_gate_checks_summarize_and_copy_scans() {
        let conn = plan_connection();
        for sql in [
            "SUMMARIZE traces",
            "COPY traces TO '/tmp/trace-partition-gate.parquet'",
        ] {
            let err = ensure_fact_scan_uses_timestamp_pruning(&conn, sql).unwrap_err();
            assert!(err.to_string().contains("traces"), "{sql}: {err}");
        }
    }

    #[test]
    fn explain_gate_rejects_unrecognized_call() {
        let conn = plan_connection();
        let err = ensure_fact_scan_uses_timestamp_pruning(&conn, "CALL scan_all_tables('traces')")
            .unwrap_err();
        assert!(err.to_string().contains("unrecognized CALL"), "{err}");
        ensure_fact_scan_uses_timestamp_pruning(&conn, "CALL ducklake_checkpoint('c')").unwrap();
    }

    #[test]
    fn explain_gate_rejects_fact_scans_hidden_behind_a_view() {
        let conn = plan_connection();
        conn.execute_batch("CREATE VIEW telemetry_view AS SELECT * FROM traces")
            .unwrap();
        let err = ensure_fact_scan_uses_timestamp_pruning(
            &conn,
            "CREATE TABLE copied AS SELECT * FROM telemetry_view",
        )
        .unwrap_err();
        assert!(err.to_string().contains("traces"), "{err}");
    }

    #[test]
    fn explain_gate_allows_non_fact_ctas() {
        let conn = plan_connection();
        ensure_fact_scan_uses_timestamp_pruning(&conn, "CREATE TABLE tmp AS SELECT 1").unwrap();
    }

    #[test]
    fn allows_select_one_and_attach() {
        assert!(ensure_sql_has_bare_timestamp_predicate("SELECT 1").is_ok());
        assert!(ensure_sql_has_bare_timestamp_predicate("ATTACH 'x' AS y").is_ok());
        assert!(ensure_sql_has_bare_timestamp_predicate("CALL ducklake_checkpoint('c')").is_ok());
    }

    #[test]
    fn health_check_select_cannot_bypass_fact_scan_filter() {
        let err =
            ensure_sql_has_bare_timestamp_predicate("SELECT 1 FROM softprobe.traces").unwrap_err();
        assert!(err.contains("timestamp predicate"), "{err}");
    }

    #[test]
    fn timestamp_comparison_in_projection_does_not_filter_fact_scan() {
        let err = ensure_sql_has_bare_timestamp_predicate(
            "SELECT timestamp >= '2026-09-10'::TIMESTAMP_NS FROM softprobe.traces",
        )
        .unwrap_err();
        assert!(err.contains("timestamp predicate"), "{err}");
    }

    #[test]
    fn rejects_fact_scan_without_timestamp_predicate() {
        let err = ensure_sql_has_bare_timestamp_predicate(
            "SELECT * FROM softprobe.traces WHERE session_id = 's'",
        )
        .unwrap_err();
        assert!(err.contains("timestamp predicate"), "{err}");
    }

    #[test]
    fn rejects_wrapped_timestamp_predicate_that_blocks_partition_pruning() {
        let err = ensure_sql_has_bare_timestamp_predicate(
            "SELECT * FROM softprobe.traces WHERE make_timestamp_ns(epoch_ns(timestamp)) >= '2026-09-10'::TIMESTAMP_NS AND make_timestamp_ns(epoch_ns(timestamp)) <= '2026-09-11'::TIMESTAMP_NS",
        )
        .unwrap_err();
        assert!(err.contains("timestamp predicate"), "{err}");
    }

    #[test]
    fn rejects_forbidden_time_columns() {
        for bad in ["record_date", "event_date", "window_ts"] {
            let sql = format!(
                "SELECT * FROM softprobe.traces WHERE {bad} = DATE '2026-09-10' AND timestamp >= 'x'"
            );
            let err = ensure_sql_has_bare_timestamp_predicate(&sql).unwrap_err();
            assert!(err.contains(bad), "{err}");
        }
    }

    #[test]
    fn rejects_uppercase_and_quoted_forbidden_time_columns() {
        for bad in ["RECORD_DATE", "\"Event_Date\"", "\"WINDOW_TS\""] {
            let err = ensure_sql_has_bare_timestamp_predicate(&format!(
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
    fn does_not_treat_end_timestamp_as_the_event_time_predicate() {
        let err = ensure_sql_has_bare_timestamp_predicate(
            "SELECT * FROM softprobe.traces WHERE end_timestamp >= '2026-09-10'::TIMESTAMP_NS",
        )
        .unwrap_err();
        assert!(err.contains("timestamp predicate"), "{err}");
    }

    #[test]
    fn accepts_one_sided_event_time_windows() {
        assert!(ensure_sql_has_bare_timestamp_predicate(
            "SELECT * FROM softprobe.logs WHERE timestamp >= '2026-09-10'::TIMESTAMP_NS"
        )
        .is_ok());
        assert!(ensure_sql_has_bare_timestamp_predicate(
            "SELECT * FROM softprobe.logs WHERE timestamp <= TIMESTAMPTZ '2026-09-11'"
        )
        .is_ok());
    }

    #[test]
    fn ignores_fake_tables_and_predicates_inside_literals_or_comments() {
        assert!(ensure_sql_has_bare_timestamp_predicate(
            "SELECT 'FROM softprobe.traces WHERE timestamp >= 0;';"
        )
        .is_ok());
        assert!(ensure_sql_has_bare_timestamp_predicate(
            "SELECT 1 /* FROM softprobe.traces WHERE timestamp >= 0; */;"
        )
        .is_ok());
        let err = ensure_sql_has_bare_timestamp_predicate(
            "SELECT * FROM softprobe.traces WHERE note = 'timestamp >= 0;';\
             SELECT * FROM softprobe.logs",
        )
        .unwrap_err();
        assert!(err.contains("timestamp predicate"), "{err}");
    }

    #[test]
    fn allows_external_parquet_insert_without_timestamp_predicate() {
        assert!(ensure_sql_has_bare_timestamp_predicate(
            "INSERT INTO softprobe.traces BY NAME SELECT * FROM read_parquet('/tmp/x.parquet');"
        )
        .is_ok());
    }

    #[test]
    fn allows_values_insert_without_timestamp_predicate() {
        assert!(ensure_sql_has_bare_timestamp_predicate(
            "INSERT INTO softprobe.scores (score_id, name, timestamp)\n\
             SELECT * FROM (VALUES ('s1', 'n', '2026-01-01'::TIMESTAMPTZ));"
        )
        .is_ok());
        assert!(ensure_sql_has_bare_timestamp_predicate(
            "INSERT INTO softprobe.logs (session_id, timestamp, body)\n\
             VALUES\n('sess', TIMESTAMPTZ '2026-01-01', 'hello');"
        )
        .is_ok());
    }

    #[test]
    fn rejects_timestamp_column_without_time_predicate() {
        let err = ensure_sql_has_bare_timestamp_predicate(
            "SELECT timestamp FROM softprobe.logs WHERE body IS NOT NULL",
        )
        .unwrap_err();
        assert!(err.contains("timestamp predicate"), "{err}");
    }

    #[test]
    fn accepts_bare_timestamp_comparison() {
        assert!(ensure_sql_has_bare_timestamp_predicate(
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
