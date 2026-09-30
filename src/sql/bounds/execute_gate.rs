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

/// Blank quoted identifiers for clause-keyword recognition. They remain in
/// `code_view` for table-name matching but must not manufacture WHERE/ORDER BY.
fn keyword_view(sql: &str) -> String {
    let mut out = code_view(sql).into_bytes();
    let mut quoted = false;
    let mut i = 0;
    while i < out.len() {
        if quoted {
            if out[i] == b'"' && out.get(i + 1) == Some(&b'"') {
                out[i] = b' ';
                out[i + 1] = b' ';
                i += 2;
                continue;
            }
            if out[i] == b'"' {
                out[i] = b' ';
                quoted = false;
            } else if out[i] != b'\n' {
                out[i] = b' ';
            }
        } else if out[i] == b'"' {
            out[i] = b' ';
            quoted = true;
        }
        i += 1;
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
    let unpruned_scan = fact_scans.iter().find(|(_, filters)| {
        !filters
            .as_deref()
            .map(filter_guarantees_timestamp_pruning)
            .unwrap_or(false)
    });
    if let Some((table, _)) = unpruned_scan {
        // DuckDB can prove a timestamp predicate redundant from file statistics
        // after inlining one bounded fact source into multiple physical scans.
        // The source-local SQL predicate remains required for every fact
        // source whose filter the optimizer removes.
        let optimized_away_bound = has_timestamp_bound_for_each_fact_source(sql);
        if !optimized_away_bound {
            anyhow::bail!(
                "DuckDB plan scans fact table `{table}` without a conjunctive pushed timestamp filter"
            );
        }
    }
    if missing_fact_scan_is_unproven(sql, fact_scans.is_empty(), &plan) {
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

fn is_zero_row_probe(sql: &str) -> bool {
    let code = code_view(sql);
    let upper = code
        .trim()
        .trim_end_matches(';')
        .trim_end()
        .to_ascii_uppercase();
    let words = upper.split_whitespace().collect::<Vec<_>>();
    words.ends_with(&["LIMIT", "0"])
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
    fact_table_source_count(sql) > 0
}

fn fact_table_source_count(sql: &str) -> usize {
    let lower = code_view(sql).replace('"', "").to_ascii_lowercase();
    crate::sql::schema::fact_table_specs()
        .map(|table| {
            ["from", "join"]
                .iter()
                .map(|keyword| {
                    lower
                        .match_indices(keyword)
                        .filter(|(start, _)| {
                            let before_ok = *start == 0
                                || !lower.as_bytes()[*start - 1].is_ascii_alphanumeric()
                                    && lower.as_bytes()[*start - 1] != b'_';
                            let after_keyword = *start + keyword.len();
                            let keyword_boundary = after_keyword == lower.len()
                                || !lower.as_bytes()[after_keyword].is_ascii_alphanumeric()
                                    && lower.as_bytes()[after_keyword] != b'_';
                            if !(before_ok && keyword_boundary) {
                                return false;
                            }
                            let table_ref = lower[after_keyword..]
                                .trim_start()
                                .split(|c: char| {
                                    c.is_ascii_whitespace() || matches!(c, ',' | '(' | ')')
                                })
                                .next()
                                .unwrap_or_default();
                            table_ref
                                .rsplit('.')
                                .next()
                                .is_some_and(|name| name.eq_ignore_ascii_case(table.name))
                        })
                        .count()
                })
                .sum::<usize>()
        })
        .sum()
}

/// DuckDB may remove a safe timestamp filter after proving it redundant from
/// file statistics. In that case, require a source-local bound for every fact
/// table reference, including nested lookups (for example scores by trace).
fn has_timestamp_bound_for_each_fact_source(sql: &str) -> bool {
    let upper = keyword_view(sql).to_ascii_uppercase();
    if upper.trim_start().starts_with("WITH RECURSIVE") {
        return false;
    }
    let sources = fact_table_from_positions(&upper);
    if sources.is_empty() || sources.len() != fact_table_source_count(&upper) {
        // JOIN-based fact sources are deliberately left to physical-plan
        // verification; this source parser only proves isolated FROM scopes.
        return false;
    }
    sources.into_iter().all(|(from, table)| {
        enclosing_query_body(&upper, from)
            .is_some_and(|query| has_isolated_fact_scope_timestamp_bound(query, &table))
    })
}

fn fact_table_from_positions(sql: &str) -> Vec<(usize, String)> {
    let mut sources = Vec::new();
    for (start, _) in sql.match_indices("FROM") {
        let before_ok = start == 0
            || !sql.as_bytes()[start - 1].is_ascii_alphanumeric()
                && sql.as_bytes()[start - 1] != b'_';
        let after = start + "FROM".len();
        let after_ok = after == sql.len()
            || !sql.as_bytes()[after].is_ascii_alphanumeric() && sql.as_bytes()[after] != b'_';
        if !(before_ok && after_ok) {
            continue;
        }
        let name = sql[after..]
            .trim_start()
            .split(|character: char| {
                character.is_ascii_whitespace() || matches!(character, ',' | '(' | ')')
            })
            .next()
            .unwrap_or_default()
            .rsplit('.')
            .next()
            .unwrap_or_default();
        if crate::sql::schema::fact_table_specs().any(|spec| spec.name.eq_ignore_ascii_case(name)) {
            sources.push((start, name.to_ascii_lowercase()));
        }
    }
    sources
}

fn has_isolated_fact_scope_timestamp_bound(query: &str, table: &str) -> bool {
    if !query.trim_start().starts_with("SELECT")
        || top_level_keyword_count(query, "FROM") != 1
        || top_level_keyword_count(query, "JOIN") != 0
        || top_level_keyword_count(query, "UNION") != 0
    {
        return false;
    }
    let Some(from) = find_top_level_keyword(query, "FROM") else {
        return false;
    };
    let source = &query[from + "FROM".len()..];
    let source_end = find_top_level_keyword(source, "WHERE").unwrap_or(source.len());
    let source_ref = source[..source_end].trim();
    if source_ref.contains(',') || source_ref.contains('"') {
        return false;
    }
    let mut source_words = source_ref.split_whitespace();
    let source_name = source_words
        .next()
        .unwrap_or_default()
        .rsplit('.')
        .next()
        .unwrap_or_default();
    if !source_name.eq_ignore_ascii_case(table) {
        return false;
    }
    let source_alias = match source_words.next() {
        Some("AS") => source_words.next().unwrap_or_default(),
        Some(alias) => alias,
        None => table,
    };
    if source_alias.is_empty() || source_words.next().is_some() {
        return false;
    }
    let Some(where_pos) = find_top_level_keyword(source, "WHERE") else {
        return false;
    };
    let predicate = &source[where_pos + "WHERE".len()..];
    let end = ["GROUP BY", "HAVING", "ORDER BY", "LIMIT", "QUALIFY"]
        .iter()
        .filter_map(|clause| find_top_level_keyword(predicate, clause))
        .min()
        .unwrap_or(predicate.len());
    let predicate = &predicate[..end];
    filter_guarantees_timestamp_pruning(predicate)
        && timestamp_references_match_source(predicate, source_alias, table)
}

fn timestamp_references_match_source(predicate: &str, alias: &str, table: &str) -> bool {
    let code = code_view(predicate).to_ascii_lowercase();
    let bytes = code.as_bytes();
    let needle = b"timestamp";
    let mut depth = 0usize;
    let mut index = 0;
    while index + needle.len() <= bytes.len() {
        match bytes[index] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        let end = index + needle.len();
        let before_ok =
            index == 0 || !bytes[index - 1].is_ascii_alphanumeric() && bytes[index - 1] != b'_';
        let after_ok =
            end == bytes.len() || !bytes[end].is_ascii_alphanumeric() && bytes[end] != b'_';
        if depth == 0 && before_ok && after_ok && bytes[index..end] == *needle {
            let mut qualifier_end = index;
            while qualifier_end > 0 && bytes[qualifier_end - 1].is_ascii_whitespace() {
                qualifier_end -= 1;
            }
            if qualifier_end > 0 && bytes[qualifier_end - 1] == b'.' {
                let mut qualifier_start = qualifier_end - 1;
                while qualifier_start > 0
                    && (bytes[qualifier_start - 1].is_ascii_alphanumeric()
                        || matches!(bytes[qualifier_start - 1], b'_' | b'.'))
                {
                    qualifier_start -= 1;
                }
                let qualifier = code[qualifier_start..qualifier_end - 1]
                    .rsplit('.')
                    .next()
                    .unwrap_or_default()
                    .trim();
                if !qualifier.eq_ignore_ascii_case(alias) && !qualifier.eq_ignore_ascii_case(table)
                {
                    return false;
                }
            }
        }
        index += 1;
    }
    true
}

fn top_level_keyword_count(sql: &str, keyword: &str) -> usize {
    let mut count = 0;
    let mut offset = 0;
    while offset < sql.len() {
        let Some(position) = find_top_level_keyword(&sql[offset..], keyword) else {
            break;
        };
        count += 1;
        offset += position + keyword.len();
    }
    count
}

fn enclosing_query_body(sql: &str, from: usize) -> Option<&str> {
    let mut stack = Vec::new();
    for (position, byte) in sql.as_bytes().iter().copied().enumerate().take(from) {
        match byte {
            b'(' => stack.push(position),
            b')' => {
                stack.pop();
            }
            _ => {}
        }
    }
    let Some(open) = stack.last().copied() else {
        let select = sql[..from]
            .match_indices("SELECT")
            .filter(|(start, _)| {
                (*start == 0
                    || !sql.as_bytes()[start - 1].is_ascii_alphanumeric()
                        && sql.as_bytes()[start - 1] != b'_')
                    && (*start + "SELECT".len() == sql.len()
                        || !sql.as_bytes()[*start + "SELECT".len()].is_ascii_alphanumeric()
                            && sql.as_bytes()[*start + "SELECT".len()] != b'_')
            })
            .map(|(start, _)| start)
            .last()?;
        return Some(&sql[select..]);
    };
    let close = matching_close_paren(sql, open)?;
    Some(sql[open + 1..close].trim())
}

fn matching_close_paren(sql: &str, open: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (offset, byte) in sql.as_bytes().iter().copied().enumerate().skip(open) {
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(offset);
                }
            }
            _ => {}
        }
    }
    None
}

fn find_top_level_keyword(sql: &str, keyword: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut start = 0usize;
    while start < bytes.len() {
        match bytes[start] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        let before_ok =
            start == 0 || !bytes[start - 1].is_ascii_alphanumeric() && bytes[start - 1] != b'_';
        if depth == 0 && before_ok {
            let mut position = start;
            let mut matches = true;
            for (index, word) in keyword.split_whitespace().enumerate() {
                if index > 0 {
                    let whitespace_start = position;
                    while position < bytes.len() && bytes[position].is_ascii_whitespace() {
                        position += 1;
                    }
                    if position == whitespace_start {
                        matches = false;
                        break;
                    }
                }
                let word = word.as_bytes();
                if !bytes[position..].starts_with(word) {
                    matches = false;
                    break;
                }
                position += word.len();
            }
            let after_ok = position == bytes.len()
                || !bytes[position].is_ascii_alphanumeric() && bytes[position] != b'_';
            if matches && after_ok {
                return Some(start);
            }
        }
        start += 1;
    }
    None
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

fn is_empty_result_plan(node: &serde_json::Value) -> bool {
    let root = node
        .as_array()
        .and_then(|nodes| (nodes.len() == 1).then(|| &nodes[0]))
        .unwrap_or(node);
    root.as_object()
        .and_then(|object| object.get("name"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|name| name.eq_ignore_ascii_case("EMPTY_RESULT"))
}

fn missing_fact_scan_is_unproven(
    sql: &str,
    scans_are_empty: bool,
    plan: &serde_json::Value,
) -> bool {
    has_fact_table_source(sql)
        && scans_are_empty
        && !is_empty_result_plan(plan)
        && !has_timestamp_bound_for_each_fact_source(sql)
}

fn filter_guarantees_timestamp_pruning(filter: &str) -> bool {
    filter_timestamp_bounds(filter) != 0
}

fn filter_timestamp_bounds(filter: &str) -> u8 {
    let filter = strip_outer_parentheses(filter.trim());
    let upper = filter.to_ascii_uppercase();
    if upper.starts_with("NOT ") || upper.starts_with("NOT(") || upper.starts_with('!') {
        return 0;
    }
    if split_top_level_boolean(filter, "OR").is_some() {
        // Any timestamp term in an OR branch can escape the selected window.
        // An identity-only OR remains safe only when another AND adds a time bound.
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
    let bytes = lower.as_bytes();
    let needle = b"timestamp";
    let mut bounds = 0;
    let mut depth = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 && bytes[i..].starts_with(needle) {
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
            let end = i + needle.len();
            let after_ok =
                end == bytes.len() || !bytes[end].is_ascii_alphanumeric() && bytes[end] != b'_';
            if before_ok && after_ok {
                let comparison = lower[end..].trim_start();
                if comparison.starts_with("<>") {
                    // Not a partition bound.
                } else if comparison.starts_with(">=") || comparison.starts_with('>') {
                    bounds |= 0b01;
                } else if comparison.starts_with("<=") || comparison.starts_with('<') {
                    bounds |= 0b10;
                } else if comparison.starts_with("between ") {
                    bounds |= 0b11;
                }
            }
        }
        i += 1;
    }
    bounds
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
/// that a timestamp bound reaches each physical fact-table scan.
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
    if names_fact_table(sql)
        && !has_bare_timestamp_predicate(sql)
        && !is_external_fact_write(sql)
        && !is_zero_row_probe(sql)
    {
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

/// Execute the repository-owned, fixed maintenance script as one trusted batch.
/// The script is generated from `src/sql/maintenance/maintenance.sql`; it does
/// not accept client SQL and uses only escaped scope identifiers.
pub(crate) fn execute_maintenance_script(
    conn: &duckdb::Connection,
    sql: &str,
) -> anyhow::Result<()> {
    conn.execute_batch(sql)
        .map_err(|error| anyhow::anyhow!("maintenance SQL script failed: {error}"))
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
             CREATE TABLE scores (timestamp TIMESTAMP_NS, value INTEGER);\
             INSERT INTO traces VALUES (TIMESTAMP_NS '2025-01-01', 1), (TIMESTAMP_NS '2027-01-01', 2);\
             INSERT INTO logs VALUES (TIMESTAMP_NS '2025-01-01', 1), (TIMESTAMP_NS '2027-01-01', 2);\
             INSERT INTO scores VALUES (TIMESTAMP_NS '2025-01-01', 1), (TIMESTAMP_NS '2027-01-01', 2);",
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
    fn explain_gate_allows_a_single_bound_proven_redundant_by_statistics() {
        let conn = plan_connection();
        let sql = "SELECT * FROM traces WHERE timestamp >= TIMESTAMP_NS '2020-01-01' \
                   AND timestamp < TIMESTAMP_NS '2030-01-01'";
        ensure_fact_scan_uses_timestamp_pruning(&conn, sql).unwrap();

        let exists = "SELECT EXISTS(SELECT 1 FROM traces WHERE \
                      timestamp >= TIMESTAMP_NS '2020-01-01' AND \
                      timestamp < TIMESTAMP_NS '2030-01-01')";
        ensure_fact_scan_uses_timestamp_pruning(&conn, exists).unwrap();
    }

    #[test]
    fn explain_gate_allows_one_filtered_fact_source_reused_through_ctes() {
        let conn = plan_connection();
        let sql = "WITH base AS (SELECT value, timestamp FROM traces WHERE \
                   timestamp >= TIMESTAMP_NS '2020-01-01' AND \
                   timestamp < TIMESTAMP_NS '2030-01-01'), \
                   matching AS (SELECT value FROM base), \
                   qualified AS (SELECT value FROM base) \
                   SELECT base.value FROM base \
                   JOIN matching USING (value) JOIN qualified USING (value)";
        let mut statement = conn
            .prepare(&format!("EXPLAIN (FORMAT JSON) {sql}"))
            .unwrap();
        let rows = statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap();
        let plan: serde_json::Value =
            serde_json::from_str(&rows.collect::<Result<Vec<_>, _>>().unwrap().join("\n")).unwrap();
        let mut fact_scans = Vec::new();
        collect_fact_scans(&plan, &mut fact_scans);
        assert!(!fact_scans.is_empty());
        assert!(fact_scans.iter().all(|(_, filters)| !filters
            .as_deref()
            .map(filter_guarantees_timestamp_pruning)
            .unwrap_or(false)));
        ensure_fact_scan_uses_timestamp_pruning(&conn, sql).unwrap();

        let lower_bound = "WITH base AS (SELECT timestamp FROM traces WHERE \
                           timestamp >= TIMESTAMP_NS '2020-01-01') SELECT * FROM base";
        let upper_bound = "WITH base AS (SELECT timestamp FROM traces WHERE \
                           timestamp < TIMESTAMP_NS '2030-01-01') SELECT * FROM base";
        assert!(has_timestamp_bound_for_each_fact_source(lower_bound));
        assert!(has_timestamp_bound_for_each_fact_source(upper_bound));
    }

    #[test]
    fn explain_gate_allows_each_bounded_fact_source_when_statistics_elide_filters() {
        let conn = plan_connection();
        let from = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let to = chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        let generated_scores =
            crate::sql::llm::compile_scores_for_trace_sql("trace", from, to).unwrap();
        assert!(has_timestamp_bound_for_each_fact_source(&generated_scores));
        let generated_session =
            crate::sql::llm::compile_session_detail_sql("session", from, to).unwrap();
        assert!(has_timestamp_bound_for_each_fact_source(&generated_session));

        conn.execute_batch(
            "ALTER TABLE traces ADD COLUMN trace_id VARCHAR;\
             ALTER TABLE traces ADD COLUMN span_id VARCHAR;\
             ALTER TABLE scores ADD COLUMN score_id VARCHAR;\
             ALTER TABLE scores ADD COLUMN trace_id VARCHAR;\
             ALTER TABLE scores ADD COLUMN span_id VARCHAR;\
             ALTER TABLE scores ADD COLUMN session_id VARCHAR;\
             ALTER TABLE scores ADD COLUMN name VARCHAR;\
             ALTER TABLE scores ADD COLUMN data_type VARCHAR;\
             ALTER TABLE scores ADD COLUMN numeric_value DOUBLE;\
             ALTER TABLE scores ADD COLUMN string_value VARCHAR;\
             ALTER TABLE scores ADD COLUMN boolean_value BOOLEAN;\
             ALTER TABLE scores ADD COLUMN source VARCHAR;\
             ALTER TABLE scores ADD COLUMN comment VARCHAR;\
             ALTER TABLE scores ADD COLUMN config_id VARCHAR;\
             ALTER TABLE scores ADD COLUMN author_id VARCHAR;\
             ALTER TABLE scores ADD COLUMN metadata JSON;\
             UPDATE traces SET trace_id = 'trace', span_id = 'span';\
             UPDATE scores SET trace_id = 'trace', span_id = 'span';",
        )
        .unwrap();
        let mut statement = conn
            .prepare(&format!("EXPLAIN (FORMAT JSON) {generated_scores}"))
            .unwrap();
        let rows = statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap();
        let plan: serde_json::Value =
            serde_json::from_str(&rows.collect::<Result<Vec<_>, _>>().unwrap().join("\n")).unwrap();
        let mut fact_scans = Vec::new();
        collect_fact_scans(&plan, &mut fact_scans);
        assert!(fact_scans.len() >= 2);
        assert!(fact_scans.iter().all(|(_, filters)| !filters
            .as_deref()
            .map(filter_guarantees_timestamp_pruning)
            .unwrap_or(false)));
        ensure_fact_scan_uses_timestamp_pruning(&conn, &generated_scores).unwrap();

        let sql = "SELECT s.value FROM scores AS s WHERE \
                   s.timestamp >= TIMESTAMP_NS '2020-01-01' AND \
                   s.timestamp < TIMESTAMP_NS '2030-01-01' AND \
                   s.value IN (SELECT t.value FROM traces AS t WHERE \
                     t.timestamp >= TIMESTAMP_NS '2020-01-01' AND \
                     t.timestamp < TIMESTAMP_NS '2030-01-01')";
        ensure_fact_scan_uses_timestamp_pruning(&conn, sql).unwrap();
    }

    #[test]
    fn explain_gate_rejects_a_multi_source_query_with_an_unbounded_fact_source() {
        let conn = plan_connection();
        let sql = "SELECT s.value FROM scores AS s WHERE \
                   s.timestamp >= TIMESTAMP_NS '2020-01-01' AND EXISTS (\
                     SELECT 1 FROM traces AS t WHERE t.value = s.value AND \
                       s.timestamp < TIMESTAMP_NS '2030-01-01')";
        let error = ensure_fact_scan_uses_timestamp_pruning(&conn, sql).unwrap_err();
        assert!(error
            .to_string()
            .contains("without a conjunctive pushed timestamp filter"));
    }

    #[test]
    fn tempo_trace_scan_keeps_its_fact_timestamp_filter_when_cte_stats_elide_it() {
        let conn = plan_connection();
        conn.execute_batch(
            "ALTER TABLE traces ADD COLUMN trace_id VARCHAR;\
             ALTER TABLE traces ADD COLUMN span_id VARCHAR;\
             ALTER TABLE traces ADD COLUMN parent_span_id VARCHAR;\
             ALTER TABLE traces ADD COLUMN message_type VARCHAR;\
             ALTER TABLE traces ADD COLUMN span_kind VARCHAR;\
             ALTER TABLE traces ADD COLUMN app_id VARCHAR;\
             ALTER TABLE traces ADD COLUMN end_timestamp TIMESTAMP_NS;\
             ALTER TABLE traces ADD COLUMN attributes JSON;\
             ALTER TABLE traces ADD COLUMN resource_attributes JSON;\
             ALTER TABLE traces ADD COLUMN instrumentation_scope JSON;\
             ALTER TABLE traces ADD COLUMN links JSON;\
             ALTER TABLE traces ADD COLUMN status_code VARCHAR;\
             ALTER TABLE traces ADD COLUMN status_message VARCHAR;\
             ALTER TABLE traces ADD COLUMN events JSON;\
             ALTER TABLE traces ADD COLUMN observation_type VARCHAR;\
             ALTER TABLE traces ADD COLUMN model_name VARCHAR;\
             ALTER TABLE traces ADD COLUMN model_provider VARCHAR;\
             ALTER TABLE traces ADD COLUMN user_id VARCHAR;\
             ALTER TABLE traces ADD COLUMN session_attr_id VARCHAR;\
             ALTER TABLE traces ADD COLUMN service_name VARCHAR;",
        )
        .unwrap();
        conn.execute_batch("UPDATE traces SET trace_id = 'trace-id'")
            .unwrap();
        let tags = std::collections::BTreeMap::new();
        let start = chrono::DateTime::parse_from_rfc3339("2020-01-01T00:00:00Z")
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();
        let end = chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();
        let sql = crate::sql::tempo::trace_scan_sql(
            crate::sql::tempo::TraceScanParams {
                tags: &tags,
                selector: None,
                min_duration_ns: None,
                max_duration_ns: None,
                start_ns: Some(start),
                end_ns: Some(end),
                limit: 100,
            },
            Some("trace-id"),
        )
        .unwrap();

        assert!(has_timestamp_bound_for_each_fact_source(&sql));
        let mut statement = conn
            .prepare(&format!("EXPLAIN (FORMAT JSON) {sql}"))
            .unwrap();
        let rows = statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap();
        let plan: serde_json::Value =
            serde_json::from_str(&rows.collect::<Result<Vec<_>, _>>().unwrap().join("\n")).unwrap();
        let mut fact_scans = Vec::new();
        collect_fact_scans(&plan, &mut fact_scans);
        assert!(!fact_scans.is_empty());
        assert!(fact_scans.iter().all(|(_, filters)| !filters
            .as_deref()
            .map(filter_guarantees_timestamp_pruning)
            .unwrap_or(false)));
        assert!(
            fact_scans.len() > 1,
            "expected CTE inlining: {fact_scans:?}"
        );
        ensure_fact_scan_uses_timestamp_pruning(&conn, &sql).unwrap();
    }

    #[test]
    fn cte_timestamp_fallback_requires_the_fact_source_where_clause() {
        let unfiltered_fact = "WITH base AS (SELECT timestamp FROM traces), \
                               other AS (SELECT timestamp >= '2020-01-01' AS bounded) \
                               SELECT * FROM base, other";
        let outer_filter = "WITH base AS (SELECT timestamp FROM traces) \
                            SELECT * FROM base WHERE timestamp >= '2020-01-01'";
        let comma_join = "WITH base AS (SELECT traces.timestamp FROM traces, dimensions \
                          WHERE traces.timestamp >= '2020-01-01') SELECT * FROM base";
        let disjunction = "WITH base AS (SELECT timestamp FROM traces WHERE \
                           timestamp >= '2020-01-01' OR value = 1) SELECT * FROM base";
        let comment_only = "WITH base AS (SELECT timestamp FROM traces \
                            /* WHERE timestamp >= '2020-01-01' */) SELECT * FROM base";
        let recursive = "WITH RECURSIVE base AS (SELECT timestamp FROM traces WHERE \
                          timestamp >= '2020-01-01') SELECT * FROM base";
        let nested_fact = "WITH base AS (SELECT timestamp FROM traces WHERE \
                           timestamp >= '2020-01-01' AND EXISTS (SELECT 1 FROM traces)) \
                           SELECT * FROM base";

        for sql in [
            unfiltered_fact,
            outer_filter,
            comma_join,
            disjunction,
            comment_only,
            recursive,
            nested_fact,
        ] {
            assert!(
                !has_timestamp_bound_for_each_fact_source(sql),
                "unsafe CTE fallback accepted: {sql}"
            );
        }
    }

    #[test]
    fn fallback_extracts_the_query_that_owns_the_fact_source() {
        let derived_table = "SELECT * FROM (SELECT timestamp FROM traces WHERE \
                             timestamp >= '2020-01-01') bounded";
        let dedupe_insert = "INSERT INTO scores SELECT incoming.* FROM \
                             (VALUES (1)) incoming(value) WHERE NOT EXISTS (\
                             SELECT 1 FROM scores existing WHERE existing.value = incoming.value \
                             AND existing.timestamp >= TIMESTAMP_NS '2020-01-01')";
        let outer_only = "SELECT * FROM scores WHERE EXISTS (\
                          SELECT timestamp >= TIMESTAMP_NS '2020-01-01')";
        let order_only = "SELECT * FROM scores WHERE value = 1 ORDER BY \
                          timestamp >= TIMESTAMP_NS '2020-01-01'";
        let delimiter_text = "WITH base AS (SELECT timestamp, ')' AS marker FROM traces WHERE \
                              timestamp >= '2020-01-01') SELECT * FROM base";
        let delimiter_comment = "WITH base AS (SELECT timestamp /* ) FROM fake */ FROM traces \
                                WHERE timestamp >= '2020-01-01') SELECT * FROM base";

        assert!(has_timestamp_bound_for_each_fact_source(derived_table));
        assert!(has_timestamp_bound_for_each_fact_source(dedupe_insert));
        assert!(!has_timestamp_bound_for_each_fact_source(outer_only));
        assert!(!has_timestamp_bound_for_each_fact_source(order_only));
        assert!(has_timestamp_bound_for_each_fact_source(delimiter_text));
        assert!(has_timestamp_bound_for_each_fact_source(delimiter_comment));
    }

    #[test]
    fn missing_fact_scan_requires_a_source_timestamp_proof() {
        let bounded = "INSERT INTO scores SELECT incoming.* FROM (VALUES (1)) incoming(value) \
                       WHERE NOT EXISTS (SELECT 1 FROM scores existing \
                       WHERE existing.value = incoming.value AND \
                       existing.timestamp >= TIMESTAMP_NS '2020-01-01')";
        let unbounded = "INSERT INTO scores SELECT incoming.* FROM (VALUES (1)) incoming(value) \
                         WHERE NOT EXISTS (SELECT 1 FROM scores existing \
                         WHERE existing.value = incoming.value)";
        let nonempty_plan = serde_json::json!([{ "name": "INSERT", "children": [] }]);

        assert!(!missing_fact_scan_is_unproven(
            bounded,
            true,
            &nonempty_plan
        ));
        assert!(missing_fact_scan_is_unproven(
            unbounded,
            true,
            &nonempty_plan
        ));
    }

    #[test]
    fn dropped_score_fact_filter_still_requires_a_bounded_source() {
        let conn = plan_connection();
        conn.execute_batch("DELETE FROM scores").unwrap();
        let bounded = "INSERT INTO scores BY NAME SELECT incoming.* FROM \
                       (VALUES (TIMESTAMP_NS '2025-01-01', 1)) incoming(timestamp, value) \
                       WHERE NOT EXISTS (SELECT 1 FROM scores existing \
                       WHERE existing.value = incoming.value AND \
                       existing.timestamp >= TIMESTAMP_NS '2020-01-01' AND \
                       existing.timestamp < TIMESTAMP_NS '2030-01-01')";
        let unbounded = "INSERT INTO scores BY NAME SELECT incoming.* FROM \
                         (VALUES (TIMESTAMP_NS '2025-01-01', 1)) incoming(timestamp, value) \
                         WHERE NOT EXISTS (SELECT 1 FROM scores existing \
                         WHERE existing.value = incoming.value)";

        let mut statement = conn
            .prepare(&format!("EXPLAIN (FORMAT JSON) {bounded}"))
            .unwrap();
        let rows = statement
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap();
        let plan: serde_json::Value =
            serde_json::from_str(&rows.collect::<Result<Vec<_>, _>>().unwrap().join("\n")).unwrap();
        let mut fact_scans = Vec::new();
        collect_fact_scans(&plan, &mut fact_scans);
        assert!(!fact_scans.is_empty());
        assert!(fact_scans.iter().all(|(_, filters)| !filters
            .as_deref()
            .map(filter_guarantees_timestamp_pruning)
            .unwrap_or(false)));

        ensure_fact_scan_uses_timestamp_pruning(&conn, bounded).unwrap();
        assert!(ensure_fact_scan_uses_timestamp_pruning(&conn, unbounded).is_err());
    }

    #[test]
    fn explain_gate_does_not_apply_elided_bound_to_joined_or_disjunctive_scans() {
        let conn = plan_connection();
        conn.execute_batch(
            "CREATE TABLE dimensions (id INTEGER, timestamp TIMESTAMP_NS);\
             INSERT INTO dimensions VALUES (1, TIMESTAMP_NS '2025-01-01');",
        )
        .unwrap();
        let joined = "SELECT t.* FROM traces t JOIN dimensions d ON d.id = t.value \
                      WHERE d.timestamp >= TIMESTAMP_NS '2020-01-01'";
        assert!(ensure_fact_scan_uses_timestamp_pruning(&conn, joined).is_err());

        let nested = "SELECT * FROM traces t WHERE EXISTS (\
                      SELECT 1 FROM dimensions d WHERE d.timestamp >= TIMESTAMP_NS '2020-01-01')";
        assert!(ensure_fact_scan_uses_timestamp_pruning(&conn, nested).is_err());

        let disjunctive = "SELECT * FROM traces WHERE timestamp >= TIMESTAMP_NS '2020-01-01' OR\n\
                            value = 1";
        assert!(ensure_fact_scan_uses_timestamp_pruning(&conn, disjunctive).is_err());

        let ordering = "SELECT * FROM traces WHERE value = 1 ORDER BY \
                        timestamp >= TIMESTAMP_NS '2020-01-01'";
        assert!(ensure_fact_scan_uses_timestamp_pruning(&conn, ordering).is_err());

        let multiline_ordering = "SELECT * FROM traces WHERE value = 1\nORDER\nBY \
                                timestamp >= TIMESTAMP_NS '2020-01-01'";
        assert!(ensure_fact_scan_uses_timestamp_pruning(&conn, multiline_ordering).is_err());

        let quoted_alias = "SELECT * FROM traces AS \"WHERE timestamp >= 1\" \
                            WHERE value = 1";
        assert!(ensure_fact_scan_uses_timestamp_pruning(&conn, quoted_alias).is_err());

        let unrelated_exists = "SELECT * FROM traces WHERE value = 1 AND \
                                EXISTS (SELECT timestamp >= TIMESTAMP_NS '2020-01-01')";
        assert!(ensure_fact_scan_uses_timestamp_pruning(&conn, unrelated_exists).is_err());
    }

    #[test]
    fn explain_gate_allows_zero_row_fact_probe() {
        let conn = plan_connection();
        ensure_fact_scan_uses_timestamp_pruning(&conn, "SELECT 1 FROM traces LIMIT 0").unwrap();
    }

    #[test]
    fn explain_gate_accepts_empty_fact_source_with_its_own_timestamp_filter() {
        let conn = plan_connection();
        conn.execute_batch("DELETE FROM scores").unwrap();
        let sql = "INSERT INTO scores SELECT * FROM scores WHERE \
                   timestamp >= TIMESTAMP_NS '2020-01-01'";
        ensure_fact_scan_uses_timestamp_pruning(&conn, sql).unwrap();
    }

    #[test]
    fn explain_gate_rejects_fact_scan_without_its_own_timestamp_filter() {
        let conn = plan_connection();
        let sql = "SELECT * FROM traces t WHERE EXISTS (\
                   SELECT 1 FROM logs l WHERE l.timestamp >= TIMESTAMP_NS '2026-01-01')";
        let err = ensure_fact_scan_uses_timestamp_pruning(&conn, sql).unwrap_err();
        assert!(err.to_string().contains("traces"), "{err}");

        conn.execute_batch(
            "CREATE TABLE dimensions (value INTEGER, timestamp TIMESTAMP_NS);\
             INSERT INTO dimensions VALUES (1, TIMESTAMP_NS '2025-01-01');",
        )
        .unwrap();
        let correlated = "SELECT 1 FROM dimensions d WHERE EXISTS (\
                          SELECT 1 FROM traces t WHERE t.value = d.value AND \
                            d.timestamp >= TIMESTAMP_NS '2020-01-01')";
        let err = ensure_fact_scan_uses_timestamp_pruning(&conn, correlated).unwrap_err();
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
    fn timestamp_bound_must_be_conjunctive() {
        assert!(filter_guarantees_timestamp_pruning(
            "timestamp >= 'from' AND timestamp < 'to'"
        ));
        assert!(filter_guarantees_timestamp_pruning("timestamp >= 'from'"));
        assert!(!filter_guarantees_timestamp_pruning(
            "timestamp >= 'from' OR value = 1"
        ));
        assert!(filter_guarantees_timestamp_pruning(
            "timestamp >= 'from' AND value = 1"
        ));
        assert!(filter_guarantees_timestamp_pruning(
            "timestamp >= 'from' AND timestamp <> 'sentinel'"
        ));
        assert!(filter_guarantees_timestamp_pruning(
            "(value = 1 OR value = 2) AND timestamp >= 'from' AND timestamp < 'to'"
        ));
    }

    #[test]
    fn empty_result_requires_the_entire_plan_to_be_empty() {
        assert!(is_empty_result_plan(&serde_json::json!([
            { "name": "EMPTY_RESULT", "children": [] }
        ])));
        assert!(!is_empty_result_plan(&serde_json::json!([
            {
                "name": "UNION",
                "children": [
                    { "name": "EMPTY_RESULT", "children": [] },
                    { "name": "SEQ_SCAN", "children": [] }
                ]
            }
        ])));
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
        assert!(is_zero_row_probe("SELECT 1 FROM softprobe.traces LIMIT 0;"));
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
            "SELECT * FROM softprobe.logs WHERE timestamp <= TIMESTAMP_NS '2026-09-11'"
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
             SELECT * FROM (VALUES ('s1', 'n', '2026-01-01'::TIMESTAMP_NS));"
        )
        .is_ok());
        assert!(ensure_sql_has_bare_timestamp_predicate(
            "INSERT INTO softprobe.logs (session_id, timestamp, body)\n\
             VALUES\n('sess', TIMESTAMP_NS '2026-01-01', 'hello');"
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
