//! Design contract: opaque PhysicalScope public API must not drift.
//!
//! These checks encode the FORBIDDEN list from the PhysicalScope encapsulation
//! plan. Weakening or deleting them to pass CI is itself a contract violation.

#[test]
fn physical_scope_and_config_forbid_public_identity_getters_and_fields() {
    let workspace = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/workspace_scope.rs"
    ));

    let physical_impl = physical_scope_impl_block(workspace)
        .expect("workspace_scope.rs must contain impl PhysicalScope");

    // Config remains a POJO DTO (YAML ingress). Opacity is enforced on PhysicalScope.
    for needle in [
        "pub fn metadata_path(",
        "pub fn metadata_schema(",
        "pub fn data_path(",
        "pub fn catalog_alias(",
        "pub fn new(",
        "pub fn with_pg_namespace(",
        "pub fn with_warehouse_uri(",
        "pub fn with_catalog_dsn(",
        "pub fn with_attach_alias(",
        "fn metadata_path(",
        "fn metadata_schema(",
        "fn data_path(",
        "fn catalog_alias(",
    ] {
        assert!(
            !physical_impl.contains(needle),
            "forbidden identity API on PhysicalScope: {needle}"
        );
    }

    // PhysicalScope must not be embedded in DuckLakeConfig (config stays POJO).
    let config = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/config.rs"));
    assert!(
        !config.contains("physical: crate::workspace_scope::PhysicalScope")
            && !config.contains("physical: PhysicalScope"),
        "DuckLakeConfig must not own PhysicalScope; config is a POJO"
    );
    assert!(
        config.contains("pub metadata_path:")
            && config.contains("pub metadata_schema:")
            && config.contains("pub data_path:")
            && config.contains("pub catalog_alias:"),
        "DuckLakeConfig must keep plain public identity fields for YAML"
    );

    for needle in [
        "pub fn key(",
        "pub fn catalog_prefix(",
        "pub fn qualified_table(",
        "pub fn attach_options(",
        "pub fn pg_table(",
        "pub fn is_main_",
        "pub fn writer_pool_key(",
        "pub fn forbidden_sql_identifiers(",
    ] {
        assert!(
            !physical_impl.contains(needle),
            "forbidden public PhysicalScope helper: {needle}"
        );
    }
}

#[test]
fn tests_must_not_hand_roll_three_string_attach() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut hits = Vec::new();
    visit_rs(&root, &mut |path, contents| {
        if path.ends_with("physical_scope_contract.rs") {
            return;
        }
        for (idx, line) in contents.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.starts_with("//") || trimmed.starts_with("///") {
                continue;
            }
            if trimmed.contains("attach_softprobe_ducklake(")
                || (trimmed.contains("METADATA_SCHEMA") && trimmed.contains("ATTACH"))
                || (trimmed.contains("META_SCHEMA") && trimmed.contains("ATTACH"))
            {
                hits.push(format!("{}:{}:{}", path.display(), idx + 1, trimmed));
            }
        }
    });
    assert!(
        hits.is_empty(),
        "forbidden hand-rolled ATTACH / three-string attach still referenced:\n{}",
        hits.join("\n")
    );
}

#[test]
fn main_namespace_branching_stays_inside_storage_ducklake() {
    let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut hits = Vec::new();
    visit_rs(&src_root, &mut |path, contents| {
        if path.components().any(|c| c.as_os_str() == "ducklake")
            && path.components().any(|c| c.as_os_str() == "storage")
        {
            return;
        }
        // workspace_scope owns is_default_duckdb_namespace implementation.
        if path.ends_with("workspace_scope.rs") {
            return;
        }
        for (idx, line) in contents.lines().enumerate() {
            if line.contains("metadata_schema == \"main\"")
                || line.contains("metadata_schema != \"main\"")
                || line.contains("pg_namespace() == \"main\"")
                || line.contains("pg_namespace() != \"main\"")
            {
                hits.push(format!("{}:{}:{}", path.display(), idx + 1, line.trim()));
            }
        }
    });
    assert!(
        hits.is_empty(),
        "main-namespace branching escaped storage/ducklake:\n{}",
        hits.join("\n")
    );
}

#[test]
fn src_outside_storage_must_not_assemble_metadata_schema_attach_options() {
    let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut hits = Vec::new();
    visit_rs(&src_root, &mut |path, contents| {
        if path.components().any(|c| c.as_os_str() == "ducklake")
            && path.components().any(|c| c.as_os_str() == "storage")
        {
            return;
        }
        for (idx, line) in contents.lines().enumerate() {
            let trimmed = line.trim();
            // ATTACH option assembly only — not env vars like DUCKLAKE_METADATA_SCHEMA
            // or POJO field names like ducklake_metadata_schema / metadata_schema.
            if trimmed.contains("METADATA_SCHEMA '")
                || trimmed.contains("META_SCHEMA '")
                || trimmed.contains("METADATA_SCHEMA \"")
                || trimmed.contains("META_SCHEMA \"")
            {
                hits.push(format!("{}:{}:{}", path.display(), idx + 1, trimmed));
            }
        }
    });
    assert!(
        hits.is_empty(),
        "METADATA_SCHEMA assembly escaped storage/ducklake:\n{}",
        hits.join("\n")
    );
}

#[test]
fn create_query_engine_uses_physical_scope_ingress_not_resolver_pool() {
    let query_mod = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/query/mod.rs"));
    let create_fn = query_mod
        .split("pub async fn create_query_engine")
        .nth(1)
        .and_then(|rest| {
            rest.split("pub(crate) async fn create_query_engine_for_scope")
                .next()
        })
        .expect("create_query_engine body");
    assert!(
        create_fn.contains("physical_scope_from_config"),
        "create_query_engine must take PhysicalScope via physical_scope_from_config"
    );
    assert!(
        !create_fn.contains("DuckLakeScopeResolver::connect"),
        "create_query_engine must not open a registry pool just for default identity"
    );
}

#[test]
fn from_ducklake_is_ingress_only_outside_cfg_test() {
    let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let allowed = [
        std::path::Path::new("runtime_engine.rs"),
        std::path::Path::new("workspace_scope.rs"),
    ];
    let mut hits = Vec::new();
    visit_rs(&src_root, &mut |path, contents| {
        let rel = path.strip_prefix(&src_root).unwrap_or(path);
        if allowed.iter().any(|a| rel.ends_with(a)) {
            return;
        }
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with("tests.rs") || n == "tests.rs")
        {
            return;
        }
        let production = strip_cfg_test_modules(contents);
        for (idx, line) in production.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.starts_with("//") || trimmed.starts_with("///") {
                continue;
            }
            if trimmed.contains("PhysicalScope::from_ducklake") {
                hits.push(format!("{}:{}:{}", path.display(), idx + 1, trimmed));
            }
        }
    });
    assert!(
        hits.is_empty(),
        "PhysicalScope::from_ducklake escaped ingress (runtime_engine / workspace_scope) \
         in non-test production code:\n{}",
        hits.join("\n")
    );
}

#[test]
fn ducklake_qualified_table_name_takes_physical_scope_not_config() {
    let attach = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/storage/ducklake/attach.rs"
    ));
    // Production signature must take PhysicalScope (not DuckLakeConfig).
    assert!(
        attach.contains(
            "pub(crate) fn ducklake_qualified_table_name(scope: &PhysicalScope, bare_table: &str)"
        ),
        "ducklake_qualified_table_name must take &PhysicalScope"
    );
    let production = strip_cfg_test_modules(attach);
    assert!(
        !production.contains("fn ducklake_qualified_table_name(cfg: &DuckLakeConfig")
            && !production.contains("fn ducklake_qualified_table_name(dk: &DuckLakeConfig")
            && !production.contains("fn ducklake_qualified_table_name(config: &DuckLakeConfig"),
        "ducklake_qualified_table_name must not take DuckLakeConfig"
    );
}

/// Drop `#[cfg(test)] mod … { … }` blocks (brace-balanced) so contract scans
/// ignore unit-test helpers that still call ingress constructors.
fn strip_cfg_test_modules(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut out = String::with_capacity(source.len());
    let mut i = 0;
    while i < bytes.len() {
        if let Some(rel) = source[i..].find("#[cfg(test)]") {
            let abs = i + rel;
            out.push_str(&source[i..abs]);
            // Skip attribute + following whitespace to the `mod` / `fn` item.
            let after_attr = abs + "#[cfg(test)]".len();
            let rest = &source[after_attr..];
            let trimmed_start = rest
                .char_indices()
                .find(|(_, c)| !c.is_whitespace())
                .map(|(idx, _)| idx)
                .unwrap_or(0);
            let item = &rest[trimmed_start..];
            if let Some(brace_rel) = item.find('{') {
                let brace_abs = after_attr + trimmed_start + brace_rel;
                let mut depth = 0usize;
                let mut j = brace_abs;
                for (k, ch) in source[brace_abs..].char_indices() {
                    match ch {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                j = brace_abs + k + 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                i = j;
                continue;
            }
            // Attribute without a following brace block — skip the attribute line only.
            i = after_attr;
            continue;
        }
        out.push_str(&source[i..]);
        break;
    }
    out
}

fn visit_rs(dir: &std::path::Path, f: &mut dyn FnMut(&std::path::Path, &str)) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            visit_rs(&path, f);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            if let Ok(contents) = std::fs::read_to_string(&path) {
                f(&path, &contents);
            }
        }
    }
}

/// First `impl PhysicalScope { ... }` block (brace-balanced).
fn physical_scope_impl_block(source: &str) -> Option<&str> {
    let start_pat = "impl PhysicalScope";
    let start = source.find(start_pat)?;
    let after = &source[start..];
    let brace = after.find('{')?;
    let mut depth = 0usize;
    for (i, ch) in after[brace..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&after[..=brace + i]);
                }
            }
            _ => {}
        }
    }
    None
}
