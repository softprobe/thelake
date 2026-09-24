//! Design contract: opaque PhysicalScope public API must not drift.
//!
//! These checks encode the FORBIDDEN list from the PhysicalScope encapsulation
//! plan. Weakening or deleting them to pass CI is itself a contract violation.

#[test]
fn physical_scope_type_is_crate_internal() {
    let workspace = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/workspace_scope.rs"
    ));
    assert!(
        workspace.contains("pub(crate) struct PhysicalScope"),
        "PhysicalScope must be pub(crate), not a public product type"
    );
    assert!(
        !workspace.contains("pub struct PhysicalScope"),
        "PhysicalScope must not be pub struct"
    );
    assert!(
        workspace.contains("pub(crate) struct ScopeId"),
        "ScopeId must be pub(crate)"
    );
    assert!(
        workspace.contains("pub(crate) enum DuckLakeAccess"),
        "DuckLakeAccess must be pub(crate)"
    );
    assert!(
        workspace.contains("pub(crate) fn from_ducklake("),
        "from_ducklake must be pub(crate) ingress only"
    );
    assert!(
        workspace.contains("pub(crate) fn new(")
            && workspace
                .split("impl WorkspaceBinding")
                .nth(1)
                .expect("WorkspaceBinding impl")
                .contains("pub(crate) fn new("),
        "WorkspaceBinding::new must be pub(crate) (takes PhysicalScope)"
    );

    let lib = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs"));
    assert!(
        !lib.contains("pub use") || !lib.contains("PhysicalScope"),
        "lib.rs must not pub-use PhysicalScope"
    );
    // Module is public for WorkspaceScopeMode / binding errors, but PhysicalScope
    // itself must not be re-exported.
    assert!(
        !lib.contains("pub use crate::workspace_scope::PhysicalScope")
            && !lib.contains("pub use workspace_scope::PhysicalScope"),
        "PhysicalScope must not be re-exported from lib.rs"
    );
}

#[test]
fn handlers_must_not_import_physical_scope() {
    let roots = [
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/api"),
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/compat"),
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/runtime_api.rs"),
    ];
    let mut hits = Vec::new();
    for root in &roots {
        if root.is_file() {
            if let Ok(contents) = std::fs::read_to_string(root) {
                let production = strip_cfg_test_modules(&contents);
                for (idx, line) in production.lines().enumerate() {
                    let trimmed = line.trim();
                    if trimmed.starts_with("//") || trimmed.starts_with("///") {
                        continue;
                    }
                    if trimmed.contains("PhysicalScope")
                        || trimmed.contains("DuckLakeAccess")
                        || trimmed.contains("ScopeId")
                    {
                        hits.push(format!("{}:{}: {}", root.display(), idx + 1, trimmed));
                    }
                }
            }
            continue;
        }
        visit_rs(root, &mut |path, contents| {
            let production = strip_cfg_test_modules(contents);
            for (idx, line) in production.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.starts_with("//") || trimmed.starts_with("///") {
                    continue;
                }
                if trimmed.contains("PhysicalScope")
                    || trimmed.contains("DuckLakeAccess")
                    || trimmed.contains("ScopeId")
                {
                    hits.push(format!("{}:{}: {}", path.display(), idx + 1, trimmed));
                }
            }
        });
    }
    assert!(
        hits.is_empty(),
        "handlers must not name PhysicalScope/DuckLakeAccess/ScopeId:\n{}",
        hits.join("\n")
    );
}

#[test]
fn integration_tests_must_not_name_physical_scope() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut hits = Vec::new();
    visit_rs(&root, &mut |path, contents| {
        // Design contracts document the type by name.
        if path.components().any(|c| c.as_os_str() == "design") {
            return;
        }
        for (idx, line) in contents.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.starts_with("//") || trimmed.starts_with("///") {
                continue;
            }
            if trimmed.contains("workspace_scope::PhysicalScope")
                || trimmed.contains("PhysicalScope::")
                || (trimmed.contains("use ") && trimmed.contains("PhysicalScope"))
            {
                hits.push(format!("{}:{}: {}", path.display(), idx + 1, trimmed));
            }
        }
    });
    assert!(
        hits.is_empty(),
        "integration tests must use attach façades, not PhysicalScope:\n{}",
        hits.join("\n")
    );
}

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
    let mut hits = Vec::new();
    visit_rs(&src_root, &mut |path, contents| {
        let rel = path.strip_prefix(&src_root).unwrap_or(path);
        // Allowed config→scope ingress sites only.
        if rel.ends_with("runtime_engine.rs")
            || rel.ends_with("workspace_scope.rs")
            || rel == std::path::Path::new("storage/ducklake/attach.rs")
        {
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
        "PhysicalScope::from_ducklake escaped ingress (runtime_engine / workspace_scope / \
         storage/ducklake/attach) in non-test production code:\n{}",
        hits.join("\n")
    );
}

#[test]
fn api_and_compat_must_not_reach_engine_internals() {
    let roots = [
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/api"),
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/compat"),
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/runtime_api.rs"),
    ];
    let forbidden = [
        ".physical_scope(",
        ".pool()",
        "scope_registry(",
        "MaintenanceScope",
        "engines.resolve_scope(",
        ".resolve_scope(",
        "DuckLakeScopeResolver",
        "TenantSummaryScope",
        "session_summary_scope",
        "binding.physical_scope",
        "pub physical_scope:",
        ".pg_namespace()",
        ".warehouse_uri()",
        ".catalog_dsn()",
        "PhysicalScope::",
        "use crate::workspace_scope::PhysicalScope",
    ];
    let mut hits = Vec::new();
    for root in &roots {
        if root.is_file() {
            if let Ok(contents) = std::fs::read_to_string(root) {
                let production = strip_cfg_test_modules(&contents);
                for (idx, line) in production.lines().enumerate() {
                    let trimmed = line.trim();
                    if trimmed.starts_with("//") || trimmed.starts_with("///") {
                        continue;
                    }
                    for needle in forbidden {
                        if trimmed.contains(needle) {
                            hits.push(format!("{}:{}: {}", root.display(), idx + 1, trimmed));
                        }
                    }
                }
            }
            continue;
        }
        visit_rs(root, &mut |path, contents| {
            let production = strip_cfg_test_modules(contents);
            for (idx, line) in production.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.starts_with("//") || trimmed.starts_with("///") {
                    continue;
                }
                for needle in forbidden {
                    if trimmed.contains(needle) {
                        hits.push(format!("{}:{}: {}", path.display(), idx + 1, trimmed));
                    }
                }
            }
        });
    }
    assert!(
        hits.is_empty(),
        "handlers must use engine façades only (no binding/physical/pool codecs):\n{}",
        hits.join("\n")
    );
}

#[test]
fn workspace_binding_does_not_expose_physical_scope() {
    let workspace = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/workspace_scope.rs"
    ));
    assert!(
        !workspace.contains("pub physical_scope: PhysicalScope"),
        "WorkspaceBinding.physical_scope must not be a public field"
    );
    // Same-module DuckLakeAccess may read the private field; Binding must not
    // hand out PhysicalScope via a getter (that re-opens the leak).
    let binding_impl = workspace
        .split("impl WorkspaceBinding")
        .nth(1)
        .and_then(|rest| rest.split("impl DuckLakeAccess").next())
        .expect("WorkspaceBinding impl");
    assert!(
        !binding_impl.contains("fn physical_scope("),
        "WorkspaceBinding must not expose a physical_scope getter"
    );
    assert!(
        binding_impl.contains("fn registry_lock_token("),
        "WorkspaceBinding may expose opaque registry_lock_token only"
    );
}

#[test]
fn manager_provision_returns_storage_hints_not_physical_scope() {
    let runtime = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_engine.rs"
    ));
    // Manager façade signature (not DuckLakeScopeResolver::provision_scope).
    let manager_impl = runtime
        .split("impl RuntimeEngineManager")
        .nth(1)
        .expect("RuntimeEngineManager impl");
    assert!(
        manager_impl.contains("pub async fn provision_scope(")
            && manager_impl.contains("Result<ScopeStorageHints>"),
        "RuntimeEngineManager::provision_scope must return ScopeStorageHints"
    );
    assert!(
        manager_impl.contains("pub async fn scope_storage_hints("),
        "RuntimeEngineManager must expose scope_storage_hints"
    );
    assert!(
        manager_impl.contains("pub fn lease_store(")
            && manager_impl.contains("pub async fn maintenance_engine("),
        "RuntimeEngineManager must expose lease_store and maintenance_engine"
    );
    assert!(
        !manager_impl.contains("fn scope_registry("),
        "RuntimeEngineManager must not expose scope_registry"
    );
    assert!(
        !manager_impl
            .split("pub async fn provision_scope(")
            .nth(1)
            .and_then(|rest| rest.split("pub async fn").next())
            .expect("provision_scope body")
            .contains("Result<PhysicalScope>"),
        "RuntimeEngineManager::provision_scope must not return PhysicalScope"
    );
}

#[test]
fn api_llm_query_uses_runtime_engine_summary_facade() {
    let query = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/api/llm/query.rs"));
    for needle in [
        "session_summary_scope",
        "TenantSummaryScope",
        "summary_scope.physical",
        ".physical.pg_namespace",
        "lookup_session_summary_window_for_workspace",
        "search_session_summary_for_workspace",
    ] {
        assert!(
            !query.contains(needle),
            "api/llm/query.rs must use RuntimeEngine summary methods only (found {needle})"
        );
    }
    assert!(
        query.contains("lookup_session_summary_window"),
        "api/llm/query.rs must call RuntimeEngine::lookup_session_summary_window"
    );
    assert!(
        query.contains("search_session_summary"),
        "api/llm/query.rs must call RuntimeEngine::search_session_summary"
    );

    let runtime = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_engine.rs"
    ));
    assert!(
        runtime.contains("struct TenantSummaryScope {"),
        "TenantSummaryScope must remain a private helper inside runtime_engine"
    );
    assert!(
        !runtime.contains("pub(crate) struct TenantSummaryScope")
            && !runtime.contains("pub struct TenantSummaryScope"),
        "TenantSummaryScope must not be exported"
    );
    assert!(
        !runtime.contains("pub(crate) fn session_summary_scope"),
        "session_summary_scope must stay private to RuntimeEngine"
    );
}

#[test]
fn leased_jobs_must_not_import_physical_scope() {
    let roots = [
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/compaction/maintenance_job.rs"
        ),
        concat!(env!("CARGO_MANIFEST_DIR"), "/src/session_summary/job.rs"),
    ];
    for path in roots {
        let src = std::fs::read_to_string(path).expect("read job source");
        let production = src.split("#[cfg(test)]").next().unwrap_or(&src);
        for needle in [
            "use crate::workspace_scope::PhysicalScope",
            "PhysicalScope::",
            ": PhysicalScope",
            "&PhysicalScope",
            "DuckLakeScopeResolver",
            "default_physical_scope",
            ".pool()",
        ] {
            assert!(
                !production.contains(needle),
                "{path} must use engine key façades only (found {needle})"
            );
        }
    }
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
