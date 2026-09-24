//! Design contract: DuckLake storage must not depend on runtime_engine.
//!
//! Writers hold WorkspaceBinding only; registry/manifest loads stay in ingest/admin.

#[test]
fn ducklake_storage_must_not_import_runtime_engine() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/storage/ducklake");
    let entries = std::fs::read_dir(dir).expect("ducklake dir");
    for entry in entries {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("read ducklake source");
        assert!(
            !src.contains("use crate::runtime_engine") && !src.contains("crate::runtime_engine::"),
            "{} must not import runtime_engine (registry belongs above the writer)",
            path.display()
        );
        assert!(
            !src.contains("scope_bound"),
            "{} must not retain scope_bound dual-API residue",
            path.display()
        );
    }
}

#[test]
fn ingest_pipeline_type_is_deleted() {
    let ingest = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/ingest_engine/mod.rs"
    ));
    assert!(
        !ingest.contains("struct IngestPipeline"),
        "IngestPipeline must be deleted; use IngestEngine::bound / bound_default"
    );
}

#[test]
fn writer_constructor_takes_binding_only() {
    let writer = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/storage/ducklake/writer.rs"
    ));
    assert!(
        writer.contains("pub async fn new(config: &Config, binding: WorkspaceBinding)"),
        "DuckLakeWriter::new must take WorkspaceBinding"
    );
    assert!(
        !writer.contains("pub async fn new(config: &Config, access:"),
        "DuckLakeWriter must not take DuckLakeAccess at construction"
    );
}

#[test]
fn engine_types_do_not_export_writer_or_resolver_fields() {
    let ingest = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/ingest_engine/mod.rs"
    ));
    for needle in [
        "pub writer:",
        "pub resolver:",
        "pub fn writer(",
        "pub fn resolver(",
        "pub fn physical_scope(",
        "pub(crate) fn physical_scope(",
    ] {
        assert!(
            !ingest.contains(needle),
            "IngestEngine/AdminEngine must not export internals via {needle}"
        );
    }

    let writer = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/storage/ducklake/writer.rs"
    ));
    assert!(
        writer.contains("pub(super) fn physical_scope("),
        "DuckLakeWriter::physical_scope must be pub(super) (ducklake-only)"
    );
    assert!(
        !writer.contains("pub(crate) fn physical_scope(")
            && !writer.contains("pub fn physical_scope("),
        "DuckLakeWriter must not expose physical_scope upward to engines"
    );
    assert!(
        writer.contains("pub(crate) fn metadata_schema("),
        "DuckLakeWriter must expose metadata_schema for engine composition"
    );

    let runtime = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/runtime_engine.rs"
    ));
    for needle in [
        "pub binding:",
        "pub catalog_pool:",
        "pub ingest:",
        "pub fn binding(",
        "pub fn catalog_pool(",
        "pub fn physical_scope(",
    ] {
        assert!(
            !runtime.contains(needle),
            "RuntimeEngine must not export internals via {needle}"
        );
    }
}
