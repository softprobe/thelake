//! Ensure canonical product-hot traces promotions when session_summary is enabled.

use crate::promotion::{
    load_active_telemetry_columns_manifests, parse_promotion_manifest, PromotionManifest,
    TelemetryColumnsManifest, TelemetryTable,
};
use crate::runtime_engine::{DuckLakeScope, DuckLakeScopeResolver};
use anyhow::{Context, Result};

/// Canonical Softprobe traces hot-attr manifest (shipped under docs/promotion/).
pub const TRACES_QUERY_HOT_ATTRS_YAML: &str =
    include_str!("../../docs/promotion/traces-query-hot-attrs.yaml");

/// Column names reduce requires (typed / product-hot).
const REQUIRED_TRACES_HOT_COLS: &[&str] = &[
    "observation_type",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "total_cost",
    "user_id",
    "model_name",
];

fn traces_hot_manifest() -> Result<TelemetryColumnsManifest> {
    match parse_promotion_manifest(TRACES_QUERY_HOT_ATTRS_YAML)
        .context("parse traces-query-hot-attrs.yaml")?
    {
        PromotionManifest::TelemetryColumns(m) => Ok(m),
        other => anyhow::bail!("expected telemetry_columns manifest, got {other:?}"),
    }
}

fn active_covers_required(manifests: &[TelemetryColumnsManifest]) -> bool {
    let mut have = std::collections::HashSet::new();
    for m in manifests {
        if !m.target.tables.contains(&TelemetryTable::Traces) {
            continue;
        }
        for c in &m.columns {
            have.insert(c.name.as_str());
        }
    }
    REQUIRED_TRACES_HOT_COLS
        .iter()
        .all(|name| have.contains(name))
}

/// Idempotently activate canonical traces product-hot specs for this tenant schema.
///
/// Columns already exist on the TraceTable Arrow schema; activating the spec makes
/// ingest fill them. Does not invent arbitrary keys (ADR-015).
pub async fn ensure_product_hot_attrs_for_scope(
    resolver: &DuckLakeScopeResolver,
    scope: &DuckLakeScope,
) -> Result<()> {
    let client = resolver.pool().get().await?;
    let active = load_active_telemetry_columns_manifests(&client, &scope.metadata_schema)
        .await
        .map_err(|e| anyhow::anyhow!("load active telemetry promotions: {e}"))?;
    if active_covers_required(&active) {
        return Ok(());
    }
    // Validate shipped yaml still parses before activating.
    let _ = traces_hot_manifest()?;
    let tables = vec!["traces".to_string()];
    resolver
        .record_active_telemetry_promotion_spec(scope, TRACES_QUERY_HOT_ATTRS_YAML, &tables)
        .await
        .context("activate traces-query-hot-attrs for session_summary")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_yaml_parses_and_lists_required_cols() {
        let m = traces_hot_manifest().expect("yaml");
        assert!(m.target.tables.contains(&TelemetryTable::Traces));
        let names: std::collections::HashSet<_> =
            m.columns.iter().map(|c| c.name.as_str()).collect();
        for req in REQUIRED_TRACES_HOT_COLS {
            assert!(names.contains(req), "missing {req}");
        }
    }

    #[test]
    fn active_covers_required_needs_all_cols() {
        let m = traces_hot_manifest().expect("yaml");
        assert!(active_covers_required(&[m]));
        assert!(!active_covers_required(&[]));
    }
}
