//! Ensure canonical product-hot traces promotions for postgres session_summary.

use crate::api::llm::query::llm_promo;
use crate::promotion::{
    load_active_telemetry_columns_manifests, parse_promotion_manifest, PromotionManifest,
    TelemetryColumnsManifest, TelemetryTable,
};
use crate::runtime_engine::{DuckLakeScope, DuckLakeScopeResolver};
use anyhow::{Context, Result};

/// Canonical Softprobe traces hot-attr manifest (shipped under docs/promotion/).
pub const TRACES_QUERY_HOT_ATTRS_YAML: &str =
    include_str!("../../docs/promotion/traces-query-hot-attrs.yaml");

/// Telemetry table name for activation (must match yaml `tables: [traces]`).
const TRACES_TABLE: &str = "traces";

/// Column names reduce requires — derived from [`llm_promo`], not a second hard-coded list.
fn reduce_required_hot_cols() -> [&'static str; 7] {
    llm_promo().reduce_required_cols()
}

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
    reduce_required_hot_cols()
        .iter()
        .all(|name| have.contains(name))
}

/// Idempotently activate canonical traces product-hot specs for this tenant schema.
///
/// Columns already exist on the TraceTable Arrow schema; activating the spec makes
/// ingest fill them. Does not invent arbitrary keys (ADR-015).
///
/// **Non-goals (Stage 5):** does not promote `sp.agent.name` into `agent_name` (auth /
/// agent observation only), and does not promote `enduser.id` into `user_id`.
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
    let tables = vec![TRACES_TABLE.to_string()];
    resolver
        .record_active_telemetry_promotion_spec(scope, TRACES_QUERY_HOT_ATTRS_YAML, &tables)
        .await
        .context("activate traces-query-hot-attrs for session_summary")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::attr_keys::{enduser, gen_ai, resource, sp};
    use crate::promotion::PromotionSource;

    #[test]
    fn canonical_yaml_covers_reduce_required_cols() {
        let m = traces_hot_manifest().expect("yaml");
        assert!(m.target.tables.contains(&TelemetryTable::Traces));
        let names: std::collections::HashSet<_> =
            m.columns.iter().map(|c| c.name.as_str()).collect();
        for req in reduce_required_hot_cols() {
            assert!(names.contains(req), "missing reduce-required col {req}");
        }
        // Stage 5 non-goal: agent_name is not a yaml promote target.
        assert!(
            !names.contains("agent_name"),
            "agent_name must stay auth/message_type — not in hot-attrs yaml"
        );
    }

    #[test]
    fn yaml_source_keys_match_attr_key_constants() {
        let m = traces_hot_manifest().expect("yaml");
        let by_name: std::collections::HashMap<_, _> =
            m.columns.iter().map(|c| (c.name.as_str(), c)).collect();

        let expect = [
            ("observation_type", sp::OBSERVATION_TYPE),
            ("model_name", gen_ai::REQUEST_MODEL),
            ("model_provider", gen_ai::PROVIDER_NAME),
            ("user_id", sp::USER_ID),
            ("input_tokens", gen_ai::USAGE_INPUT_TOKENS),
            ("output_tokens", gen_ai::USAGE_OUTPUT_TOKENS),
            ("total_tokens", gen_ai::USAGE_TOTAL_TOKENS),
            ("total_cost", sp::COST_TOTAL),
            ("session_attr_id", sp::SESSION_ID),
            ("service_name", resource::SERVICE_NAME),
        ];
        for (col, key) in expect {
            let c = by_name
                .get(col)
                .unwrap_or_else(|| panic!("missing col {col}"));
            match &c.source {
                PromotionSource::Attribute { key: k }
                | PromotionSource::ResourceAttribute { key: k } => {
                    assert_eq!(k.as_str(), key, "col {col}");
                }
                other => panic!("col {col}: unexpected source {other:?}"),
            }
        }
        // Non-goal: bag agent / enduser must not appear as promote sources.
        for c in &m.columns {
            let key = match &c.source {
                PromotionSource::Attribute { key } | PromotionSource::ResourceAttribute { key } => {
                    key.as_str()
                }
                _ => continue,
            };
            assert_ne!(key, sp::AGENT_NAME, "must not promote {}", sp::AGENT_NAME);
            assert_ne!(key, enduser::ID, "must not promote {}", enduser::ID);
        }
    }

    #[test]
    fn active_covers_required_needs_all_cols() {
        let m = traces_hot_manifest().expect("yaml");
        assert!(active_covers_required(std::slice::from_ref(&m)));
        assert!(!active_covers_required(&[]));

        let mut partial = m;
        partial.columns.retain(|c| c.name != llm_promo().user_id);
        assert!(
            !active_covers_required(std::slice::from_ref(&partial)),
            "missing user_id must fail cover check"
        );
    }
}
