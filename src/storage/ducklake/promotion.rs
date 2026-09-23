//! Promotion specs store and DuckLakeWriter apply/load methods.
//!
//! Specs persist as `{catalog_alias}.promotion_specs` through the writer's attached DuckDB
//! connection. Apply is serialized under the tenant DuckLake resolver's Postgres advisory lock
//! (`DuckLakeScopeResolver::apply_telemetry_promotion_guarded` /
//! `apply_business_promotion_guarded`), which the normal runtime path always has.

use crate::promotion::{
    business_table_create_ddls, telemetry_column_add_ddls, telemetry_manifest_from_row,
    BusinessApplyError, BusinessTableManifest, PromotionSpecLoadError, TelemetryColumnsManifest,
};
use crate::workspace_scope::PhysicalScope;
use anyhow::{anyhow, Result};
use duckdb::Connection;

use super::util::quote_duckdb_ident;
use super::DuckLakeWriter;

fn table_missing(err: &duckdb::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("does not exist") || msg.contains("not found") || msg.contains("catalog error")
}

/// Load active telemetry manifests from the local DuckLake catalog.
///
/// A missing `promotion_specs` table means no promotions have been applied yet — returns empty.
pub(super) fn load_active_telemetry_manifests(
    conn: &Connection,
    catalog_alias: &str,
) -> Result<Vec<TelemetryColumnsManifest>, PromotionSpecLoadError> {
    let catalog = quote_duckdb_ident(catalog_alias);
    let sql = format!(
        "SELECT spec_id, manifest_json FROM {catalog}.promotion_specs \
WHERE status = 'active' AND target_kind = 'telemetry_columns';"
    );
    let mut stmt = match conn.prepare(&sql) {
        Ok(s) => s,
        Err(err) if table_missing(&err) => return Ok(Vec::new()),
        Err(err) => return Err(PromotionSpecLoadError::Backend(err.to_string())),
    };
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|err| PromotionSpecLoadError::Backend(err.to_string()))?;
    let mut out = Vec::new();
    for row in rows {
        let (spec_id, manifest_json) =
            row.map_err(|err| PromotionSpecLoadError::Backend(err.to_string()))?;
        if let Some(m) = telemetry_manifest_from_row(&spec_id, &manifest_json)? {
            out.push(m);
        }
    }
    Ok(out)
}

impl DuckLakeWriter {
    /// Apply telemetry DDL and activate the spec under the Postgres advisory
    /// lock held by the tenant DuckLake resolver, so concurrent applies cannot
    /// interleave DDL with a different manifest's activation. The normal
    /// runtime path always has a resolver (`DuckLakeScopeResolver::connect`
    /// always connects).
    pub async fn apply_and_record_telemetry_promotion(
        &self,
        scope: &PhysicalScope,
        manifest_yaml: &str,
        spec: &TelemetryColumnsManifest,
        target_tables: &[String],
    ) -> Result<String> {
        let resolver = &self.tenant_ducklake;
        resolver
            .apply_telemetry_promotion_guarded(scope, manifest_yaml, target_tables, || async {
                self.apply_telemetry_column_promotion(scope, spec)
                    .await
                    .map(|_| ())
            })
            .await
    }

    /// Guarded business-table apply (load → validate → DDL → record) under the
    /// tenant DuckLake resolver's Postgres advisory lock. The normal runtime
    /// path always has a resolver (`DuckLakeScopeResolver::connect` always
    /// connects).
    pub async fn apply_business_promotion_guarded(
        &self,
        scope: &PhysicalScope,
        manifest_yaml: &str,
        spec: &BusinessTableManifest,
    ) -> std::result::Result<String, BusinessApplyError> {
        let resolver = &self.tenant_ducklake;
        resolver
            .apply_business_promotion_guarded(scope, manifest_yaml, spec, || async {
                self.apply_business_table_promotion(scope, spec)
                    .await
                    .map(|_| ())
            })
            .await
    }

    /// Apply additive telemetry promotion DDL inside one tenant DuckLake scope.
    ///
    /// `promotion apply` owns schema changes for promoted telemetry columns. It first materializes
    /// the hardcoded canonical telemetry tables if they do not exist, then runs the nullable
    /// `ALTER TABLE ADD COLUMN IF NOT EXISTS` statements generated from the tenant manifest.
    pub async fn apply_telemetry_column_promotion(
        &self,
        scope: &PhysicalScope,
        spec: &TelemetryColumnsManifest,
    ) -> Result<Vec<String>> {
        let dk = self.effective_ducklake(scope);
        for table in &spec.target.tables {
            self.ensure_telemetry_table_for(&dk, table).await?;
        }
        let ddls = self.with_attached_conn(&dk, |conn| {
            let prefix = if dk.metadata_schema == "main" {
                dk.catalog_alias.clone()
            } else {
                format!(
                    "{}.{}",
                    quote_duckdb_ident(&dk.catalog_alias),
                    quote_duckdb_ident(&dk.metadata_schema)
                )
            };
            let ddls = telemetry_column_add_ddls(&prefix, spec)
                .map_err(|err| anyhow!("telemetry promotion validation failed: {err}"))?;
            for ddl in &ddls {
                conn.execute_batch(ddl)?;
            }
            Ok(ddls)
        })?;
        Ok(ddls)
    }

    /// Apply generated business table DDL inside one tenant DuckLake scope.
    ///
    /// Business promotion manifests own the physical table and current view. The runtime executes
    /// generated DDL in order so agents do not need to write tenant-specific `CREATE TABLE` SQL.
    pub async fn apply_business_table_promotion(
        &self,
        scope: &PhysicalScope,
        spec: &BusinessTableManifest,
    ) -> Result<Vec<String>> {
        let dk = self.effective_ducklake(scope);
        let ddls = self.with_attached_conn(&dk, |conn| {
            // Prefer catalog.schema when metadata lives outside `main`; fall back to catalog-only
            // (matches write-path table name candidates when ATTACH uses METADATA_SCHEMA).
            let prefixes = if dk.metadata_schema == "main" {
                vec![dk.catalog_alias.clone()]
            } else {
                vec![
                    format!(
                        "{}.{}",
                        quote_duckdb_ident(&dk.catalog_alias),
                        quote_duckdb_ident(&dk.metadata_schema)
                    ),
                    dk.catalog_alias.clone(),
                ]
            };
            let mut last_err: Option<anyhow::Error> = None;
            for prefix in prefixes {
                let ddls = business_table_create_ddls(&prefix, spec)
                    .map_err(|err| anyhow!("business table promotion validation failed: {err}"))?;
                match ddls
                    .iter()
                    .try_for_each(|ddl| conn.execute_batch(ddl).map(|_| ()))
                {
                    Ok(()) => return Ok(ddls),
                    Err(err) => {
                        last_err = Some(anyhow!(
                            "business table promotion failed with prefix {prefix}: {err}"
                        ));
                    }
                }
            }
            Err(last_err.unwrap_or_else(|| anyhow!("business table promotion failed")))
        })?;
        Ok(ddls)
    }
}
