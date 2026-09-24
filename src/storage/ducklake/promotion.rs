//! Promotion specs store and DuckLakeWriter apply/load methods.
//!
//! Specs persist in the physical scope's Postgres DuckLake metadata schema. Apply is serialized
//! under the tenant DuckLake resolver's Postgres advisory lock
//! (`DuckLakeScopeResolver::apply_telemetry_promotion_guarded` /
//! `apply_business_promotion_guarded`), which the normal runtime path always has.

use super::util::quote_duckdb_ident;
use super::DuckLakeWriter;
use crate::promotion::{
    business_table_create_ddls, telemetry_column_add_ddls, BusinessApplyError,
    BusinessTableManifest, TelemetryColumnsManifest,
};
use crate::workspace_scope::PhysicalScope;
use anyhow::{anyhow, Result};

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
        for table in &spec.target.tables {
            self.ensure_telemetry_table_for(scope, table).await?;
        }
        let ddls = self.with_attached_conn(scope, |conn| {
            let prefix = scope.catalog_prefix();
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
        let ddls = self.with_attached_conn(scope, |conn| {
            // Prefer catalog.schema when metadata lives outside `main`; fall back to catalog-only
            // (matches write-path table name candidates when ATTACH uses METADATA_SCHEMA).
            let prefixes = {
                if scope.is_default_duckdb_namespace() {
                    vec![scope.attach_alias().to_owned()]
                } else {
                    vec![
                        format!(
                            "{}.{}",
                            quote_duckdb_ident(scope.attach_alias()),
                            quote_duckdb_ident(scope.pg_namespace())
                        ),
                        scope.attach_alias().to_owned(),
                    ]
                }
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
