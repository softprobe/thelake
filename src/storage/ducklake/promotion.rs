//! Promotion DDL apply methods on DuckLakeWriter.
//!
//! Specs persist in the physical scope's Postgres DuckLake metadata schema.
//! Guarded apply (advisory lock + activation) lives on AdminEngine / the resolver;
//! this module only runs additive DuckLake DDL against the bound writer scope.

use super::util::quote_duckdb_ident;
use super::DuckLakeWriter;
use crate::promotion::{
    business_table_create_ddls, telemetry_column_add_ddls, BusinessTableManifest,
    TelemetryColumnsManifest,
};
use anyhow::{anyhow, Result};

impl DuckLakeWriter {
    /// Apply additive telemetry promotion DDL inside the bound DuckLake scope.
    ///
    /// `promotion apply` owns schema changes for promoted telemetry columns. It first materializes
    /// the hardcoded canonical telemetry tables if they do not exist, then runs the nullable
    /// `ALTER TABLE ADD COLUMN IF NOT EXISTS` statements generated from the tenant manifest.
    pub async fn apply_telemetry_column_promotion(
        &self,
        spec: &TelemetryColumnsManifest,
    ) -> Result<Vec<String>> {
        for table in &spec.target.tables {
            self.ensure_telemetry_table_for(table).await?;
        }
        let prefix = self.physical_scope().catalog_prefix();
        let ddls = self.with_attached_conn(|conn| {
            let ddls = telemetry_column_add_ddls(&prefix, spec)
                .map_err(|err| anyhow!("telemetry promotion validation failed: {err}"))?;
            for ddl in &ddls {
                conn.execute_batch(ddl)?;
            }
            Ok(ddls)
        })?;
        Ok(ddls)
    }

    /// Apply generated business table DDL inside the bound DuckLake scope.
    ///
    /// Business promotion manifests own the physical table and current view. The runtime executes
    /// generated DDL in order so agents do not need to write tenant-specific `CREATE TABLE` SQL.
    pub async fn apply_business_table_promotion(
        &self,
        spec: &BusinessTableManifest,
    ) -> Result<Vec<String>> {
        let scope = self.physical_scope().clone();
        let ddls = self.with_attached_conn(|conn| {
            let prefix = format!(
                "{}.{}",
                quote_duckdb_ident(scope.attach_alias()),
                quote_duckdb_ident(scope.pg_namespace())
            );
            let ddls = business_table_create_ddls(&prefix, spec)
                .map_err(|err| anyhow!("business table promotion validation failed: {err}"))?;
            for ddl in &ddls {
                conn.execute_batch(ddl).map_err(|err| {
                    anyhow!("business table promotion failed with prefix {prefix}: {err}")
                })?;
            }
            Ok(ddls)
        })?;
        Ok(ddls)
    }
}
