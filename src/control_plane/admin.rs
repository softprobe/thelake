//! Administrative schema/promotion surface for one bound workspace.
//!
//! Promotion changes are deliberately separate from the ingest data path:
//! callers cannot reach schema DDL through the ordinary signal-write facade.
//! In shared mode the bound physical scope makes these changes global to every
//! workspace using that scope.

use crate::promotion::{BusinessApplyError, BusinessTableManifest, TelemetryColumnsManifest};
use crate::storage::ducklake::DuckLakeWriter;
use crate::workspace::DuckLakeScopeResolver;
use anyhow::Result;
use std::sync::Arc;

#[derive(Clone)]
pub struct AdminEngine {
    writer: Arc<DuckLakeWriter>,
    resolver: DuckLakeScopeResolver,
}

impl AdminEngine {
    pub(crate) fn new(writer: Arc<DuckLakeWriter>, resolver: DuckLakeScopeResolver) -> Self {
        Self { writer, resolver }
    }

    pub async fn apply_and_record_telemetry_promotion(
        &self,
        manifest_yaml: &str,
        spec: &TelemetryColumnsManifest,
        target_tables: &[String],
    ) -> Result<String> {
        self.resolver
            .apply_telemetry_promotion_guarded(
                self.writer.metadata_schema(),
                manifest_yaml,
                target_tables,
                || async {
                    self.writer
                        .apply_telemetry_column_promotion(spec)
                        .await
                        .map(|_| ())
                },
            )
            .await
    }

    pub async fn apply_business_promotion_guarded(
        &self,
        manifest_yaml: &str,
        spec: &BusinessTableManifest,
    ) -> std::result::Result<String, BusinessApplyError> {
        self.resolver
            .apply_business_promotion_guarded(
                self.writer.metadata_schema(),
                manifest_yaml,
                spec,
                || async {
                    self.writer
                        .apply_business_table_promotion(spec)
                        .await
                        .map(|_| ())
                },
            )
            .await
    }
}
