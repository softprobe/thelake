use crate::models::{Log, Span};
use crate::promotion::{
    extract_telemetry_promoted_value, PromotionColumn, TelemetryColumnsManifest,
    TelemetryPromotionEvent, TelemetryPromotionRow, TelemetryTable,
};
use crate::storage::schema::arrow;
use crate::storage::schema::tables::{OtlpLogsTable, TraceTable};
use crate::workspace_scope::PhysicalScope;
use anyhow::Result;
use std::sync::Arc;

use super::DuckLakeWriter;

impl DuckLakeWriter {
    pub(super) fn flatten_spans(batches: Vec<Vec<Span>>) -> Vec<Span> {
        batches.into_iter().flatten().collect()
    }

    pub(super) fn flatten_logs(batches: Vec<Vec<Log>>) -> Vec<Log> {
        batches.into_iter().flatten().collect()
    }

    pub(super) fn telemetry_columns_for_table(
        manifests: &[TelemetryColumnsManifest],
        table: TelemetryTable,
    ) -> Vec<PromotionColumn> {
        manifests
            .iter()
            .filter(|manifest| manifest.target.tables.contains(&table))
            .flat_map(|manifest| manifest.columns.iter().cloned())
            .collect()
    }

    pub(super) fn apply_span_promotions(
        spans: &mut [Span],
        columns: &[PromotionColumn],
    ) -> Result<()> {
        for span in spans {
            let events = span
                .events
                .iter()
                .map(|event| TelemetryPromotionEvent {
                    name: event.name.clone(),
                    attributes: event.attributes.clone(),
                })
                .collect::<Vec<_>>();
            let row = TelemetryPromotionRow {
                resource_attributes: &span.resource_attributes,
                attributes: &span.attributes,
                events: &events,
                http_request_body: span.http_request_body.as_deref(),
                http_response_body: span.http_response_body.as_deref(),
            };
            let mut promoted = Vec::new();
            for column in columns {
                if let Some(value) = extract_telemetry_promoted_value(&row, column)? {
                    promoted.push((column.name.clone(), value));
                }
            }
            span.attributes.extend(promoted);
        }
        Ok(())
    }

    pub(super) fn apply_log_promotions(
        logs: &mut [Log],
        columns: &[PromotionColumn],
    ) -> Result<()> {
        for log in logs {
            let row = TelemetryPromotionRow {
                resource_attributes: &log.resource_attributes,
                attributes: &log.attributes,
                events: &[],
                http_request_body: None,
                http_response_body: None,
            };
            let mut promoted = Vec::new();
            for column in columns {
                if let Some(value) = extract_telemetry_promoted_value(&row, column)? {
                    promoted.push((column.name.clone(), value));
                }
            }
            log.attributes.extend(promoted);
        }
        Ok(())
    }

    pub(crate) async fn write_span_batches(&self, batches: Vec<Vec<Span>>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        for batch in &batches {
            for span in batch {
                self.validate_shared_ownership(span.tenant_id.as_deref(), "span")?;
            }
        }
        let resolver = &self.tenant_ducklake;
        // An unbound writer is a single configured-scope composition/test
        // surface. It must never route by tenant_id from the payload.
        let scope = self
            .tenant_bound_scope()
            .cloned()
            .unwrap_or_else(|| self.physical.clone());
        let manifests = if self.scope_bound {
            resolver
                .load_active_telemetry_columns_manifests_for_scope(&scope)
                .await?
        } else {
            resolver
                .load_active_telemetry_columns_manifests("")
                .await?
                .1
        };
        let mut spans = Self::flatten_spans(batches);
        if spans.is_empty() {
            return Ok(());
        }
        let columns = Self::telemetry_columns_for_table(&manifests, TelemetryTable::Traces);
        Self::apply_span_promotions(&mut spans, &columns)?;
        let schema = Arc::new(TraceTable::schema_with_promoted_columns(&columns));
        let record_batches = Span::to_record_batches_by_date(spans, schema.as_ref())?;
        self.write_record_batches_internal_with_ducklake(&scope, "traces", record_batches)
            .await
    }

    pub(super) async fn write_tenant_log_batches(
        &self,
        scope: &PhysicalScope,
        manifests: &[TelemetryColumnsManifest],
        batches: Vec<Vec<Log>>,
    ) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let mut logs = Self::flatten_logs(batches);
        if logs.is_empty() {
            return Ok(());
        }
        for log in &logs {
            self.validate_shared_ownership(log.tenant_id.as_deref(), "log")?;
        }
        let columns = Self::telemetry_columns_for_table(manifests, TelemetryTable::Logs);
        Self::apply_log_promotions(&mut logs, &columns)?;
        let schema = Arc::new(OtlpLogsTable::schema_with_promoted_columns(&columns));
        let record_batches = arrow::logs_to_record_batches_by_date(logs, schema.as_ref())?;
        self.write_record_batches_internal_with_ducklake(scope, "logs", record_batches)
            .await?;
        Ok(())
    }

    pub(crate) async fn write_log_batches(&self, batches: Vec<Vec<Log>>) -> Result<()> {
        let resolver = &self.tenant_ducklake;
        // Non-scope-bound writers (single-tenant / tests) use the configured DuckLake scope.
        let scope = self
            .tenant_bound_scope()
            .cloned()
            .unwrap_or_else(|| self.physical.clone());
        let manifests = if self.scope_bound {
            resolver
                .load_active_telemetry_columns_manifests_for_scope(&scope)
                .await?
        } else {
            resolver
                .load_active_telemetry_columns_manifests("")
                .await?
                .1
        };
        self.write_tenant_log_batches(&scope, &manifests, batches)
            .await
    }
}
