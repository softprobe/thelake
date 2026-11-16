//! Track per-workspace attribute query patterns and rank DuckLake promotion candidates.

use crate::compat::backends::label_match::LabelMatcher;
use crate::compat::tempo::traceql::{TraceField, TracePredicate, TraceSelector};
use crate::promotion::{
    merge_telemetry_columns_manifests, telemetry_columns_manifest_to_yaml, PromotionColumn,
    PromotionDataType, PromotionSource, PromotionValidationError, TelemetryColumnsManifest,
    TelemetryColumnsTarget, TelemetryTable,
};
use crate::query::QueryResult;
use crate::sql::promotion_stats::{
    compile_attribute_stats_query, StatsCandidateKey, StatsSource, StatsTable,
};
use crate::sql::telemetry::{TelemetryFilterExpr, TelemetrySearchRequest};
use crate::workspace::WorkspaceContext;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_TRACKED_KEYS_PER_TABLE: usize = 512;
const MAX_CANDIDATES_PER_TABLE: usize = 16;
const SAMPLE_LOOKBACK_NS: i64 = 7 * 24 * 60 * 60 * 1_000_000_000;
const SAMPLE_FUTURE_SKEW_NS: i64 = 5 * 60 * 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RecommendationTable {
    Traces,
    Logs,
}

impl RecommendationTable {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Traces => "traces",
            Self::Logs => "logs",
        }
    }

    fn to_stats_table(self) -> StatsTable {
        match self {
            Self::Traces => StatsTable::Traces,
            Self::Logs => StatsTable::Logs,
        }
    }

    pub fn to_telemetry_table(self) -> TelemetryTable {
        match self {
            Self::Traces => TelemetryTable::Traces,
            Self::Logs => TelemetryTable::Logs,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationSource {
    Attribute,
    ResourceAttribute,
}

impl RecommendationSource {
    fn to_stats_source(self) -> StatsSource {
        match self {
            Self::Attribute => StatsSource::Attribute,
            Self::ResourceAttribute => StatsSource::ResourceAttribute,
        }
    }

    pub fn to_promotion_source(self, key: String) -> PromotionSource {
        match self {
            Self::Attribute => PromotionSource::Attribute { key },
            Self::ResourceAttribute => PromotionSource::ResourceAttribute { key },
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::Attribute => "attr",
            Self::ResourceAttribute => "res",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UsageKey {
    table: RecommendationTable,
    source: RecommendationSource,
    attribute_key: String,
}

#[derive(Debug, Clone, Default)]
pub struct AttrUsageStats {
    pub filter_count: u64,
    pub group_by_count: u64,
    pub query_count: u64,
    pub last_seen_epoch_sec: u64,
}

#[derive(Debug, Clone)]
pub struct TrackedAttributeUsage {
    pub table: RecommendationTable,
    pub source: RecommendationSource,
    pub attribute_key: String,
    pub filter_count: u64,
    pub group_by_count: u64,
    pub query_count: u64,
    pub last_seen_epoch_sec: u64,
}

#[derive(Debug, Default)]
pub struct AttrUsageTracker {
    workspaces: RwLock<HashMap<String, HashMap<UsageKey, AttrUsageStats>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageKind {
    Filter,
    GroupBy,
}

impl AttrUsageTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(
        &self,
        workspace_key: &str,
        table: RecommendationTable,
        source: RecommendationSource,
        attribute_key: &str,
        kind: UsageKind,
    ) {
        let key = attribute_key.trim();
        if workspace_key.trim().is_empty()
            || key.is_empty()
            || is_intrinsic_or_builtin_key(table, key)
        {
            return;
        }
        let now_sec = epoch_seconds_now();
        let mut guard = self.workspaces.write().expect("usage lock poisoned");
        let table_map = guard.entry(workspace_key.to_string()).or_default();
        let usage_key = UsageKey {
            table,
            source,
            attribute_key: key.to_string(),
        };
        if !table_map.contains_key(&usage_key) {
            let table_entries = table_map.keys().filter(|k| k.table == table).count();
            if table_entries >= MAX_TRACKED_KEYS_PER_TABLE {
                if let Some(evict_key) = table_map
                    .iter()
                    .filter(|(k, _)| k.table == table)
                    .min_by_key(|(_, v)| (v.query_count, v.last_seen_epoch_sec))
                    .map(|(k, _)| k.clone())
                {
                    table_map.remove(&evict_key);
                }
            }
        }
        let stats = table_map.entry(usage_key).or_default();
        stats.query_count = stats.query_count.saturating_add(1);
        match kind {
            UsageKind::Filter => stats.filter_count = stats.filter_count.saturating_add(1),
            UsageKind::GroupBy => stats.group_by_count = stats.group_by_count.saturating_add(1),
        }
        stats.last_seen_epoch_sec = now_sec;
    }

    pub fn record_tempo_search(
        &self,
        workspace_key: &str,
        tags: &std::collections::BTreeMap<String, String>,
        selector: Option<&TraceSelector>,
    ) {
        for tag_key in tags.keys() {
            let (source, attr_key) = classify_tempo_key(tag_key);
            self.record(
                workspace_key,
                RecommendationTable::Traces,
                source,
                attr_key,
                UsageKind::Filter,
            );
        }
        if let Some(sel) = selector {
            record_trace_selector(self, workspace_key, sel);
        }
    }

    pub fn record_telemetry_search(&self, workspace_key: &str, request: &TelemetrySearchRequest) {
        if let Some(filter) = request.filter.as_ref() {
            record_telemetry_filter(self, workspace_key, filter);
        }
        for col in &request.columns {
            if let Some((source, key)) = parse_prefixed_field(col) {
                self.record(
                    workspace_key,
                    RecommendationTable::Traces,
                    source,
                    key,
                    UsageKind::GroupBy,
                );
            }
        }
        for sort in &request.sort {
            if let Some((source, key)) = parse_prefixed_field(&sort.field) {
                self.record(
                    workspace_key,
                    RecommendationTable::Traces,
                    source,
                    key,
                    UsageKind::GroupBy,
                );
            }
        }
    }

    pub fn record_logql_matchers(
        &self,
        workspace_key: &str,
        matchers: &[LabelMatcher],
        parsed_filters: &[LabelMatcher],
    ) {
        for matcher in matchers.iter().chain(parsed_filters.iter()) {
            let (source, key) = classify_log_label(&matcher.name);
            self.record(
                workspace_key,
                RecommendationTable::Logs,
                source,
                key,
                UsageKind::Filter,
            );
        }
    }

    pub fn snapshot(&self, workspace_key: &str) -> Vec<TrackedAttributeUsage> {
        let guard = self.workspaces.read().expect("usage lock poisoned");
        let Some(map) = guard.get(workspace_key) else {
            return Vec::new();
        };
        let mut out: Vec<_> = map
            .iter()
            .map(|(k, v)| TrackedAttributeUsage {
                table: k.table,
                source: k.source,
                attribute_key: k.attribute_key.clone(),
                filter_count: v.filter_count,
                group_by_count: v.group_by_count,
                query_count: v.query_count,
                last_seen_epoch_sec: v.last_seen_epoch_sec,
            })
            .collect();
        out.sort_by(|a, b| {
            b.query_count
                .cmp(&a.query_count)
                .then_with(|| a.attribute_key.cmp(&b.attribute_key))
        });
        out
    }
}

fn record_trace_selector(
    tracker: &AttrUsageTracker,
    workspace_key: &str,
    selector: &TraceSelector,
) {
    match selector {
        TraceSelector::All => {}
        TraceSelector::Predicate(pred) => {
            let field = match pred {
                TracePredicate::Eq(f, _)
                | TracePredicate::NotEq(f, _)
                | TracePredicate::Regex(f, _)
                | TracePredicate::NotRegex(f, _)
                | TracePredicate::Greater(f, _)
                | TracePredicate::GreaterOrEqual(f, _)
                | TracePredicate::Less(f, _)
                | TracePredicate::LessOrEqual(f, _) => f,
            };
            match field {
                TraceField::Resource(key) => tracker.record(
                    workspace_key,
                    RecommendationTable::Traces,
                    RecommendationSource::ResourceAttribute,
                    key,
                    UsageKind::Filter,
                ),
                TraceField::Span(key) | TraceField::Unscoped(key) => {
                    let (source, attr_key) = classify_tempo_key(key);
                    tracker.record(
                        workspace_key,
                        RecommendationTable::Traces,
                        source,
                        attr_key,
                        UsageKind::Filter,
                    );
                }
                TraceField::Intrinsic(_) | TraceField::Instrumentation(_) => {}
            }
        }
        TraceSelector::And(lhs, rhs) | TraceSelector::Or(lhs, rhs) => {
            record_trace_selector(tracker, workspace_key, lhs);
            record_trace_selector(tracker, workspace_key, rhs);
        }
    }
}

fn record_telemetry_filter(
    tracker: &AttrUsageTracker,
    workspace_key: &str,
    expr: &TelemetryFilterExpr,
) {
    match expr {
        TelemetryFilterExpr::And { and } => {
            for item in and {
                record_telemetry_filter(tracker, workspace_key, item);
            }
        }
        TelemetryFilterExpr::Or { or } => {
            for item in or {
                record_telemetry_filter(tracker, workspace_key, item);
            }
        }
        TelemetryFilterExpr::Predicate(pred) => {
            if let Some((source, key)) = parse_prefixed_field(&pred.field) {
                tracker.record(
                    workspace_key,
                    RecommendationTable::Traces,
                    source,
                    key,
                    UsageKind::Filter,
                );
            }
        }
    }
}

fn parse_prefixed_field(field: &str) -> Option<(RecommendationSource, &str)> {
    let trimmed = field.trim();
    if let Some(key) = trimmed.strip_prefix("attributes.") {
        return Some((RecommendationSource::Attribute, key));
    }
    if let Some(key) = trimmed.strip_prefix("resource_attributes.") {
        return Some((RecommendationSource::ResourceAttribute, key));
    }
    if let Some(key) = trimmed.strip_prefix("span.") {
        return Some((RecommendationSource::Attribute, key));
    }
    if let Some(key) = trimmed.strip_prefix("resource.") {
        return Some((RecommendationSource::ResourceAttribute, key));
    }
    None
}

fn classify_tempo_key(raw: &str) -> (RecommendationSource, &str) {
    let trimmed = raw.trim();
    if let Some(key) = trimmed.strip_prefix("resource.") {
        (RecommendationSource::ResourceAttribute, key)
    } else if let Some(key) = trimmed.strip_prefix("span.") {
        (RecommendationSource::Attribute, key)
    } else if trimmed == "service.name" || trimmed.starts_with("service.") {
        (RecommendationSource::ResourceAttribute, trimmed)
    } else {
        (RecommendationSource::Attribute, trimmed)
    }
}

fn classify_log_label(raw: &str) -> (RecommendationSource, &str) {
    let trimmed = raw.trim();
    if let Some(key) = trimmed.strip_prefix("resource.") {
        (RecommendationSource::ResourceAttribute, key)
    } else {
        (RecommendationSource::Attribute, trimmed)
    }
}

fn is_intrinsic_or_builtin_key(table: RecommendationTable, key: &str) -> bool {
    if matches!(
        key,
        "service.name"
            | "service_name"
            | "status"
            | "status_code"
            | "duration"
            | "name"
            | "kind"
            | "trace_id"
            | "span_id"
            | "parent_span_id"
            | "severity_text"
            | "body"
            | "__error__"
    ) {
        return true;
    }
    if table == RecommendationTable::Traces && builtin_traces_hot_keys().contains(key) {
        return true;
    }
    false
}

fn builtin_traces_hot_keys() -> &'static HashSet<String> {
    static KEYS: OnceLock<HashSet<String>> = OnceLock::new();
    KEYS.get_or_init(|| {
        let Ok(crate::promotion::PromotionManifest::TelemetryColumns(manifest)) =
            crate::promotion::parse_promotion_manifest(include_str!(
                "../docs/promotion/traces-query-hot-attrs.yaml"
            ))
        else {
            return HashSet::new();
        };
        manifest
            .columns
            .into_iter()
            .filter_map(|c| match c.source {
                PromotionSource::Attribute { key } | PromotionSource::ResourceAttribute { key } => {
                    Some(key)
                }
                _ => None,
            })
            .collect()
    })
}

fn epoch_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivePromotedColumnView {
    pub table: RecommendationTable,
    pub column_name: String,
    pub data_type: String,
    pub source: RecommendationSource,
    pub attribute_key: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttributePromotionRecommendation {
    pub table: RecommendationTable,
    pub source: RecommendationSource,
    pub attribute_key: String,
    pub column_name: String,
    pub data_type: String,
    pub query_count: u64,
    pub filter_count: u64,
    pub group_by_count: u64,
    pub sampled_rows: u64,
    pub distinct_cardinality: u64,
    pub null_ratio: f64,
    pub avg_value_bytes: f64,
    pub estimated_scan_reduction_pct: f64,
    pub schema_width_cost: &'static str,
    pub score: f64,
    pub manifest_yaml: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovePromotionRequest {
    pub table: RecommendationTable,
    pub source: RecommendationSource,
    pub attribute_key: String,
    pub column_name: Option<String>,
    pub data_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CandidateStats {
    pub sampled_rows: u64,
    pub non_null_count: u64,
    pub distinct_cardinality: u64,
    pub avg_value_bytes: f64,
    pub bool_count: u64,
    pub int_count: u64,
    pub double_count: u64,
}

pub fn suggest_column_name(source: RecommendationSource, attribute_key: &str) -> String {
    let mut out = String::with_capacity(attribute_key.len() + 5);
    out.push_str(source.prefix());
    out.push('_');
    let mut prev_underscore = true;
    for ch in attribute_key.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_underscore = false;
        } else if !prev_underscore {
            out.push('_');
            prev_underscore = true;
        }
    }
    while out.ends_with('_') && out.len() > source.prefix().len() + 1 {
        out.pop();
    }
    if out.len() <= source.prefix().len() + 1 {
        out.push_str("col");
    }
    if out.len() > 63 {
        out.truncate(63);
        while out.ends_with('_') {
            out.pop();
        }
    }
    out
}

pub fn infer_promotion_data_type(stats: &CandidateStats) -> PromotionDataType {
    if stats.non_null_count == 0 {
        return PromotionDataType::String;
    }
    if stats.int_count == stats.non_null_count {
        PromotionDataType::Int64
    } else if stats.double_count == stats.non_null_count {
        PromotionDataType::Double
    } else if stats.bool_count == stats.non_null_count {
        PromotionDataType::Bool
    } else {
        PromotionDataType::String
    }
}

pub fn promotion_data_type_str(dt: &PromotionDataType) -> &'static str {
    match dt {
        PromotionDataType::String => "string",
        PromotionDataType::Bool => "bool",
        PromotionDataType::Int64 => "int64",
        PromotionDataType::Double => "double",
        PromotionDataType::Decimal => "decimal",
        PromotionDataType::Timestamp => "timestamp",
        PromotionDataType::Json => "json",
    }
}

pub fn parse_promotion_data_type(raw: &str) -> Option<PromotionDataType> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "string" => Some(PromotionDataType::String),
        "bool" | "boolean" => Some(PromotionDataType::Bool),
        "int64" | "bigint" => Some(PromotionDataType::Int64),
        "double" | "float64" => Some(PromotionDataType::Double),
        "decimal" => Some(PromotionDataType::Decimal),
        "timestamp" => Some(PromotionDataType::Timestamp),
        "json" => Some(PromotionDataType::Json),
        _ => None,
    }
}

pub fn estimate_scan_reduction_and_cost(stats: &CandidateStats) -> (f64, f64, &'static str) {
    if stats.sampled_rows == 0 || stats.non_null_count == 0 {
        return (1.0, 15.0, "low");
    }
    let non_null_ratio = stats.non_null_count as f64 / stats.sampled_rows as f64;
    let null_ratio = (1.0 - non_null_ratio).clamp(0.0, 1.0);

    // Zone-map pruning effectiveness grows with distinct cardinality across row groups
    // and drops when values are constant (cardinality <= 1) or overly wide strings.
    let selectivity_factor = if stats.distinct_cardinality <= 1 {
        0.05
    } else {
        let card = stats.distinct_cardinality as f64;
        (1.0 - (1.0 / card)).clamp(0.1, 0.92)
    };
    let width_cost = if stats.avg_value_bytes > 256.0 {
        "high"
    } else if stats.avg_value_bytes > 64.0 {
        "medium"
    } else {
        "low"
    };
    let width_penalty = match width_cost {
        "low" => 1.0,
        "medium" => 0.85,
        "high" => 0.55,
        _ => 1.0,
    };
    let coverage_boost = (0.35 + 0.65 * non_null_ratio).clamp(0.35, 1.0);
    let reduction = (selectivity_factor * coverage_boost * width_penalty * 90.0).clamp(5.0, 90.0);
    (
        (null_ratio * 1000.0).round() / 1000.0,
        (reduction * 10.0).round() / 10.0,
        width_cost,
    )
}

pub fn merge_column_into_active_manifest(
    table: TelemetryTable,
    active_manifests: &[TelemetryColumnsManifest],
    new_col: PromotionColumn,
) -> std::result::Result<String, PromotionValidationError> {
    let mut manifests_to_merge: Vec<TelemetryColumnsManifest> = active_manifests
        .iter()
        .filter(|m| m.target.tables.contains(&table))
        .map(|m| TelemetryColumnsManifest {
            target: TelemetryColumnsTarget {
                tables: vec![table.clone()],
            },
            columns: m.columns.clone(),
        })
        .collect();
    manifests_to_merge.push(TelemetryColumnsManifest {
        target: TelemetryColumnsTarget {
            tables: vec![table],
        },
        columns: vec![new_col],
    });
    let merged = merge_telemetry_columns_manifests(&manifests_to_merge)?;
    Ok(telemetry_columns_manifest_to_yaml(&merged))
}

fn active_promoted_key_set(
    manifests: &[TelemetryColumnsManifest],
    table: RecommendationTable,
) -> HashSet<(RecommendationSource, String)> {
    let target = table.to_telemetry_table();
    let mut set = HashSet::new();
    for m in manifests {
        if !m.target.tables.contains(&target) {
            continue;
        }
        for col in &m.columns {
            match &col.source {
                PromotionSource::Attribute { key } => {
                    set.insert((RecommendationSource::Attribute, key.clone()));
                }
                PromotionSource::ResourceAttribute { key } => {
                    set.insert((RecommendationSource::ResourceAttribute, key.clone()));
                }
                _ => {}
            }
        }
    }
    set
}

pub fn active_promoted_views(
    manifests: &[TelemetryColumnsManifest],
) -> Vec<ActivePromotedColumnView> {
    let mut out = Vec::new();
    for m in manifests {
        for tbl in &m.target.tables {
            let rec_table = match tbl {
                TelemetryTable::Traces => RecommendationTable::Traces,
                TelemetryTable::Logs => RecommendationTable::Logs,
            };
            for col in &m.columns {
                let (source, key) = match &col.source {
                    PromotionSource::Attribute { key } => {
                        (RecommendationSource::Attribute, key.clone())
                    }
                    PromotionSource::ResourceAttribute { key } => {
                        (RecommendationSource::ResourceAttribute, key.clone())
                    }
                    _ => continue,
                };
                out.push(ActivePromotedColumnView {
                    table: rec_table,
                    column_name: col.name.clone(),
                    data_type: promotion_data_type_str(&col.data_type).to_string(),
                    source,
                    attribute_key: key,
                });
            }
        }
    }
    out
}

pub async fn build_recommendations(
    ws: &WorkspaceContext,
    usage: &[TrackedAttributeUsage],
    active_manifests: &[TelemetryColumnsManifest],
) -> Result<Vec<AttributePromotionRecommendation>> {
    let now_ns = (epoch_seconds_now() as i64) * 1_000_000_000;
    let start_ns = now_ns.saturating_sub(SAMPLE_LOOKBACK_NS);
    let end_ns = now_ns.saturating_add(SAMPLE_FUTURE_SKEW_NS);
    let mut all = Vec::new();

    for table in [RecommendationTable::Traces, RecommendationTable::Logs] {
        let promoted = active_promoted_key_set(active_manifests, table);
        let mut candidates: Vec<&TrackedAttributeUsage> = usage
            .iter()
            .filter(|u| {
                u.table == table
                    && !is_intrinsic_or_builtin_key(table, &u.attribute_key)
                    && !promoted.contains(&(u.source, u.attribute_key.clone()))
            })
            .collect();
        candidates.sort_by(|a, b| b.query_count.cmp(&a.query_count));
        candidates.truncate(MAX_CANDIDATES_PER_TABLE);
        if candidates.is_empty() {
            continue;
        }

        let keys: Vec<StatsCandidateKey> = candidates
            .iter()
            .map(|c| StatsCandidateKey {
                source: c.source.to_stats_source(),
                key: c.attribute_key.clone(),
            })
            .collect();
        let stats_vec =
            match compile_attribute_stats_query(table.to_stats_table(), start_ns, end_ns, &keys) {
                Ok(sql) => {
                    let result = ws.query().execute_trusted(sql).await.ok();
                    parse_stats_result(result.as_ref(), candidates.len())
                }
                Err(_) => parse_stats_result(None, candidates.len()),
            };

        for (candidate, stats) in candidates.into_iter().zip(stats_vec.into_iter()) {
            let inferred_type = infer_promotion_data_type(&stats);
            let col_name = suggest_column_name(candidate.source, &candidate.attribute_key);
            let (null_ratio, estimated_scan_reduction_pct, schema_width_cost) =
                estimate_scan_reduction_and_cost(&stats);
            let score = ((estimated_scan_reduction_pct
                * (1.0 + candidate.query_count as f64).ln())
                * 100.0)
                .round()
                / 100.0;
            let Ok(manifest_yaml) = merge_column_into_active_manifest(
                table.to_telemetry_table(),
                active_manifests,
                PromotionColumn {
                    name: col_name.clone(),
                    data_type: inferred_type.clone(),
                    nullable: true,
                    source: candidate
                        .source
                        .to_promotion_source(candidate.attribute_key.clone()),
                },
            ) else {
                continue;
            };
            all.push(AttributePromotionRecommendation {
                table,
                source: candidate.source,
                attribute_key: candidate.attribute_key.clone(),
                column_name: col_name,
                data_type: promotion_data_type_str(&inferred_type).to_string(),
                query_count: candidate.query_count,
                filter_count: candidate.filter_count,
                group_by_count: candidate.group_by_count,
                sampled_rows: stats.sampled_rows,
                distinct_cardinality: stats.distinct_cardinality,
                null_ratio,
                avg_value_bytes: (stats.avg_value_bytes * 10.0).round() / 10.0,
                estimated_scan_reduction_pct,
                schema_width_cost,
                score,
                manifest_yaml,
            });
        }
    }

    all.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.query_count.cmp(&a.query_count))
    });
    Ok(all)
}

fn parse_stats_result(result: Option<&QueryResult>, count: usize) -> Vec<CandidateStats> {
    let default = vec![
        CandidateStats {
            sampled_rows: 0,
            non_null_count: 0,
            distinct_cardinality: 0,
            avg_value_bytes: 0.0,
            bool_count: 0,
            int_count: 0,
            double_count: 0,
        };
        count
    ];
    let Some(res) = result else {
        return default;
    };
    let Some(row) = res.rows.first() else {
        return default;
    };
    let total_rows = col_u64(&res.columns, row, "total_rows");
    (0..count)
        .map(|idx| CandidateStats {
            sampled_rows: total_rows,
            non_null_count: col_u64(&res.columns, row, &format!("non_null_{idx}")),
            distinct_cardinality: col_u64(&res.columns, row, &format!("distinct_{idx}")),
            avg_value_bytes: col_f64(&res.columns, row, &format!("avg_bytes_{idx}")),
            bool_count: col_u64(&res.columns, row, &format!("bool_{idx}")),
            int_count: col_u64(&res.columns, row, &format!("int_{idx}")),
            double_count: col_u64(&res.columns, row, &format!("double_{idx}")),
        })
        .collect()
}

fn col_u64(columns: &[String], row: &[serde_json::Value], name: &str) -> u64 {
    columns
        .iter()
        .position(|c| c == name)
        .and_then(|idx| row.get(idx))
        .and_then(|v| v.as_u64().or_else(|| v.as_i64().map(|n| n.max(0) as u64)))
        .unwrap_or(0)
}

fn col_f64(columns: &[String], row: &[serde_json::Value], name: &str) -> f64 {
    columns
        .iter()
        .position(|c| c == name)
        .and_then(|idx| row.get(idx))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::backends::label_match::MatcherOp;
    use crate::compat::tempo::traceql::parse_traceql;
    use crate::promotion::parse_promotion_manifest;
    use crate::sql::telemetry::{
        TelemetryFilter, TelemetrySearchScope, TelemetrySort, TelemetrySortDirection,
    };

    #[test]
    fn tracker_records_traceql_telemetry_and_logql_while_skipping_builtins() {
        let tracker = AttrUsageTracker::new();
        let selector = parse_traceql(
            r#"{ span.tenant.tier = "enterprise" && resource.deploy.region = "us-west" && status = error }"#,
        )
        .expect("traceql");
        let mut tags = std::collections::BTreeMap::new();
        tags.insert("order.region".to_string(), "eu".to_string());
        tracker.record_tempo_search("ws-1", &tags, Some(&selector));

        let telemetry_req = TelemetrySearchRequest {
            version: 1,
            scope: TelemetrySearchScope::Traces,
            time_range: None,
            filter: Some(TelemetryFilterExpr::Predicate(TelemetryFilter {
                field: "attributes.tenant.tier".to_string(),
                op: "eq".to_string(),
                value: Some(serde_json::json!("enterprise")),
            })),
            columns: vec!["attributes.workflow.stage".to_string()],
            sort: vec![TelemetrySort {
                field: "resource_attributes.deploy.region".to_string(),
                direction: TelemetrySortDirection::Desc,
            }],
            limit: None,
            cursor: None,
        };
        tracker.record_telemetry_search("ws-1", &telemetry_req);

        tracker.record_logql_matchers(
            "ws-1",
            &[
                LabelMatcher {
                    name: "service_name".to_string(),
                    op: MatcherOp::Eq,
                    value: "checkout".to_string(),
                },
                LabelMatcher {
                    name: "mcp.tool.name".to_string(),
                    op: MatcherOp::Eq,
                    value: "book_flight".to_string(),
                },
            ],
            &[],
        );

        let snap = tracker.snapshot("ws-1");
        assert!(!snap.iter().any(|u| u.attribute_key == "status"));
        assert!(!snap.iter().any(|u| u.attribute_key == "service_name"));

        let tier = snap
            .iter()
            .find(|u| u.attribute_key == "tenant.tier")
            .expect("tenant.tier");
        assert_eq!(tier.query_count, 2);
        assert_eq!(tier.filter_count, 2);
        assert_eq!(tier.source, RecommendationSource::Attribute);

        let stage = snap
            .iter()
            .find(|u| u.attribute_key == "workflow.stage")
            .expect("workflow.stage");
        assert_eq!(stage.group_by_count, 1);

        let tool = snap
            .iter()
            .find(|u| u.attribute_key == "mcp.tool.name")
            .expect("mcp.tool.name");
        assert_eq!(tool.table, RecommendationTable::Logs);
    }

    #[test]
    fn infers_types_and_scores_selective_narrow_attributes_higher() {
        let selective = CandidateStats {
            sampled_rows: 1000,
            non_null_count: 950,
            distinct_cardinality: 12,
            avg_value_bytes: 10.0,
            bool_count: 0,
            int_count: 0,
            double_count: 0,
        };
        let constant_wide = CandidateStats {
            sampled_rows: 1000,
            non_null_count: 950,
            distinct_cardinality: 1,
            avg_value_bytes: 512.0,
            bool_count: 0,
            int_count: 0,
            double_count: 0,
        };
        let (_, red_sel, cost_sel) = estimate_scan_reduction_and_cost(&selective);
        let (_, red_const, cost_const) = estimate_scan_reduction_and_cost(&constant_wide);
        assert!(red_sel > red_const);
        assert_eq!(cost_sel, "low");
        assert_eq!(cost_const, "high");

        assert_eq!(
            infer_promotion_data_type(&CandidateStats {
                sampled_rows: 10,
                non_null_count: 8,
                distinct_cardinality: 5,
                avg_value_bytes: 4.0,
                bool_count: 0,
                int_count: 8,
                double_count: 8,
            }),
            PromotionDataType::Int64
        );
        assert_eq!(
            suggest_column_name(RecommendationSource::Attribute, "tenant.tier-v2"),
            "attr_tenant_tier_v2"
        );
    }

    #[test]
    fn merge_column_preserves_existing_active_columns_and_produces_valid_yaml() {
        let existing = match parse_promotion_manifest(
            r#"
specVersion: softprobe.promotion.v1
target:
  kind: telemetry_columns
  tables: [traces]
columns:
  - name: attr_existing_key
    type: string
    nullable: true
    source:
      from: attribute
      key: existing.key
"#,
        )
        .expect("parse existing")
        {
            crate::promotion::PromotionManifest::TelemetryColumns(m) => m,
            _ => unreachable!(),
        };

        let merged_yaml = merge_column_into_active_manifest(
            TelemetryTable::Traces,
            &[existing],
            PromotionColumn {
                name: "attr_tenant_tier".to_string(),
                data_type: PromotionDataType::String,
                nullable: true,
                source: PromotionSource::Attribute {
                    key: "tenant.tier".to_string(),
                },
            },
        )
        .expect("merge yaml");

        let parsed = parse_promotion_manifest(&merged_yaml).expect("valid manifest yaml");
        let crate::promotion::PromotionManifest::TelemetryColumns(m) = parsed else {
            panic!("expected TelemetryColumns");
        };
        assert_eq!(m.columns.len(), 2);
        assert_eq!(m.columns[0].name, "attr_existing_key");
        assert_eq!(m.columns[1].name, "attr_tenant_tier");
    }

    #[test]
    fn tracker_evicts_lowest_count_when_capacity_reached() {
        let tracker = AttrUsageTracker::new();
        for i in 0..=MAX_TRACKED_KEYS_PER_TABLE {
            let key = format!("custom.key.{i}");
            tracker.record(
                "ws-cap",
                RecommendationTable::Traces,
                RecommendationSource::Attribute,
                &key,
                UsageKind::Filter,
            );
            if i > 0 {
                tracker.record(
                    "ws-cap",
                    RecommendationTable::Traces,
                    RecommendationSource::Attribute,
                    &key,
                    UsageKind::Filter,
                );
            }
        }
        let snap = tracker.snapshot("ws-cap");
        assert_eq!(snap.len(), MAX_TRACKED_KEYS_PER_TABLE);
        assert!(!snap.iter().any(|u| u.attribute_key == "custom.key.0"));
    }
}
