//! Session list skinny-delta stats: manifest + merge vocabulary.
//!
//! `softprobe.session_stats.v1` declares mergeable measures/dimensions for
//! `session_stats_delta`. Ops are limited to sum/min/max/any (no sketches).

use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};
use std::fmt;

use anyhow::Result;
use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, MapArray, StringArray,
    StructArray, TimestampNanosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Utc};
use std::sync::Arc;

use crate::models::Span;

pub const SESSION_STATS_SPEC_VERSION: &str = "softprobe.session_stats.v1";

/// Builtin product defaults for Explorer session list (SoT:
/// `docs/session_stats/default.yaml`).
pub const BUILTIN_SESSION_STATS_YAML: &str = include_str!("../docs/session_stats/default.yaml");

/// Physical columns already on `session_stats_delta` (no apply DDL needed).
pub fn is_physical_session_stats_column(name: &str) -> bool {
    matches!(
        name,
        "session_id"
            | "record_date"
            | "start_time"
            | "end_time"
            | "observation_count"
            | "error_count"
            | "trace_count"
            | "input_tokens"
            | "output_tokens"
            | "total_tokens"
            | "total_cost"
            | "agent_name"
            | "is_nested_child"
            | "measures"
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStatsManifest {
    pub key: Vec<String>,
    pub measures: Vec<SessionStatsMeasure>,
    pub dimensions: Vec<SessionStatsDimension>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStatsMeasure {
    pub name: String,
    pub op: MergeOp,
    pub source: MeasureSource,
    /// True when this measure is stored in the `measures` MAP rather than a core column.
    pub map_backed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStatsDimension {
    pub name: String,
    pub source: DimensionSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeOp {
    Sum,
    Min,
    Max,
    Any,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeasureSource {
    CountRows,
    CountWhere { column: String, eq: String },
    CountDistinct { column: String },
    Column { column: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DimensionSource {
    Column { column: String },
    FlagAttr { key: String },
}

/// Core columns always present on `session_stats_delta` (not map-backed).
pub fn is_core_measure_name(name: &str) -> bool {
    matches!(
        name,
        "observation_count"
            | "error_count"
            | "total_tokens"
            | "total_cost"
            | "input_tokens"
            | "output_tokens"
            | "trace_count"
            | "start_time"
            | "end_time"
    )
}

pub fn is_core_dimension_name(name: &str) -> bool {
    matches!(
        name,
        "agent_name" | "is_nested_child" | "user_id" | "model_name"
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStatsValidationError {
    pub code: String,
    pub path: String,
    pub message: String,
}

impl SessionStatsValidationError {
    pub fn new(
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for SessionStatsValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {}: {}", self.code, self.path, self.message)
    }
}

impl std::error::Error for SessionStatsValidationError {}

pub fn parse_session_stats_manifest(
    input: &str,
) -> Result<SessionStatsManifest, SessionStatsValidationError> {
    let raw: RawSessionStatsManifest = serde_yaml::from_str(input).map_err(|err| {
        SessionStatsValidationError::new(
            "invalid_yaml",
            "$",
            format!("session stats manifest YAML is invalid: {err}"),
        )
    })?;
    raw.validate()
}

pub fn builtin_session_stats_manifest() -> SessionStatsManifest {
    parse_session_stats_manifest(BUILTIN_SESSION_STATS_YAML)
        .expect("builtin session stats manifest must parse")
}

/// Describe apply-side schema effects for a session_stats manifest (no I/O).
pub fn session_stats_schema_changes(manifest: &SessionStatsManifest) -> Vec<serde_json::Value> {
    let mut changes = Vec::new();
    for measure in &manifest.measures {
        if measure.map_backed {
            changes.push(serde_json::json!({
                "table": "session_stats_delta",
                "action": "map_measure",
                "column": measure.name,
            }));
        }
    }
    for dim in &manifest.dimensions {
        if !is_physical_session_stats_column(&dim.name) {
            changes.push(serde_json::json!({
                "table": "session_stats_delta",
                "action": "add_column",
                "column": dim.name,
                "type": "string",
                "nullable": true,
            }));
        }
    }
    changes
}

/// Idempotent ADD COLUMN DDLs for dimensions that are not already physical.
pub fn session_stats_dimension_add_ddls(
    catalog_schema_prefix: &str,
    manifest: &SessionStatsManifest,
) -> Vec<String> {
    let mut ddls = Vec::new();
    for dim in &manifest.dimensions {
        if is_physical_session_stats_column(&dim.name) {
            continue;
        }
        // Identifiers are validated at parse time ([a-z_][a-z0-9_]*).
        ddls.push(format!(
            "ALTER TABLE {}.session_stats_delta ADD COLUMN IF NOT EXISTS \"{}\" VARCHAR;",
            catalog_schema_prefix, dim.name
        ));
    }
    ddls
}

/// Test hook: when true, `write_session_stats_deltas_best_effort` skips the
/// delta write after spans commit (simulates INSERT failure isolation).
///
/// Integration tests that flip this flag must serialize with other ingest tests
/// via `tests/util/session_stats_serial.rs`.
pub fn set_fail_session_stats_delta_write_for_test(fail: bool) {
    *SESSION_STATS_DELTA_WRITE_FAIL
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = fail;
}

pub fn fail_session_stats_delta_write_for_test() -> bool {
    *SESSION_STATS_DELTA_WRITE_FAIL
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

static SESSION_STATS_DELTA_WRITE_FAIL: std::sync::Mutex<bool> = std::sync::Mutex::new(false);

/// One skinny delta row per session_id in an ingest batch.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionStatsDeltaRow {
    pub session_id: String,
    pub record_date: NaiveDate,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub observation_count: i64,
    pub error_count: i64,
    pub trace_count: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    pub total_cost: f64,
    pub agent_name: Option<String>,
    pub is_nested_child: bool,
    pub measures: BTreeMap<String, f64>,
}

/// Derive mergeable session deltas from a span batch using the active manifest.
///
/// Skips empty `session_id` and recording observations. Multiple batches for the
/// same session produce multiple rows (merge-on-read later); no in-place UPDATE.
pub fn derive_session_deltas(
    spans: &[Span],
    manifest: &SessionStatsManifest,
) -> Vec<SessionStatsDeltaRow> {
    let mut by_session: BTreeMap<String, Vec<&Span>> = BTreeMap::new();
    for span in spans {
        if span.session_id.trim().is_empty() {
            continue;
        }
        if is_recording_observation(span) {
            continue;
        }
        by_session
            .entry(span.session_id.clone())
            .or_default()
            .push(span);
    }

    by_session
        .into_iter()
        .map(|(session_id, group)| derive_one_session(&session_id, &group, manifest))
        .collect()
}

fn derive_one_session(
    session_id: &str,
    group: &[&Span],
    manifest: &SessionStatsManifest,
) -> SessionStatsDeltaRow {
    let mut start_time = group[0].timestamp;
    let mut end_time = group[0].end_timestamp.unwrap_or(group[0].timestamp);
    let mut error_count = 0i64;
    let mut input_tokens = 0i64;
    let mut output_tokens = 0i64;
    let mut total_tokens = 0i64;
    let mut total_cost = 0.0f64;
    let mut agent_name: Option<String> = None;
    let mut is_nested_child = false;
    let mut traces = HashSet::new();
    let mut map_measures: BTreeMap<String, f64> = BTreeMap::new();

    for span in group {
        if span.timestamp < start_time {
            start_time = span.timestamp;
        }
        let span_end = span.end_timestamp.unwrap_or(span.timestamp);
        if span_end > end_time {
            end_time = span_end;
        }
        if span.status_code.as_deref() == Some("ERROR") {
            error_count += 1;
        }
        input_tokens += attr_i64(span, &["input_tokens", "gen_ai.usage.input_tokens"]).unwrap_or(0);
        output_tokens +=
            attr_i64(span, &["output_tokens", "gen_ai.usage.output_tokens"]).unwrap_or(0);
        total_tokens += attr_i64(span, &["total_tokens", "gen_ai.usage.total_tokens"]).unwrap_or(0);
        total_cost += attr_f64(span, &["total_cost", "sp.cost.total"]).unwrap_or(0.0);
        if agent_name.is_none() {
            agent_name = first_agent_name(span);
        }
        if !span.trace_id.is_empty() {
            traces.insert(span.trace_id.as_str());
        }
        if observation_type(span).as_deref() == Some("agent") {
            if let Some(parent) = span.attributes.get("sp.metadata.opencode.parentSessionID") {
                if !parent.trim().is_empty() {
                    is_nested_child = true;
                }
            }
        }
    }

    for measure in &manifest.measures {
        if !measure.map_backed {
            continue;
        }
        if let MeasureSource::Column { column } = &measure.source {
            let mut sum = 0.0;
            for span in group {
                sum += attr_f64(span, &[column.as_str()]).unwrap_or(0.0);
            }
            map_measures.insert(measure.name.clone(), sum);
        }
    }

    // Manifest flag_attr dimensions (builtin: is_nested_child) already applied above
    // for the known parentSessionID key; keep agent_name from first non-null.
    for dim in &manifest.dimensions {
        if dim.name == "is_nested_child" {
            if let DimensionSource::FlagAttr { key } = &dim.source {
                if key != "sp.metadata.opencode.parentSessionID" {
                    is_nested_child = group.iter().any(|span| {
                        span.attributes
                            .get(key)
                            .map(|v| !v.trim().is_empty())
                            .unwrap_or(false)
                    });
                }
            }
        }
    }

    SessionStatsDeltaRow {
        session_id: session_id.to_string(),
        record_date: start_time.date_naive(),
        start_time,
        end_time,
        observation_count: group.len() as i64,
        error_count,
        trace_count: traces.len() as i64,
        input_tokens,
        output_tokens,
        total_tokens,
        total_cost,
        agent_name,
        is_nested_child,
        measures: map_measures,
    }
}

fn is_recording_observation(span: &Span) -> bool {
    observation_type(span).as_deref() == Some("recording")
}

fn observation_type(span: &Span) -> Option<String> {
    attr_str(span, &["observation_type", "sp.observation.type"])
}

fn first_agent_name(span: &Span) -> Option<String> {
    if let Some(name) = span
        .agent_name
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        return Some(name.to_string());
    }
    attr_str(span, &["agent_name", "sp.agent.name"])
}

fn attr_str(span: &Span, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = span.attributes.get(*key) {
            let t = v.trim();
            if !t.is_empty() {
                return Some(t.to_string());
            }
        }
    }
    None
}

fn attr_i64(span: &Span, keys: &[&str]) -> Option<i64> {
    attr_str(span, keys).and_then(|v| v.parse::<i64>().ok())
}

fn attr_f64(span: &Span, keys: &[&str]) -> Option<f64> {
    attr_str(span, keys).and_then(|v| v.parse::<f64>().ok())
}

/// Convert delta rows to an Arrow RecordBatch matching [`SessionStatsDeltaTable`] schema.
pub fn session_stats_deltas_to_record_batch(
    rows: &[SessionStatsDeltaRow],
    schema: &Schema,
) -> Result<RecordBatch> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let session_ids: ArrayRef = Arc::new(StringArray::from(
        rows.iter()
            .map(|r| r.session_id.as_str())
            .collect::<Vec<_>>(),
    ));
    let record_dates: ArrayRef = Arc::new(Date32Array::from(
        rows.iter()
            .map(|r| (r.record_date - epoch).num_days() as i32)
            .collect::<Vec<_>>(),
    ));
    let start_times: ArrayRef = Arc::new(TimestampNanosecondArray::from(
        rows.iter()
            .map(|r| r.start_time.timestamp_nanos_opt())
            .collect::<Vec<_>>(),
    ));
    let end_times: ArrayRef = Arc::new(TimestampNanosecondArray::from(
        rows.iter()
            .map(|r| r.end_time.timestamp_nanos_opt())
            .collect::<Vec<_>>(),
    ));
    let observation_counts: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.observation_count).collect::<Vec<_>>(),
    ));
    let error_counts: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.error_count).collect::<Vec<_>>(),
    ));
    let trace_counts: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.trace_count).collect::<Vec<_>>(),
    ));
    let input_tokens: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.input_tokens).collect::<Vec<_>>(),
    ));
    let output_tokens: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.output_tokens).collect::<Vec<_>>(),
    ));
    let total_tokens: ArrayRef = Arc::new(Int64Array::from(
        rows.iter().map(|r| r.total_tokens).collect::<Vec<_>>(),
    ));
    let total_costs: ArrayRef = Arc::new(Float64Array::from(
        rows.iter().map(|r| r.total_cost).collect::<Vec<_>>(),
    ));
    let agent_names: ArrayRef = Arc::new(StringArray::from(
        rows.iter()
            .map(|r| r.agent_name.as_deref())
            .collect::<Vec<_>>(),
    ));
    let nested: ArrayRef = Arc::new(BooleanArray::from(
        rows.iter().map(|r| r.is_nested_child).collect::<Vec<_>>(),
    ));
    let measures_field = schema
        .field_with_name("measures")
        .map_err(|e| anyhow::anyhow!("measures field missing: {e}"))?;
    let measures = build_double_map_array(rows.iter().map(|r| &r.measures), measures_field)?;

    // Build columns in schema field order.
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let col: ArrayRef = match field.name().as_str() {
            "session_id" => Arc::clone(&session_ids),
            "record_date" => Arc::clone(&record_dates),
            "start_time" => Arc::clone(&start_times),
            "end_time" => Arc::clone(&end_times),
            "observation_count" => Arc::clone(&observation_counts),
            "error_count" => Arc::clone(&error_counts),
            "trace_count" => Arc::clone(&trace_counts),
            "input_tokens" => Arc::clone(&input_tokens),
            "output_tokens" => Arc::clone(&output_tokens),
            "total_tokens" => Arc::clone(&total_tokens),
            "total_cost" => Arc::clone(&total_costs),
            "agent_name" => Arc::clone(&agent_names),
            "is_nested_child" => Arc::clone(&nested),
            "measures" => Arc::clone(&measures),
            other => {
                return Err(anyhow::anyhow!(
                    "unexpected session_stats_delta column: {other}"
                ));
            }
        };
        columns.push(col);
    }
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
}

fn build_double_map_array<'a, I>(
    maps: I,
    measures_field: &arrow::datatypes::Field,
) -> Result<ArrayRef>
where
    I: IntoIterator<Item = &'a BTreeMap<String, f64>>,
{
    let mut keys = Vec::new();
    let mut values: Vec<Option<f64>> = Vec::new();
    let mut offsets = vec![0i32];
    let mut offset = 0i32;
    for map in maps {
        for (key, value) in map {
            keys.push(key.as_str());
            values.push(Some(*value));
            offset += 1;
        }
        offsets.push(offset);
    }

    let entries_field = match measures_field.data_type() {
        DataType::Map(field, _) => field.clone(),
        _ => return Err(anyhow::anyhow!("Expected Map type for measures")),
    };
    let struct_fields = match entries_field.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => return Err(anyhow::anyhow!("Expected Struct type in measures map")),
    };
    let entries = StructArray::new(
        struct_fields,
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(Float64Array::from(values)),
        ],
        None,
    );
    Ok(Arc::new(MapArray::try_new(
        entries_field,
        OffsetBuffer::new(offsets.into()),
        entries,
        None,
        false,
    )?))
}

#[derive(Debug, Deserialize)]
struct RawSessionStatsManifest {
    #[serde(rename = "specVersion")]
    spec_version: String,
    #[serde(default)]
    key: Vec<String>,
    #[serde(default)]
    measures: Vec<RawMeasure>,
    #[serde(default)]
    dimensions: Vec<RawDimension>,
}

#[derive(Debug, Deserialize)]
struct RawMeasure {
    name: String,
    op: String,
    source: RawMeasureSource,
}

#[derive(Debug, Deserialize)]
struct RawMeasureSource {
    kind: String,
    #[serde(default)]
    column: Option<String>,
    #[serde(default)]
    eq: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawDimension {
    name: String,
    source: RawDimensionSource,
}

#[derive(Debug, Deserialize)]
struct RawDimensionSource {
    kind: String,
    #[serde(default)]
    column: Option<String>,
    #[serde(default)]
    key: Option<String>,
}

impl RawSessionStatsManifest {
    fn validate(self) -> Result<SessionStatsManifest, SessionStatsValidationError> {
        if self.spec_version != SESSION_STATS_SPEC_VERSION {
            return Err(SessionStatsValidationError::new(
                "unsupported_spec_version",
                "specVersion",
                format!(
                    "expected {}, got {}",
                    SESSION_STATS_SPEC_VERSION, self.spec_version
                ),
            ));
        }
        if self.key.is_empty() || !self.key.iter().any(|k| k == "session_id") {
            return Err(SessionStatsValidationError::new(
                "missing_session_id_key",
                "key",
                "session stats manifest key must include session_id",
            ));
        }
        if self.measures.is_empty() {
            return Err(SessionStatsValidationError::new(
                "missing_measures",
                "measures",
                "session stats manifest must declare at least one measure",
            ));
        }

        let mut names = HashSet::new();
        let mut measures = Vec::with_capacity(self.measures.len());
        for (idx, m) in self.measures.into_iter().enumerate() {
            validate_identifier(&format!("measures[{idx}].name"), &m.name)?;
            if !names.insert(m.name.clone()) {
                return Err(SessionStatsValidationError::new(
                    "duplicate_measure",
                    format!("measures[{idx}].name"),
                    "measure name is declared more than once",
                ));
            }
            let op = parse_op(idx, &m.op)?;
            let source = parse_measure_source(idx, m.source)?;
            let map_backed = !is_core_measure_name(&m.name);
            measures.push(SessionStatsMeasure {
                name: m.name,
                op,
                source,
                map_backed,
            });
        }

        let mut dimensions = Vec::with_capacity(self.dimensions.len());
        for (idx, d) in self.dimensions.into_iter().enumerate() {
            validate_identifier(&format!("dimensions[{idx}].name"), &d.name)?;
            if !names.insert(d.name.clone()) {
                return Err(SessionStatsValidationError::new(
                    "duplicate_name",
                    format!("dimensions[{idx}].name"),
                    "name collides with a measure or another dimension",
                ));
            }
            let source = parse_dimension_source(idx, d.source)?;
            dimensions.push(SessionStatsDimension {
                name: d.name,
                source,
            });
        }

        Ok(SessionStatsManifest {
            key: self.key,
            measures,
            dimensions,
        })
    }
}

fn parse_op(idx: usize, op: &str) -> Result<MergeOp, SessionStatsValidationError> {
    match op {
        "sum" => Ok(MergeOp::Sum),
        "min" => Ok(MergeOp::Min),
        "max" => Ok(MergeOp::Max),
        "any" => Ok(MergeOp::Any),
        other => Err(SessionStatsValidationError::new(
            "unsupported_op",
            format!("measures[{idx}].op"),
            format!("unsupported merge op '{other}'; allowed: sum, min, max, any"),
        )),
    }
}

fn parse_measure_source(
    idx: usize,
    raw: RawMeasureSource,
) -> Result<MeasureSource, SessionStatsValidationError> {
    let path = format!("measures[{idx}].source");
    match raw.kind.as_str() {
        "count_rows" => Ok(MeasureSource::CountRows),
        "count_where" => {
            let column = require_field(&path, "column", raw.column)?;
            let eq = require_field(&path, "eq", raw.eq)?;
            Ok(MeasureSource::CountWhere { column, eq })
        }
        "count_distinct" => {
            let column = require_field(&path, "column", raw.column)?;
            Ok(MeasureSource::CountDistinct { column })
        }
        "column" => {
            let column = require_field(&path, "column", raw.column)?;
            Ok(MeasureSource::Column { column })
        }
        other => Err(SessionStatsValidationError::new(
            "unknown_source_kind",
            format!("{path}.kind"),
            format!("unknown measure source kind '{other}'"),
        )),
    }
}

fn parse_dimension_source(
    idx: usize,
    raw: RawDimensionSource,
) -> Result<DimensionSource, SessionStatsValidationError> {
    let path = format!("dimensions[{idx}].source");
    match raw.kind.as_str() {
        "column" => {
            let column = require_field(&path, "column", raw.column)?;
            Ok(DimensionSource::Column { column })
        }
        "flag_attr" => {
            let key = require_field(&path, "key", raw.key)?;
            Ok(DimensionSource::FlagAttr { key })
        }
        other => Err(SessionStatsValidationError::new(
            "unknown_source_kind",
            format!("{path}.kind"),
            format!("unknown dimension source kind '{other}'"),
        )),
    }
}

fn require_field(
    path: &str,
    field: &str,
    value: Option<String>,
) -> Result<String, SessionStatsValidationError> {
    let v = value.ok_or_else(|| {
        SessionStatsValidationError::new(
            "missing_field",
            format!("{path}.{field}"),
            format!("required field '{field}' is missing"),
        )
    })?;
    if v.trim().is_empty() {
        return Err(SessionStatsValidationError::new(
            "empty_field",
            format!("{path}.{field}"),
            format!("field '{field}' must be non-empty"),
        ));
    }
    Ok(v)
}

fn validate_identifier(path: &str, value: &str) -> Result<(), SessionStatsValidationError> {
    let ok = !value.is_empty()
        && value.chars().enumerate().all(|(i, c)| {
            if i == 0 {
                c.is_ascii_lowercase() || c == '_'
            } else {
                c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'
            }
        });
    if !ok {
        return Err(SessionStatsValidationError::new(
            "invalid_identifier",
            path,
            "must match [a-z_][a-z0-9_]*",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_default_parses_with_core_measures_and_ops() {
        let m = builtin_session_stats_manifest();
        assert!(m.key.iter().any(|k| k == "session_id"));
        let names: Vec<_> = m.measures.iter().map(|x| x.name.as_str()).collect();
        assert!(names.contains(&"observation_count"));
        assert!(names.contains(&"error_count"));
        assert!(names.contains(&"total_tokens"));
        assert!(names.contains(&"total_cost"));
        assert!(names.contains(&"start_time"));
        assert!(names.contains(&"end_time"));
        for measure in &m.measures {
            assert!(
                matches!(
                    measure.op,
                    MergeOp::Sum | MergeOp::Min | MergeOp::Max | MergeOp::Any
                ),
                "op for {}",
                measure.name
            );
            assert!(
                !measure.map_backed,
                "builtin core measure {} should not be map-backed",
                measure.name
            );
        }
        assert!(m.dimensions.iter().any(|d| d.name == "agent_name"));
        assert!(m.dimensions.iter().any(|d| d.name == "is_nested_child"));
        let nested = m
            .dimensions
            .iter()
            .find(|d| d.name == "is_nested_child")
            .unwrap();
        assert!(matches!(
            &nested.source,
            DimensionSource::FlagAttr { key } if key == "sp.metadata.opencode.parentSessionID"
        ));
    }

    #[test]
    fn include_str_builtin_equals_default_yaml_file() {
        let from_const = parse_session_stats_manifest(BUILTIN_SESSION_STATS_YAML).unwrap();
        let file = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/docs/session_stats/default.yaml"
        ))
        .expect("default.yaml must exist");
        let from_file = parse_session_stats_manifest(&file).unwrap();
        assert_eq!(from_const, from_file);
        assert_eq!(builtin_session_stats_manifest(), from_file);
    }

    #[test]
    fn rejects_unknown_spec_version() {
        let err = parse_session_stats_manifest(
            "specVersion: softprobe.session_stats.v0\nkey: [session_id]\nmeasures:\n  - name: observation_count\n    op: sum\n    source: { kind: count_rows }\n",
        )
        .expect_err("must reject");
        assert_eq!(err.code, "unsupported_spec_version");
    }

    #[test]
    fn rejects_unknown_op_uniq() {
        let err = parse_session_stats_manifest(
            r#"
specVersion: softprobe.session_stats.v1
key: [session_id]
measures:
  - name: users
    op: uniq
    source: { kind: column, column: user_id }
"#,
        )
        .expect_err("must reject uniq");
        assert_eq!(err.code, "unsupported_op");
    }

    #[test]
    fn rejects_unknown_source_kind() {
        let err = parse_session_stats_manifest(
            r#"
specVersion: softprobe.session_stats.v1
key: [session_id]
measures:
  - name: observation_count
    op: sum
    source: { kind: magic }
"#,
        )
        .expect_err("must reject");
        assert_eq!(err.code, "unknown_source_kind");
    }

    #[test]
    fn rejects_duplicate_measure_name() {
        let err = parse_session_stats_manifest(
            r#"
specVersion: softprobe.session_stats.v1
key: [session_id]
measures:
  - name: observation_count
    op: sum
    source: { kind: count_rows }
  - name: observation_count
    op: sum
    source: { kind: count_rows }
"#,
        )
        .expect_err("must reject");
        assert_eq!(err.code, "duplicate_measure");
    }

    #[test]
    fn rejects_missing_session_id_key() {
        let err = parse_session_stats_manifest(
            r#"
specVersion: softprobe.session_stats.v1
key: [trace_id]
measures:
  - name: observation_count
    op: sum
    source: { kind: count_rows }
"#,
        )
        .expect_err("must reject");
        assert_eq!(err.code, "missing_session_id_key");
    }

    #[test]
    fn extra_measure_is_map_backed() {
        let m = parse_session_stats_manifest(
            r#"
specVersion: softprobe.session_stats.v1
key: [session_id]
measures:
  - name: observation_count
    op: sum
    source: { kind: count_rows }
  - name: tool_calls
    op: sum
    source: { kind: column, column: tool_call_count }
"#,
        )
        .expect("parse");
        let tool = m.measures.iter().find(|x| x.name == "tool_calls").unwrap();
        assert!(tool.map_backed);
        assert!(matches!(
            &tool.source,
            MeasureSource::Column { column } if column == "tool_call_count"
        ));
        let obs = m
            .measures
            .iter()
            .find(|x| x.name == "observation_count")
            .unwrap();
        assert!(!obs.map_backed);
    }

    #[test]
    fn flag_attr_dimension_parses() {
        let m = parse_session_stats_manifest(
            r#"
specVersion: softprobe.session_stats.v1
key: [session_id]
measures:
  - name: observation_count
    op: sum
    source: { kind: count_rows }
dimensions:
  - name: is_nested_child
    source:
      kind: flag_attr
      key: sp.metadata.opencode.parentSessionID
"#,
        )
        .expect("parse");
        assert_eq!(m.dimensions.len(), 1);
    }

    #[test]
    fn builtin_load_is_idempotent() {
        let a = builtin_session_stats_manifest();
        let b = builtin_session_stats_manifest();
        assert_eq!(a, b);
    }

    fn span(session: &str, trace: &str, status: Option<&str>) -> Span {
        use chrono::TimeZone;
        Span {
            session_id: session.to_string(),
            trace_id: trace.to_string(),
            span_id: format!("span-{trace}"),
            parent_span_id: None,
            app_id: "app".into(),
            organization_id: None,
            tenant_id: None,
            agent_id: None,
            agent_name: None,
            message_type: "span".into(),
            span_kind: None,
            timestamp: Utc.with_ymd_and_hms(2026, 1, 2, 12, 0, 0).unwrap(),
            end_timestamp: Some(Utc.with_ymd_and_hms(2026, 1, 2, 12, 0, 5).unwrap()),
            attributes: Default::default(),
            resource_attributes: Default::default(),
            events: vec![],
            status_code: status.map(str::to_string),
            status_message: None,
            http_request_method: None,
            http_request_path: None,
            http_request_headers: None,
            http_request_body: None,
            http_response_status_code: None,
            http_response_headers: None,
            http_response_body: None,
        }
    }

    #[test]
    fn derive_single_session_multi_span_sums_tokens_and_errors() {
        let mut a = span("s1", "t1", Some("ERROR"));
        a.attributes.insert("total_tokens".into(), "10".into());
        a.attributes.insert("total_cost".into(), "0.1".into());
        a.attributes.insert("input_tokens".into(), "4".into());
        a.attributes.insert("output_tokens".into(), "6".into());
        a.agent_name = Some("Refund".into());
        let mut b = span("s1", "t2", None);
        b.timestamp = a.timestamp + chrono::Duration::seconds(10);
        b.end_timestamp = Some(b.timestamp + chrono::Duration::seconds(2));
        b.attributes.insert("total_tokens".into(), "20".into());
        b.attributes.insert("total_cost".into(), "0.2".into());

        let rows = derive_session_deltas(&[a, b], &builtin_session_stats_manifest());
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.session_id, "s1");
        assert_eq!(row.observation_count, 2);
        assert_eq!(row.error_count, 1);
        assert_eq!(row.trace_count, 2);
        assert_eq!(row.total_tokens, 30);
        assert!((row.total_cost - 0.3).abs() < 1e-9);
        assert_eq!(row.input_tokens, 4);
        assert_eq!(row.output_tokens, 6);
        assert_eq!(row.agent_name.as_deref(), Some("Refund"));
        assert!(!row.is_nested_child);
    }

    #[test]
    fn derive_two_sessions_and_excludes_empty_and_recording() {
        let ok = span("s1", "t1", None);
        let mut empty = span("", "t2", None);
        empty.session_id = "".into();
        let mut recording = span("s2", "t3", None);
        recording
            .attributes
            .insert("observation_type".into(), "recording".into());
        let other = span("s3", "t4", None);

        let rows = derive_session_deltas(
            &[ok, empty, recording, other],
            &builtin_session_stats_manifest(),
        );
        let ids: Vec<_> = rows.iter().map(|r| r.session_id.as_str()).collect();
        assert_eq!(ids, vec!["s1", "s3"]);
    }

    #[test]
    fn derive_nested_child_flag_from_agent_parent_meta() {
        let mut root = span("root", "t1", None);
        root.attributes
            .insert("observation_type".into(), "agent".into());
        let mut child = span("child", "t2", None);
        child
            .attributes
            .insert("observation_type".into(), "agent".into());
        child
            .attributes
            .insert("sp.metadata.opencode.parentSessionID".into(), "root".into());
        let rows = derive_session_deltas(&[root, child], &builtin_session_stats_manifest());
        let by_id: std::collections::HashMap<_, _> = rows
            .into_iter()
            .map(|r| (r.session_id.clone(), r))
            .collect();
        assert!(!by_id["root"].is_nested_child);
        assert!(by_id["child"].is_nested_child);
    }

    #[test]
    fn derive_map_backed_extra_measure() {
        let manifest = parse_session_stats_manifest(
            r#"
specVersion: softprobe.session_stats.v1
key: [session_id]
measures:
  - name: observation_count
    op: sum
    source: { kind: count_rows }
  - name: tool_calls
    op: sum
    source: { kind: column, column: tool_call_count }
"#,
        )
        .unwrap();
        let mut a = span("s1", "t1", None);
        a.attributes.insert("tool_call_count".into(), "1".into());
        let mut b = span("s1", "t1", None);
        b.attributes.insert("tool_call_count".into(), "2".into());
        let rows = derive_session_deltas(&[a, b], &manifest);
        assert_eq!(rows[0].measures.get("tool_calls"), Some(&3.0));
    }

    #[test]
    fn derive_multiple_batches_produce_separate_rows() {
        let a = span("s1", "t1", None);
        let b = span("s1", "t2", None);
        let r1 = derive_session_deltas(&[a], &builtin_session_stats_manifest());
        let r2 = derive_session_deltas(&[b], &builtin_session_stats_manifest());
        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
        assert_eq!(r1[0].observation_count, 1);
        assert_eq!(r2[0].observation_count, 1);
    }

    #[test]
    fn delta_rows_to_record_batch_round_trips_core_fields() {
        use crate::storage::schema::SessionStatsDeltaTable;
        let mut row_span = span("s1", "t1", Some("ERROR"));
        row_span
            .attributes
            .insert("total_tokens".into(), "5".into());
        let rows = derive_session_deltas(&[row_span], &builtin_session_stats_manifest());
        let batch =
            session_stats_deltas_to_record_batch(&rows, &SessionStatsDeltaTable::schema()).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.num_columns(),
            SessionStatsDeltaTable::schema().fields().len()
        );
    }

    #[test]
    fn derive_missing_tokens_and_cost_treat_as_zero() {
        let a = span("s1", "t1", None);
        let rows = derive_session_deltas(&[a], &builtin_session_stats_manifest());
        assert_eq!(rows[0].total_tokens, 0);
        assert_eq!(rows[0].input_tokens, 0);
        assert_eq!(rows[0].output_tokens, 0);
        assert_eq!(rows[0].total_cost, 0.0);
    }

    #[test]
    fn rejects_empty_key_list() {
        let err = parse_session_stats_manifest(
            r#"
specVersion: softprobe.session_stats.v1
key: []
measures:
  - name: observation_count
    op: sum
    source: { kind: count_rows }
"#,
        )
        .expect_err("must reject");
        assert_eq!(err.code, "missing_session_id_key");
    }
}
