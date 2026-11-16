//! Typed DuckLake trace query backend for the Tempo adapter.

use super::traces::{
    persisted_status_code_numeric_value, visible_span_attributes, TraceAttribute, TraceData,
    TraceEvent, TraceLookupBounds, TraceQueryBackend, TraceSearchHit, TraceSearchRequest,
    TraceServiceStats, TraceSpan, TraceSpanSet, TraceSpanSummary,
};
use crate::compat::errors::{CompatError, CompatErrorCode};
use crate::compat::projection::tempo::{project_tempo_link_attributes, project_tempo_tags};
use crate::compat::tempo::traceql::{is_numeric_field, TraceField, TracePredicate, TraceSelector};
use crate::compat::workspace::CompatWorkspaceContext;
use crate::query::{QueryEngine, QueryResult};
use crate::sql::tempo::{
    scan_reached_cap, tag_event_link_rows_sql, tag_names_map_keys_sql, tag_values_sql,
    trace_scan_cap, trace_scan_sql,
};
use crate::sql::trusted::TrustedSql;
use crate::storage::schema::attribute_map::attribute_map_json_to_string_map;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

pub struct DuckLakeTraceBackend {
    query: Arc<QueryEngine>,
    tag_index: Option<crate::tag_index::TagIndexReader>,
}

impl DuckLakeTraceBackend {
    pub fn new(query: Arc<QueryEngine>) -> Self {
        Self {
            query,
            tag_index: None,
        }
    }

    pub fn with_tag_index(
        query: Arc<QueryEngine>,
        tag_index: crate::tag_index::TagIndexReader,
    ) -> Self {
        Self {
            query,
            tag_index: Some(tag_index),
        }
    }

    async fn execute(
        &self,
        ctx: &CompatWorkspaceContext,
        trusted: TrustedSql,
    ) -> Result<QueryResult, CompatError> {
        if ctx.remaining().is_zero() {
            return Err(deadline());
        }
        match tokio::time::timeout(ctx.remaining(), self.query.execute_trusted(trusted)).await {
            Err(_) => Err(deadline()),
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => {
                let message = err.to_string();
                if message.contains("Table with name traces does not exist") {
                    Ok(QueryResult {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        row_count: 0,
                    })
                } else if message.starts_with("Interrupt Error")
                    || message.contains("DuckDB query cancelled")
                {
                    Err(deadline())
                } else {
                    Err(CompatError::new(
                        CompatErrorCode::BadRequest,
                        format!("trace query failed: {message}"),
                    ))
                }
            }
        }
    }

    async fn scan(
        &self,
        ctx: &CompatWorkspaceContext,
        request: &TraceSearchRequest,
        trace_id: Option<&str>,
    ) -> Result<Vec<TraceSpan>, CompatError> {
        if let (Some(start), Some(end)) = (request.start_ns, request.end_ns) {
            if start == end {
                return Ok(Vec::new());
            }
        }
        let trusted = trace_scan_sql(request.scan_params(), trace_id)
            .map_err(|msg| CompatError::new(CompatErrorCode::BadRequest, msg))?;
        let result = self.execute(ctx, trusted).await?;
        let scan_cap = trace_scan_cap(request.limit);
        if scan_reached_cap(result.rows.len(), scan_cap) {
            return Err(CompatError::new(
                CompatErrorCode::LimitExceeded,
                format!(
                    "trace scan reaches bounded scan cap {} before post-filtering",
                    scan_cap
                ),
            ));
        }
        parse_rows(&result)
    }

    fn tags_for(&self, span: &TraceSpan, max_tags: usize) -> BTreeMap<String, String> {
        let mut resource = span
            .resource_attributes
            .iter()
            .map(|attribute| (attribute.key.clone(), attribute.value.clone()))
            .collect::<HashMap<_, _>>();
        if let Some(service) = &span.service_name {
            resource
                .entry("service.name".into())
                .or_insert_with(|| service.clone());
        }
        let attributes = visible_span_attributes(span)
            .iter()
            .map(|attribute| (attribute.key.clone(), attribute.value.clone()))
            .collect::<HashMap<_, _>>();
        let mut additional = HashMap::new();
        for event in &span.events {
            for attribute in &event.attributes {
                additional.insert(attribute.key.clone(), attribute.value.clone());
            }
        }
        additional.extend(project_tempo_link_attributes(&span.links));
        project_tempo_tags(&resource, &attributes, &additional, max_tags)
    }

    fn matches_tags(&self, span: &TraceSpan, request: &TraceSearchRequest) -> bool {
        if request.tags.is_empty() {
            return true;
        }
        let tags = self.tags_for(span, usize::MAX);
        request.tags.iter().all(|(key, expected)| {
            tags.get(key)
                .map(|actual| {
                    actual
                        .to_ascii_lowercase()
                        .contains(&expected.to_ascii_lowercase())
                })
                .unwrap_or(false)
        })
    }

    fn matches_selector(selector: &TraceSelector, span: &TraceSpan) -> bool {
        match selector {
            TraceSelector::All => true,
            TraceSelector::Predicate(predicate) => Self::matches_predicate(predicate, span),
            TraceSelector::And(left, right) => {
                Self::matches_selector(left, span) && Self::matches_selector(right, span)
            }
            TraceSelector::Or(left, right) => {
                Self::matches_selector(left, span) || Self::matches_selector(right, span)
            }
        }
    }

    fn matches_predicate(predicate: &TracePredicate, span: &TraceSpan) -> bool {
        use crate::compat::tempo::traceql::{canonical_status_code, is_status_field};

        let (field, operation, expected) = match predicate {
            TracePredicate::Eq(field, value) => (field, "eq", value),
            TracePredicate::NotEq(field, value) => (field, "ne", value),
            TracePredicate::Regex(field, value) => (field, "regex", value),
            TracePredicate::NotRegex(field, value) => (field, "not_regex", value),
            TracePredicate::Greater(field, value) => (field, "gt", value),
            TracePredicate::GreaterOrEqual(field, value) => (field, "gte", value),
            TracePredicate::Less(field, value) => (field, "lt", value),
            TracePredicate::LessOrEqual(field, value) => (field, "lte", value),
        };
        let actual = field_value(field, span);
        if is_status_field(field) {
            let actual_val = actual.as_deref().unwrap_or_default();
            let actual_code = canonical_status_code(actual_val);
            let expected_code = canonical_status_code(expected.as_str());
            match operation {
                "eq" => {
                    if let (Some(a), Some(e)) = (actual_code, expected_code) {
                        a == e
                    } else if let Some(a) = actual_val.strip_prefix("STATUS_CODE_") {
                        a.eq_ignore_ascii_case(expected.as_str())
                    } else {
                        actual_val.eq_ignore_ascii_case(expected.as_str())
                    }
                }
                "ne" => {
                    if let (Some(a), Some(e)) = (actual_code, expected_code) {
                        a != e
                    } else if let Some(a) = actual_val.strip_prefix("STATUS_CODE_") {
                        !a.eq_ignore_ascii_case(expected.as_str())
                    } else {
                        !actual_val.eq_ignore_ascii_case(expected.as_str())
                    }
                }
                "regex" | "not_regex" => {
                    let canonical_name = match actual_code {
                        Some(0) => "unset",
                        Some(1) => "ok",
                        Some(2) => "error",
                        _ => actual_val,
                    };
                    let matched = regex::Regex::new(expected.as_str())
                        .ok()
                        .map(|re| re.is_match(actual_val) || re.is_match(canonical_name))
                        .unwrap_or(false);
                    if operation == "regex" {
                        matched
                    } else {
                        !matched
                    }
                }
                "gt" | "gte" | "lt" | "lte" => {
                    let a = actual_code.or_else(|| actual_val.parse::<i64>().ok());
                    let e = expected_code.or_else(|| expected.as_str().parse::<i64>().ok());
                    match (a, e) {
                        (Some(a), Some(e)) => match operation {
                            "gt" => a > e,
                            "gte" => a >= e,
                            "lt" => a < e,
                            _ => a <= e,
                        },
                        _ => false,
                    }
                }
                _ => false,
            }
        } else {
            match operation {
                "eq" => actual.as_deref() == Some(expected.as_str()),
                "ne" => actual
                    .as_deref()
                    .is_some_and(|value| value != expected.as_str()),
                "regex" | "not_regex" => {
                    let matched = actual
                        .as_deref()
                        .and_then(|value| {
                            regex::Regex::new(expected.as_str())
                                .ok()
                                .map(|re| re.is_match(value))
                        })
                        .unwrap_or(false);
                    if operation == "regex" {
                        matched
                    } else {
                        !matched
                    }
                }
                "gt" | "gte" | "lt" | "lte" => actual
                    .as_deref()
                    .and_then(|value| compare_value(field, value, expected.as_str()))
                    .map(|ordering| match operation {
                        "gt" => ordering.is_gt(),
                        "gte" => ordering.is_ge(),
                        "lt" => ordering.is_lt(),
                        _ => ordering.is_le(),
                    })
                    .unwrap_or(false),
                _ => false,
            }
        }
    }

    async fn event_link_tags(
        &self,
        ctx: &CompatWorkspaceContext,
        bounds: TraceLookupBounds,
    ) -> Result<Vec<(String, String)>, CompatError> {
        let trusted =
            tag_event_link_rows_sql(bounds.start_ns, bounds.end_ns, ctx.limits.max_series)
                .map_err(|msg| CompatError::new(CompatErrorCode::BadRequest, msg))?;
        let result = self.execute(ctx, trusted).await?;
        if result.rows.is_empty() {
            return Ok(Vec::new());
        }
        let events_col = result.columns.iter().position(|c| c == "events");
        let links_col = result.columns.iter().position(|c| c == "links");
        let mut pairs = Vec::new();
        for (row_index, row) in result.rows.iter().enumerate() {
            if let Some(value) = optional_cell(row, events_col, row_index, "events")? {
                for event in parse_events(value, 0, row_index)? {
                    for attr in event.attributes {
                        if !attr.key.is_empty() {
                            pairs.push((attr.key, attr.value));
                        }
                    }
                }
            }
            if let Some(value) = optional_cell(row, links_col, row_index, "links")? {
                let links = parse_links(value, row_index)?;
                for (key, val) in project_tempo_link_attributes(&links) {
                    if !key.is_empty() {
                        pairs.push((key, val));
                    }
                }
            }
        }
        Ok(pairs)
    }
}

const TRAILING_WINDOW_SLACK_NS: i64 = 120_000_000_000;

fn trailing_tag_index_min_last_seen_ns(bounds: TraceLookupBounds, now_ns: i64) -> Option<i64> {
    if let Some(end) = bounds.end_ns {
        if end < now_ns.saturating_sub(TRAILING_WINDOW_SLACK_NS) {
            return None;
        }
    }
    Some(bounds.start_ns.map_or(i64::MIN, |start| {
        start.saturating_sub(crate::tag_index::TAG_FLUSH_WINDOW_NS)
    }))
}

async fn try_read_tag_index(
    tag_index: Option<&crate::tag_index::TagIndexReader>,
    bounds: TraceLookupBounds,
    max_series: usize,
) -> Option<Vec<String>> {
    let reader = tag_index?;
    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX);
    let min_last_seen_ns = trailing_tag_index_min_last_seen_ns(bounds, now_ns)?;
    match reader.list_since(min_last_seen_ns, max_series).await {
        Ok(Some(tags)) => Some(tags),
        Ok(None) => None,
        Err(err) => {
            tracing::warn!(error = %err, "tag_index read failed; falling back to DuckLake scan");
            None
        }
    }
}

async fn should_scan_event_link_tags(
    tag_index: Option<&crate::tag_index::TagIndexReader>,
    tag: &str,
) -> bool {
    let Some(reader) = tag_index else {
        return true;
    };
    match reader.source_mask(tag).await {
        Ok(Some(mask)) => (mask & crate::tag_index::TAG_SOURCE_EVENT_OR_LINK) != 0,
        Ok(None) => true,
        Err(err) => {
            tracing::warn!(error = %err, "tag_index source_mask failed; scanning event/link tags");
            true
        }
    }
}

#[async_trait]
impl TraceQueryBackend for DuckLakeTraceBackend {
    async fn get_trace(
        &self,
        ctx: &CompatWorkspaceContext,
        trace_id: &str,
        bounds: TraceLookupBounds,
    ) -> Result<Option<TraceData>, CompatError> {
        let request = TraceSearchRequest {
            tags: BTreeMap::new(),
            selector: None,
            min_duration_ns: None,
            max_duration_ns: None,
            start_ns: bounds.start_ns,
            end_ns: bounds.end_ns,
            limit: ctx.limits.max_series,
        };
        let spans = self.scan(ctx, &request, Some(trace_id)).await?;
        if spans.is_empty() {
            return Ok(None);
        }
        Ok(Some(TraceData { spans }))
    }

    async fn search(
        &self,
        ctx: &CompatWorkspaceContext,
        request: TraceSearchRequest,
    ) -> Result<Vec<TraceSearchHit>, CompatError> {
        let spans = self.scan(ctx, &request, None).await?;
        let mut by_trace: BTreeMap<String, Vec<TraceSpan>> = BTreeMap::new();
        for span in spans {
            by_trace
                .entry(span.trace_id.clone())
                .or_default()
                .push(span);
        }

        let mut hits = Vec::new();
        for (trace_id, spans) in by_trace {
            let matched_spans = spans
                .iter()
                .filter(|span| {
                    self.matches_tags(span, &request)
                        && request
                            .selector
                            .as_ref()
                            .map(|selector| Self::matches_selector(selector, span))
                            .unwrap_or(true)
                })
                .collect::<Vec<_>>();
            if matched_spans.is_empty() {
                continue;
            }
            let start = spans
                .iter()
                .map(|span| span.start_time_unix_nano)
                .min()
                .unwrap_or(0);
            let end = spans
                .iter()
                .map(|span| span.end_time_unix_nano.unwrap_or(span.start_time_unix_nano))
                .max()
                .unwrap_or(start);
            let duration = end.saturating_sub(start);
            if request.min_duration_ns.is_some_and(|min| duration < min)
                || request.max_duration_ns.is_some_and(|max| duration > max)
            {
                continue;
            }
            let span_ids = spans
                .iter()
                .map(|span| span.span_id.as_str())
                .collect::<BTreeSet<_>>();
            let root = spans
                .iter()
                .find(|span| span.parent_span_id.is_none())
                .or_else(|| {
                    spans.iter().find(|span| {
                        span.parent_span_id
                            .as_deref()
                            .is_some_and(|pid| !span_ids.contains(pid))
                    })
                })
                .or_else(|| spans.first());
            let orphan_parent = root.and_then(|span| {
                span.parent_span_id
                    .as_deref()
                    .filter(|pid| !span_ids.contains(*pid))
            });
            let service_stats = summarize_service_stats(&spans);
            let span_set = build_span_set(&matched_spans, root, orphan_parent);
            hits.push(TraceSearchHit {
                trace_id,
                root_service_name: root.and_then(|span| span.service_name.clone()),
                root_trace_name: root.map(|span| span.name.clone()),
                start_time_unix_nano: start,
                duration_ms: duration / 1_000_000,
                service_stats,
                span_set,
            });
        }
        if hits.len() > request.limit {
            hits.sort_by(|left, right| {
                right
                    .start_time_unix_nano
                    .cmp(&left.start_time_unix_nano)
                    .then_with(|| right.trace_id.cmp(&left.trace_id))
            });
            hits.truncate(request.limit);
        }
        hits.sort_by(|left, right| {
            left.start_time_unix_nano
                .cmp(&right.start_time_unix_nano)
                .then_with(|| left.trace_id.cmp(&right.trace_id))
        });
        Ok(hits)
    }

    async fn search_tags(
        &self,
        ctx: &CompatWorkspaceContext,
        bounds: TraceLookupBounds,
    ) -> Result<Vec<String>, CompatError> {
        if let (Some(start), Some(end)) = (bounds.start_ns, bounds.end_ns) {
            if start == end {
                return Ok(Vec::new());
            }
            if start > end {
                return Err(CompatError::new(
                    CompatErrorCode::BadRequest,
                    "`start` must be <= `end`",
                ));
            }
        }
        if let Some(tags) =
            try_read_tag_index(self.tag_index.as_ref(), bounds, ctx.limits.max_series).await
        {
            return Ok(tags);
        }
        let trusted = tag_names_map_keys_sql(bounds.start_ns, bounds.end_ns, ctx.limits.max_series)
            .map_err(|msg| CompatError::new(CompatErrorCode::BadRequest, msg))?;
        let result = self.execute(ctx, trusted).await?;
        let mut names = BTreeSet::new();
        for row in &result.rows {
            if let Some(key) = row.first().and_then(cell_string) {
                if !key.is_empty()
                    && key != crate::models::span::INSTRUMENTATION_SCOPE_ATTRIBUTE
                    && key != crate::models::span::LINKS_ATTRIBUTE
                {
                    names.insert(key);
                }
            }
        }
        for (key, _) in self.event_link_tags(ctx, bounds).await? {
            names.insert(key);
        }
        Ok(names.into_iter().take(ctx.limits.max_series).collect())
    }

    async fn search_tag_values(
        &self,
        ctx: &CompatWorkspaceContext,
        tag: &str,
        bounds: TraceLookupBounds,
    ) -> Result<Vec<String>, CompatError> {
        if let (Some(start), Some(end)) = (bounds.start_ns, bounds.end_ns) {
            if start == end {
                return Ok(Vec::new());
            }
            if start > end {
                return Err(CompatError::new(
                    CompatErrorCode::BadRequest,
                    "`start` must be <= `end`",
                ));
            }
        }
        let trusted = tag_values_sql(tag, bounds.start_ns, bounds.end_ns, ctx.limits.max_series)
            .map_err(|msg| CompatError::new(CompatErrorCode::BadRequest, msg))?;
        let result = self.execute(ctx, trusted).await?;
        let mut values = BTreeSet::new();
        for row in &result.rows {
            if let Some(value) = row.first().and_then(cell_string) {
                values.insert(value);
            }
        }
        if should_scan_event_link_tags(self.tag_index.as_ref(), tag).await {
            for (key, value) in self.event_link_tags(ctx, bounds).await? {
                if key == tag {
                    values.insert(value);
                }
            }
        }
        Ok(values.into_iter().take(ctx.limits.max_series).collect())
    }
}

fn parse_rows(result: &QueryResult) -> Result<Vec<TraceSpan>, CompatError> {
    if result.rows.is_empty() {
        return Ok(Vec::new());
    }

    let index = |name: &str| result.columns.iter().position(|column| column == name);
    let (
        Some(trace_id),
        Some(span_id),
        parent_span_id,
        Some(name),
        kind,
        Some(app_id),
        Some(start),
        end,
        attributes,
        resource_attributes,
        instrumentation_scope,
        links,
        status_code,
        status_message,
        events,
    ) = (
        index("trace_id"),
        index("span_id"),
        index("parent_span_id"),
        index("message_type"),
        index("span_kind"),
        index("app_id"),
        index("start_time_unix_nano"),
        index("end_time_unix_nano"),
        index("attributes"),
        index("resource_attributes"),
        index("instrumentation_scope"),
        index("links"),
        index("status_code"),
        index("status_message"),
        index("events"),
    )
    else {
        return Err(malformed_rows(
            "trace query result is missing required columns",
        ));
    };
    let agent_id_col = index("agent_id");
    let agent_name_col = index("agent_name");
    let session_id_col = index("session_id");
    let session_attr_col = index("session_attr_id");

    result
        .rows
        .iter()
        .enumerate()
        .map(|(row_index, row)| {
            let trace_id = required_string(row, trace_id, row_index, "trace_id")?;
            let span_id = required_string(row, span_id, row_index, "span_id")?;
            let name = required_string(row, name, row_index, "message_type")?;
            let start_time_unix_nano = required_i64(row, start, row_index, "start_time_unix_nano")?;
            let end_time_unix_nano = optional_i64(row, end, row_index, "end_timestamp")?;
            let mut attributes = optional_cell(row, attributes, row_index, "attributes")?
                .map(|value| parse_attributes(value, row_index, "attributes"))
                .transpose()?
                .unwrap_or_default();
            let mut resource_attributes =
                optional_cell(row, resource_attributes, row_index, "resource_attributes")?
                    .map(|value| parse_attributes(value, row_index, "resource_attributes"))
                    .transpose()?
                    .unwrap_or_default();
            let row_agent_id = agent_id_col
                .and_then(|idx| row.get(idx))
                .and_then(cell_string)
                .filter(|v| !v.is_empty());
            let row_session_id = session_id_col
                .and_then(|idx| row.get(idx))
                .and_then(cell_string)
                .unwrap_or_default();
            if let Some(aid) = row_agent_id {
                if !resource_attributes.iter().any(|a| a.key == "sp.agent.id") {
                    resource_attributes.push(TraceAttribute {
                        key: "sp.agent.id".into(),
                        value: aid,
                    });
                }
                if let Some(aname) = agent_name_col
                    .and_then(|idx| row.get(idx))
                    .and_then(cell_string)
                    .filter(|v| !v.is_empty())
                {
                    if !resource_attributes.iter().any(|a| a.key == "sp.agent.name") {
                        resource_attributes.push(TraceAttribute {
                            key: "sp.agent.name".into(),
                            value: aname,
                        });
                    }
                }
                resource_attributes.sort_by(|left, right| left.key.cmp(&right.key));

                let stored_session = (!row_session_id.is_empty())
                    .then_some(row_session_id)
                    .or_else(|| {
                        session_attr_col
                            .and_then(|idx| row.get(idx))
                            .and_then(cell_string)
                            .filter(|v| !v.is_empty())
                    });
                if let Some(sid) = stored_session {
                    upsert_attribute(&mut attributes, "sp.session.id", sid);
                }
            }
            Ok(TraceSpan {
                trace_id,
                span_id,
                parent_span_id: parent_span_id
                    .and_then(|idx| row.get(idx))
                    .and_then(cell_string)
                    .filter(|v| !v.is_empty()),
                name,
                kind: kind.and_then(|idx| row.get(idx)).and_then(cell_string),
                start_time_unix_nano,
                end_time_unix_nano,
                attributes,
                resource_attributes,
                instrumentation_scope: optional_cell(
                    row,
                    instrumentation_scope,
                    row_index,
                    "instrumentation_scope",
                )?
                .map(|value| parse_json_object(value, row_index, "instrumentation_scope"))
                .transpose()?,
                status_code: status_code
                    .and_then(|idx| row.get(idx))
                    .and_then(cell_string),
                status_message: status_message
                    .and_then(|idx| row.get(idx))
                    .and_then(cell_string),
                links: optional_cell(row, links, row_index, "links")?
                    .map(|value| parse_links(value, row_index))
                    .transpose()?
                    .unwrap_or_default(),
                events: optional_cell(row, events, row_index, "events")?
                    .map(|value| parse_events(value, start_time_unix_nano, row_index))
                    .transpose()?
                    .unwrap_or_default(),
                service_name: row.get(app_id).and_then(cell_string),
            })
        })
        .collect()
}

fn required_string(
    row: &[Value],
    column: usize,
    row_index: usize,
    field: &str,
) -> Result<String, CompatError> {
    match row.get(column) {
        Some(Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        _ => Err(malformed_row(row_index, field)),
    }
}

fn required_i64(
    row: &[Value],
    column: usize,
    row_index: usize,
    field: &str,
) -> Result<i64, CompatError> {
    row.get(column)
        .and_then(cell_i64)
        .ok_or_else(|| malformed_row(row_index, field))
}

fn optional_cell<'a>(
    row: &'a [Value],
    column: Option<usize>,
    row_index: usize,
    field: &str,
) -> Result<Option<&'a Value>, CompatError> {
    let Some(column) = column else {
        return Ok(None);
    };
    match row.get(column) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() || value.trim() == "null" => Ok(None),
        Some(value) => Ok(Some(value)),
        None => Err(malformed_row(row_index, field)),
    }
}

fn optional_i64(
    row: &[Value],
    column: Option<usize>,
    row_index: usize,
    field: &str,
) -> Result<Option<i64>, CompatError> {
    optional_cell(row, column, row_index, field)?
        .map(|value| cell_i64(value).ok_or_else(|| malformed_row(row_index, field)))
        .transpose()
}

fn malformed_row(row_index: usize, field: &str) -> CompatError {
    malformed_rows(format!(
        "trace query row {row_index} has invalid required field '{field}'"
    ))
}

fn malformed_rows(message: impl Into<String>) -> CompatError {
    CompatError::new(CompatErrorCode::BadRequest, message)
}

fn parse_attributes(
    value: &Value,
    row_index: usize,
    field: &str,
) -> Result<Vec<TraceAttribute>, CompatError> {
    let value = parse_json_object(value, row_index, field)?;
    let mut values = attribute_map_json_to_string_map(&value)
        .into_iter()
        .filter(|(key, _)| {
            key != crate::models::span::INSTRUMENTATION_SCOPE_ATTRIBUTE
                && key != crate::models::span::LINKS_ATTRIBUTE
        })
        .map(|(key, value)| TraceAttribute { key, value })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(values)
}

fn parse_events(
    value: &Value,
    fallback_timestamp: i64,
    row_index: usize,
) -> Result<Vec<TraceEvent>, CompatError> {
    let value = strict_json_value(value, row_index, "events")?;
    let Value::Array(events) = value else {
        return Err(malformed_row(row_index, "events"));
    };
    events
        .into_iter()
        .map(|event| {
            let Value::Object(event) = event else {
                return Err(malformed_row(row_index, "events"));
            };
            let name = event
                .get("name")
                .and_then(|value| match value {
                    Value::String(value) if !value.is_empty() => Some(value.clone()),
                    _ => None,
                })
                .ok_or_else(|| malformed_row(row_index, "events"))?;
            let timestamp_unix_nano = event
                .get("timestamp")
                .and_then(|value| parse_timestamp_value(value, fallback_timestamp))
                .ok_or_else(|| malformed_row(row_index, "events"))?;
            let attributes = event
                .get("attributes")
                .filter(|value| !value.is_null())
                .map(|value| parse_attributes(value, row_index, "events"))
                .transpose()?
                .unwrap_or_default();
            Ok(TraceEvent {
                name,
                timestamp_unix_nano,
                attributes,
            })
        })
        .collect()
}

fn parse_json_object(value: &Value, row_index: usize, field: &str) -> Result<Value, CompatError> {
    let value = strict_json_value(value, row_index, field)?;
    let value = crate::storage::schema::parse_projected_json_value(value);
    if value.is_object() {
        Ok(value)
    } else {
        Err(malformed_row(row_index, field))
    }
}

fn strict_json_value(value: &Value, row_index: usize, field: &str) -> Result<Value, CompatError> {
    match value {
        Value::String(text) => {
            serde_json::from_str(text).map_err(|_| malformed_row(row_index, field))
        }
        other => Ok(other.clone()),
    }
}

fn upsert_attribute(attributes: &mut Vec<TraceAttribute>, key: &str, value: String) {
    if let Some(existing) = attributes.iter_mut().find(|a| a.key == key) {
        existing.value = value;
    } else {
        attributes.push(TraceAttribute {
            key: key.to_string(),
            value,
        });
        attributes.sort_by(|left, right| left.key.cmp(&right.key));
    }
}

fn is_http_body_helper(span: &TraceSpan) -> bool {
    span.name == "softprobe.http.body.capture"
        || span
            .attributes
            .iter()
            .any(|a| a.key == "sp.observation.type" && a.value == "http_body")
}

fn is_error_span(span: &TraceSpan) -> bool {
    span.status_code
        .as_deref()
        .and_then(persisted_status_code_numeric_value)
        == Some(2)
        || span.attributes.iter().any(|a| {
            matches!(
                a.key.as_str(),
                "http.response.status_code" | "http.status_code"
            ) && a.value.parse::<i64>().is_ok_and(|code| code >= 500)
        })
}

fn summarize_service_stats(spans: &[TraceSpan]) -> BTreeMap<String, TraceServiceStats> {
    let non_helper = spans
        .iter()
        .filter(|span| !is_http_body_helper(span))
        .collect::<Vec<_>>();
    let source = if non_helper.is_empty() {
        spans.iter().collect::<Vec<_>>()
    } else {
        non_helper
    };
    let mut stats = BTreeMap::<String, TraceServiceStats>::new();
    for span in source {
        let service = span
            .service_name
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("unknown");
        let entry = stats.entry(service.to_string()).or_default();
        entry.span_count += 1;
        if is_error_span(span) {
            entry.error_count += 1;
        }
    }
    stats
}

fn build_span_set(
    matched_spans: &[&TraceSpan],
    root: Option<&TraceSpan>,
    orphan_parent: Option<&str>,
) -> TraceSpanSet {
    let non_helper_matched = matched_spans
        .iter()
        .copied()
        .filter(|span| !is_http_body_helper(span))
        .collect::<Vec<_>>();
    let candidates = if non_helper_matched.is_empty() {
        matched_spans.to_vec()
    } else {
        non_helper_matched
    };
    let matched = candidates.len();
    let mut chosen: Vec<&TraceSpan> = Vec::with_capacity(3);
    if let Some(root_span) = root {
        chosen.push(root_span);
    }
    for span in candidates.iter().copied().filter(|s| is_error_span(s)) {
        if chosen.len() >= 3 {
            break;
        }
        if !chosen
            .iter()
            .any(|existing| existing.span_id == span.span_id)
        {
            chosen.push(span);
        }
    }
    for span in &candidates {
        if chosen.len() >= 3 {
            break;
        }
        if !chosen
            .iter()
            .any(|existing| existing.span_id == span.span_id)
        {
            chosen.push(span);
        }
    }
    let summaries = chosen
        .into_iter()
        .map(|span| {
            let is_root = root.is_some_and(|r| r.span_id == span.span_id);
            let mut attrs = BTreeMap::<String, String>::new();
            if let Some(svc) = span.service_name.as_deref().filter(|s| !s.is_empty()) {
                attrs.insert("service.name".into(), svc.to_string());
            }
            for attr in &span.resource_attributes {
                if is_summary_safe_attribute(&attr.key, &attr.value) {
                    attrs.insert(attr.key.clone(), attr.value.clone());
                }
            }
            for attr in visible_span_attributes(span) {
                if is_summary_safe_attribute(&attr.key, &attr.value) {
                    attrs.insert(attr.key, attr.value);
                }
            }
            if is_root {
                if let Some(pid) = orphan_parent {
                    attrs.insert("sp.orphan_parent_span_id".into(), pid.to_string());
                }
            }
            TraceSpanSummary {
                span_id: span.span_id.clone(),
                name: span.name.clone(),
                start_time_unix_nano: span.start_time_unix_nano,
                duration_nanos: span
                    .end_time_unix_nano
                    .unwrap_or(span.start_time_unix_nano)
                    .saturating_sub(span.start_time_unix_nano),
                attributes: attrs
                    .into_iter()
                    .map(|(key, value)| TraceAttribute { key, value })
                    .collect(),
            }
        })
        .collect();
    TraceSpanSet {
        spans: summaries,
        matched,
    }
}

fn is_summary_safe_attribute(key: &str, value: &str) -> bool {
    value.len() <= 256
        && !matches!(
            key,
            "http.request.body.content"
                | "http.response.body.content"
                | "http.request_body"
                | "http.response_body"
                | "gen_ai.input.messages"
                | "gen_ai.output.messages"
                | "sp.rrweb.events"
        )
}

fn span_or_session_attr(span: &TraceSpan, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key == key)
        .map(|attribute| attribute.value.clone())
        .or_else(|| {
            if key == "session.id" {
                span.attributes
                    .iter()
                    .find(|attribute| attribute.key == "sp.session.id")
                    .map(|attribute| attribute.value.clone())
            } else if key == "sp.session.id" {
                span.attributes
                    .iter()
                    .find(|attribute| attribute.key == "session.id")
                    .map(|attribute| attribute.value.clone())
            } else {
                None
            }
        })
}

fn field_value(field: &TraceField, span: &TraceSpan) -> Option<String> {
    match field {
        TraceField::Span(key) | TraceField::Unscoped(key)
            if key == "status_code" || key == "status" =>
        {
            span_status_code_value(span)
        }
        TraceField::Span(key) => span_or_session_attr(span, key),
        TraceField::Unscoped(key) => span_or_session_attr(span, key)
            .or_else(|| {
                span.resource_attributes
                    .iter()
                    .find(|attribute| attribute.key == *key)
                    .map(|attribute| attribute.value.clone())
            })
            .or_else(|| {
                (key == "service.name")
                    .then(|| span.service_name.clone())
                    .flatten()
            }),
        TraceField::Resource(key) => span
            .resource_attributes
            .iter()
            .find(|attribute| attribute.key == *key)
            .map(|attribute| attribute.value.clone())
            .or_else(|| {
                (key == "service.name")
                    .then(|| span.service_name.clone())
                    .flatten()
            }),
        TraceField::Instrumentation(key) => {
            instrumentation_scope_value(span.instrumentation_scope.as_ref(), key)
        }
        TraceField::Intrinsic(key) => match key.as_str() {
            "name" | "span:name" | "span.name" => Some(span.name.clone()),
            "kind" | "span:kind" | "span.kind" => span.kind.clone(),
            "status" | "span:status" | "span.status" => {
                span.status_code.clone().filter(|s| !s.is_empty())
            }
            "status_code" | "span:status_code" | "span.status_code" => span_status_code_value(span),
            "statusMessage" | "span:statusMessage" | "span.statusMessage" => {
                span.status_message.clone()
            }
            "duration" | "span:duration" | "span.duration" | "traceDuration" | "trace:duration"
            | "trace.duration" => Some(
                span.end_time_unix_nano
                    .unwrap_or(span.start_time_unix_nano)
                    .saturating_sub(span.start_time_unix_nano)
                    .to_string(),
            ),
            _ => None,
        },
    }
}

fn span_status_code_value(span: &TraceSpan) -> Option<String> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key == "status_code" || attribute.key == "status")
        .map(|attribute| attribute.value.clone())
        .or_else(|| span.status_code.clone())
}

fn instrumentation_scope_value(scope: Option<&Value>, key: &str) -> Option<String> {
    let scope = scope?;
    match key {
        "name" | "version" => scope
            .get(key)
            .and_then(|value| value.as_str())
            .map(str::to_string),
        _ => None,
    }
}

fn compare_value(field: &TraceField, actual: &str, expected: &str) -> Option<std::cmp::Ordering> {
    if is_numeric_field(field) {
        let actual = if is_persisted_status_code_field(field) {
            persisted_status_code_numeric_value(actual)?.to_string()
        } else {
            actual.to_string()
        };
        return actual
            .parse::<i64>()
            .ok()?
            .partial_cmp(&expected.parse::<i64>().ok()?);
    }
    if crate::compat::tempo::traceql::is_duration_field(field) {
        return actual
            .parse::<i64>()
            .ok()?
            .partial_cmp(&crate::compat::tempo::traceql::parse_duration_ns(expected)?);
    }
    Some(actual.cmp(expected))
}

fn is_persisted_status_code_field(field: &TraceField) -> bool {
    crate::compat::tempo::traceql::is_status_field(field)
}

fn parse_timestamp_value(value: &Value, _fallback: i64) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(value) => chrono::DateTime::parse_from_rfc3339(value)
            .ok()
            .and_then(|timestamp| timestamp.timestamp_nanos_opt())
            .or_else(|| {
                ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f"]
                    .into_iter()
                    .find_map(|format| {
                        chrono::NaiveDateTime::parse_from_str(value, format)
                            .ok()
                            .and_then(|timestamp| timestamp.and_utc().timestamp_nanos_opt())
                    })
            })
            .or_else(|| value.parse::<i64>().ok()),
        _ => None,
    }
}

fn parse_links(value: &Value, row_index: usize) -> Result<Vec<Value>, CompatError> {
    let value = strict_json_value(value, row_index, "links")?;
    // MAP(VARCHAR,VARCHAR) storage: empty maps CAST to `{}`; JSON arrays that
    // cannot flatten are stored under `_raw` by `reserved_json_to_string_map`.
    let value = unwrap_map_encoded_json_array(value);
    match value {
        Value::Array(values) if values.iter().all(Value::is_object) => Ok(values),
        Value::Object(map) if map.is_empty() => Ok(Vec::new()),
        _ => Err(malformed_row(row_index, "links")),
    }
}

/// Recover a JSON array from MAP-bag encoding (`{}` or `{ "_raw": "<json>" }`).
fn unwrap_map_encoded_json_array(value: Value) -> Value {
    let Value::Object(mut map) = value else {
        return value;
    };
    if map.is_empty() {
        return Value::Array(Vec::new());
    }
    if map.len() == 1 {
        if let Some(raw) = map.remove("_raw") {
            let parsed = crate::storage::schema::parse_projected_json_value(raw);
            return match parsed {
                Value::Array(_) => parsed,
                Value::String(text) => serde_json::from_str(&text).unwrap_or(Value::String(text)),
                other => other,
            };
        }
    }
    Value::Object(map)
}

fn cell_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        other => Some(other.to_string()),
    }
}

fn cell_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(value) => value.parse::<i64>().ok(),
        _ => None,
    }
}

fn deadline() -> CompatError {
    CompatError::new(CompatErrorCode::LimitExceeded, "query deadline exceeded")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::errors::CompatErrorCode;

    #[test]
    fn selector_matches_span_and_resource_intrinsics() {
        let span = TraceSpan {
            trace_id: "t".into(),
            span_id: "s".into(),
            parent_span_id: None,
            name: "GET /users".into(),
            kind: Some("SPAN_KIND_SERVER".into()),
            start_time_unix_nano: 10,
            end_time_unix_nano: Some(20),
            attributes: vec![TraceAttribute {
                key: "http.method".into(),
                value: "GET".into(),
            }],
            status_code: Some("STATUS_CODE_OK".into()),
            status_message: None,
            events: Vec::new(),
            service_name: Some("api".into()),
            resource_attributes: vec![TraceAttribute {
                key: "deployment.environment".into(),
                value: "prod".into(),
            }],
            instrumentation_scope: Some(serde_json::json!({"name": "otel-rust"})),
            links: vec![serde_json::json!({"traceId": "linked", "spanId": "span"})],
        };
        let selector = crate::compat::tempo::traceql::parse_traceql(
            r#"{ resource.service.name = "api" && span.http.method =~ "G.*" }"#,
        )
        .unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(&selector, &span));

        let selector = crate::compat::tempo::traceql::parse_traceql(
            r#"{ instrumentation.name = "otel-rust" }"#,
        )
        .unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(&selector, &span));

        let mut numeric_span = span.clone();
        numeric_span.status_code = Some("500".into());
        let selector =
            crate::compat::tempo::traceql::parse_traceql(r#"{ status_code >= 500 }"#).unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(
            &selector,
            &numeric_span
        ));
    }

    #[test]
    fn numeric_status_code_intrinsic_aliases_match() {
        let span = TraceSpan {
            trace_id: "t".into(),
            span_id: "s".into(),
            parent_span_id: None,
            name: "root".into(),
            kind: None,
            start_time_unix_nano: 1,
            end_time_unix_nano: Some(2),
            attributes: Vec::new(),
            status_code: Some("500".into()),
            status_message: None,
            events: Vec::new(),
            service_name: None,
            resource_attributes: Vec::new(),
            instrumentation_scope: None,
            links: Vec::new(),
        };

        for query in [
            r#"{ status_code >= 500 }"#,
            r#"{ span:status_code >= 500 }"#,
            r#"{ span.status_code >= 500 }"#,
        ] {
            let selector = crate::compat::tempo::traceql::parse_traceql(query).unwrap();
            assert!(
                DuckLakeTraceBackend::matches_selector(&selector, &span),
                "query should match: {query}"
            );
        }
    }

    #[test]
    fn status_and_duration_filters_match_in_memory() {
        let error_span = TraceSpan {
            trace_id: "t1".into(),
            span_id: "s1".into(),
            parent_span_id: None,
            name: "checkout".into(),
            kind: Some("SPAN_KIND_SERVER".into()),
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: Some(2_500_000_000), // 1.5s duration
            attributes: Vec::new(),
            status_code: Some("STATUS_CODE_ERROR".into()),
            status_message: Some("timeout".into()),
            events: Vec::new(),
            service_name: Some("api".into()),
            resource_attributes: Vec::new(),
            instrumentation_scope: None,
            links: Vec::new(),
        };

        let ok_span = TraceSpan {
            trace_id: "t2".into(),
            span_id: "s2".into(),
            parent_span_id: None,
            name: "query".into(),
            kind: Some("SPAN_KIND_CLIENT".into()),
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: Some(1_100_000_000), // 100ms duration
            attributes: Vec::new(),
            status_code: Some("STATUS_CODE_OK".into()),
            status_message: None,
            events: Vec::new(),
            service_name: Some("api".into()),
            resource_attributes: Vec::new(),
            instrumentation_scope: None,
            links: Vec::new(),
        };

        // Status filters
        for query in [
            r#"{ status = error }"#,
            r#"{ status = "error" }"#,
            r#"{ status = "ERROR" }"#,
            r#"{ status != ok }"#,
            r#"{ span:status = error }"#,
            r#"{ span.status = error }"#,
            r#"{ status_code = 2 }"#,
            r#"{ span.status_code >= 2 }"#,
        ] {
            let selector = crate::compat::tempo::traceql::parse_traceql(query).unwrap();
            assert!(
                DuckLakeTraceBackend::matches_selector(&selector, &error_span),
                "error span should match: {query}"
            );
            assert!(
                !DuckLakeTraceBackend::matches_selector(&selector, &ok_span),
                "ok span should NOT match error query: {query}"
            );
        }

        // OK status filters
        for query in [
            r#"{ status = ok }"#,
            r#"{ status = "ok" }"#,
            r#"{ status = "OK" }"#,
            r#"{ status != error }"#,
            r#"{ span.status = ok }"#,
            r#"{ status_code = 1 }"#,
        ] {
            let selector = crate::compat::tempo::traceql::parse_traceql(query).unwrap();
            assert!(
                DuckLakeTraceBackend::matches_selector(&selector, &ok_span),
                "ok span should match: {query}"
            );
            assert!(
                !DuckLakeTraceBackend::matches_selector(&selector, &error_span),
                "error span should NOT match ok query: {query}"
            );
        }

        // Duration filters
        let slow_sel =
            crate::compat::tempo::traceql::parse_traceql(r#"{ duration >= 1.5s }"#).unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(
            &slow_sel,
            &error_span
        ));
        assert!(!DuckLakeTraceBackend::matches_selector(&slow_sel, &ok_span));

        let fast_sel =
            crate::compat::tempo::traceql::parse_traceql(r#"{ duration < 500ms }"#).unwrap();
        assert!(!DuckLakeTraceBackend::matches_selector(
            &fast_sel,
            &error_span
        ));
        assert!(DuckLakeTraceBackend::matches_selector(&fast_sel, &ok_span));
    }

    #[test]
    fn numeric_status_code_intrinsics_share_span_alias_field_value() {
        let span = TraceSpan {
            trace_id: "t".into(),
            span_id: "s".into(),
            parent_span_id: None,
            name: "root".into(),
            kind: None,
            start_time_unix_nano: 1,
            end_time_unix_nano: Some(2),
            attributes: vec![TraceAttribute {
                key: "status_code".into(),
                value: "500".into(),
            }],
            status_code: Some("STATUS_CODE_OK".into()),
            status_message: None,
            events: Vec::new(),
            service_name: None,
            resource_attributes: Vec::new(),
            instrumentation_scope: None,
            links: Vec::new(),
        };

        for query in [
            r#"{ status_code >= 500 }"#,
            r#"{ span:status_code >= 500 }"#,
            r#"{ span.status_code >= 500 }"#,
        ] {
            let selector = crate::compat::tempo::traceql::parse_traceql(query).unwrap();
            assert!(
                DuckLakeTraceBackend::matches_selector(&selector, &span),
                "query should use the same post-filter field value: {query}"
            );
        }
    }

    #[test]
    fn pure_status_intrinsic_ignores_custom_status_attribute_while_scoped_aliases_read_attribute() {
        let span = TraceSpan {
            trace_id: "t".into(),
            span_id: "s".into(),
            parent_span_id: None,
            name: "root".into(),
            kind: None,
            start_time_unix_nano: 1,
            end_time_unix_nano: Some(2),
            attributes: vec![TraceAttribute {
                key: "status".into(),
                value: "error".into(),
            }],
            status_code: Some("STATUS_CODE_OK".into()),
            status_message: None,
            events: Vec::new(),
            service_name: None,
            resource_attributes: Vec::new(),
            instrumentation_scope: None,
            links: Vec::new(),
        };

        for intrinsic_ok in [r#"{ status = ok }"#, r#"{ span:status = ok }"#] {
            let selector = crate::compat::tempo::traceql::parse_traceql(intrinsic_ok).unwrap();
            assert!(
                DuckLakeTraceBackend::matches_selector(&selector, &span),
                "OTLP status intrinsic must read scalar status_code: {intrinsic_ok}"
            );
        }
        for intrinsic_err in [r#"{ status = error }"#, r#"{ span:status = error }"#] {
            let selector = crate::compat::tempo::traceql::parse_traceql(intrinsic_err).unwrap();
            assert!(
                !DuckLakeTraceBackend::matches_selector(&selector, &span),
                "OTLP status intrinsic must not be shadowed by custom attributes['status']: {intrinsic_err}"
            );
        }
        for scoped_err in [r#"{ span.status = error }"#, r#"{ .status = error }"#] {
            let selector = crate::compat::tempo::traceql::parse_traceql(scoped_err).unwrap();
            assert!(
                DuckLakeTraceBackend::matches_selector(&selector, &span),
                "scoped .status / span.status should match custom attribute: {scoped_err}"
            );
        }
    }

    #[test]
    fn persisted_otlp_error_status_is_numeric_for_span_status_code() {
        let span = TraceSpan {
            trace_id: "t".into(),
            span_id: "s".into(),
            parent_span_id: None,
            name: "root".into(),
            kind: None,
            start_time_unix_nano: 1,
            end_time_unix_nano: Some(2),
            attributes: Vec::new(),
            status_code: Some("ERROR".into()),
            status_message: None,
            events: Vec::new(),
            service_name: None,
            resource_attributes: Vec::new(),
            instrumentation_scope: None,
            links: Vec::new(),
        };

        let numeric =
            crate::compat::tempo::traceql::parse_traceql(r#"{ span.status_code >= 2 }"#).unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(&numeric, &span));

        let exact_status =
            crate::compat::tempo::traceql::parse_traceql(r#"{ status = "ERROR" }"#).unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(&exact_status, &span));
    }

    #[test]
    fn resource_predicates_use_all_resource_attributes() {
        let span = TraceSpan {
            trace_id: "t".into(),
            span_id: "s".into(),
            parent_span_id: None,
            name: "root".into(),
            kind: None,
            start_time_unix_nano: 1,
            end_time_unix_nano: Some(2),
            attributes: Vec::new(),
            status_code: None,
            status_message: None,
            events: Vec::new(),
            service_name: Some("api".into()),
            resource_attributes: vec![TraceAttribute {
                key: "deployment.environment".into(),
                value: "prod".into(),
            }],
            instrumentation_scope: None,
            links: Vec::new(),
        };
        let selector = crate::compat::tempo::traceql::parse_traceql(
            r#"{ resource.deployment.environment = "prod" }"#,
        )
        .unwrap();
        assert!(DuckLakeTraceBackend::matches_selector(&selector, &span));
    }

    #[test]
    fn parses_duckdb_json_timestamp_without_timezone() {
        assert_eq!(
            parse_timestamp_value(&Value::String("2023-11-14 22:13:21.123456789".into()), 0,),
            Some(1_700_000_001_123_456_789)
        );
    }

    #[test]
    fn malformed_required_trace_rows_return_an_explicit_error() {
        let result = QueryResult {
            columns: vec![
                "trace_id".into(),
                "span_id".into(),
                "parent_span_id".into(),
                "message_type".into(),
                "span_kind".into(),
                "app_id".into(),
                "start_time_unix_nano".into(),
                "end_time_unix_nano".into(),
                "attributes".into(),
                "resource_attributes".into(),
                "instrumentation_scope".into(),
                "links".into(),
                "status_code".into(),
                "status_message".into(),
                "events".into(),
            ],
            rows: vec![vec![
                Value::String("trace".into()),
                Value::String("span".into()),
                Value::Null,
                Value::String("root".into()),
                Value::Null,
                Value::String("api".into()),
                Value::String("not-a-timestamp".into()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ]],
            row_count: 1,
        };

        let err = match parse_rows(&result) {
            Ok(_) => panic!("malformed rows must not be dropped"),
            Err(err) => err,
        };
        assert_eq!(err.code, CompatErrorCode::BadRequest);
        assert!(err.message.contains("start_time_unix_nano"));
    }

    #[tokio::test]
    async fn empty_query_result_is_valid_empty_trace_data_for_encoders() {
        let result = QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: 0,
        };

        let spans = parse_rows(&result).expect("empty result");
        assert!(spans.is_empty());

        let data = TraceData { spans };
        assert_eq!(
            crate::compat::tempo::encode::trace_v1_response(&data, 1024)
                .expect("v1 response")
                .status(),
            axum::http::StatusCode::OK
        );
        assert_eq!(
            crate::compat::tempo::encode::trace_v2_response(&data, 1024)
                .expect("v2 response")
                .status(),
            axum::http::StatusCode::OK
        );
    }

    #[test]
    fn older_trace_schema_without_optional_columns_uses_defaults() {
        let result = QueryResult {
            columns: vec![
                "trace_id".into(),
                "span_id".into(),
                "message_type".into(),
                "app_id".into(),
                "start_time_unix_nano".into(),
            ],
            rows: vec![vec![
                Value::String("trace".into()),
                Value::String("span".into()),
                Value::String("root".into()),
                Value::String("api".into()),
                Value::Number(10.into()),
            ]],
            row_count: 1,
        };

        let span = parse_rows(&result)
            .expect("legacy schema")
            .pop()
            .expect("legacy span");
        assert_eq!(span.end_time_unix_nano, None);
        assert!(span.attributes.is_empty());
        assert!(span.resource_attributes.is_empty());
        assert_eq!(span.instrumentation_scope, None);
        assert!(span.links.is_empty());
        assert!(span.events.is_empty());
        assert_eq!(span.service_name.as_deref(), Some("api"));
    }

    fn valid_trace_result() -> QueryResult {
        QueryResult {
            columns: vec![
                "trace_id".into(),
                "span_id".into(),
                "parent_span_id".into(),
                "message_type".into(),
                "span_kind".into(),
                "app_id".into(),
                "start_time_unix_nano".into(),
                "end_time_unix_nano".into(),
                "attributes".into(),
                "resource_attributes".into(),
                "instrumentation_scope".into(),
                "links".into(),
                "status_code".into(),
                "status_message".into(),
                "events".into(),
            ],
            rows: vec![vec![
                Value::String("trace".into()),
                Value::String("span".into()),
                Value::Null,
                Value::String("root".into()),
                Value::Null,
                Value::String("api".into()),
                Value::Number(10.into()),
                Value::Number(20.into()),
                Value::String(r#"{"http.status_code":"2"}"#.into()),
                Value::String(r#"{"service.name":"api"}"#.into()),
                Value::String(r#"{"name":"scope"}"#.into()),
                Value::String("[]".into()),
                Value::String("OK".into()),
                Value::Null,
                Value::String(r#"[{"name":"event","timestamp":20,"attributes":{}}]"#.into()),
            ]],
            row_count: 1,
        }
    }

    #[test]
    fn null_and_empty_optional_trace_fields_are_absent() {
        let optional_fields = [
            ("end_time_unix_nano", "end_timestamp"),
            ("attributes", "attributes"),
            ("resource_attributes", "resource_attributes"),
            ("instrumentation_scope", "instrumentation_scope"),
            ("links", "links"),
            ("events", "events"),
        ];

        for (column_name, field) in optional_fields {
            for value in [
                Value::Null,
                Value::String(String::new()),
                Value::String("null".into()),
            ] {
                let mut result = valid_trace_result();
                let column = result
                    .columns
                    .iter()
                    .position(|column| column == column_name)
                    .expect("test field column");
                result.rows[0][column] = value;

                let span = parse_rows(&result)
                    .unwrap_or_else(|err| panic!("{field} should be absent: {err:?}"))
                    .pop()
                    .expect("test span");
                match field {
                    "end_timestamp" => assert_eq!(span.end_time_unix_nano, None),
                    "attributes" => assert!(span.attributes.is_empty()),
                    "resource_attributes" => assert!(span.resource_attributes.is_empty()),
                    "instrumentation_scope" => assert_eq!(span.instrumentation_scope, None),
                    "links" => assert!(span.links.is_empty()),
                    "events" => assert!(span.events.is_empty()),
                    _ => unreachable!("test field must be handled"),
                }
            }
        }
    }

    #[test]
    fn map_encoded_empty_and_raw_links_round_trip() {
        let mut empty = valid_trace_result();
        let links_col = empty
            .columns
            .iter()
            .position(|c| c == "links")
            .expect("links");
        empty.rows[0][links_col] = Value::String("{}".into());
        let span = parse_rows(&empty)
            .expect("empty MAP links")
            .pop()
            .expect("span");
        assert!(span.links.is_empty());

        let mut raw = valid_trace_result();
        let payload = r#"[{"traceId":"aa","spanId":"bb"}]"#;
        raw.rows[0][links_col] = serde_json::json!({ "_raw": payload });
        let span = parse_rows(&raw)
            .expect("_raw MAP links")
            .pop()
            .expect("span");
        assert_eq!(span.links.len(), 1);
        assert_eq!(span.links[0]["traceId"], "aa");
    }

    #[test]
    fn malformed_optional_trace_fields_return_bad_request() {
        let malformed = [
            (
                "end_time_unix_nano",
                "end_timestamp",
                Value::String("not-a-timestamp".into()),
            ),
            ("attributes", "attributes", Value::String("{".into())),
            (
                "resource_attributes",
                "resource_attributes",
                Value::String("[]".into()),
            ),
            (
                "instrumentation_scope",
                "instrumentation_scope",
                Value::String("not-json".into()),
            ),
            // Empty MAP `{}` is a valid no-links encoding for attribute maps.
            ("links", "links", Value::String("[1]".into())),
            ("events", "events", Value::String("{}".into())),
        ];

        for (column_name, field, value) in malformed {
            let mut result = valid_trace_result();
            let column = result
                .columns
                .iter()
                .position(|column| column == column_name)
                .expect("test field column");
            result.rows[0][column] = value;

            let err = parse_rows(&result).expect_err(field);
            assert_eq!(err.code, CompatErrorCode::BadRequest, "{field}: {err:?}");
            assert!(err.message.contains(field), "{field}: {err:?}");
        }
    }

    #[test]
    fn tag_discovery_scan_sql_uses_default_lookback_window() {
        use crate::sql::assert_sql_has_otlp_time_predicates;
        let request = TraceSearchRequest {
            tags: BTreeMap::new(),
            selector: None,
            min_duration_ns: None,
            max_duration_ns: None,
            start_ns: None,
            end_ns: None,
            limit: 5,
        };
        let sql = trace_scan_sql(request.scan_params(), None).expect("default lookback");
        assert_sql_has_otlp_time_predicates(sql.as_str());
    }

    #[tokio::test]
    async fn try_read_tag_index_returns_none_when_absent_or_unreachable() {
        assert!(try_read_tag_index(None, TraceLookupBounds::default(), 100)
            .await
            .is_none());

        let mut pg = tokio_postgres::Config::new();
        pg.host("127.0.0.1");
        pg.port(1);
        pg.dbname("x");
        pg.user("x");
        let mgr = deadpool_postgres::Manager::from_config(
            pg,
            tokio_postgres::NoTls,
            deadpool_postgres::ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            },
        );
        let pool = deadpool_postgres::Pool::builder(mgr)
            .max_size(1)
            .build()
            .unwrap();
        let reader = crate::tag_index::TagIndexReader::new(pool, "s", "ws");
        assert!(
            try_read_tag_index(Some(&reader), TraceLookupBounds::default(), 100)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    #[ignore = "requires ducklake-postgres; make test-lease-pg / make test-e2e"]
    async fn postgres_try_read_tag_index_returns_none_when_empty_and_some_when_populated() {
        let schema = "thelake_tag_idx_backend_test";
        let mut pg = tokio_postgres::Config::new();
        pg.host("localhost");
        pg.port(5432);
        pg.dbname("ducklake");
        pg.user("ducklake");
        pg.password("ducklake");
        let mgr = deadpool_postgres::Manager::from_config(
            pg,
            tokio_postgres::NoTls,
            deadpool_postgres::ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            },
        );
        let pool = deadpool_postgres::Pool::builder(mgr)
            .max_size(2)
            .build()
            .unwrap();
        let client = pool.get().await.expect("ducklake-postgres required");
        crate::tag_index::ensure_tag_index_table(&client, schema)
            .await
            .unwrap();
        client
            .execute(&format!("TRUNCATE \"{schema}\".tag_index"), &[])
            .await
            .unwrap();

        let reader = crate::tag_index::TagIndexReader::new(pool.clone(), schema, "ws-backend");
        assert!(
            try_read_tag_index(Some(&reader), TraceLookupBounds::default(), 100)
                .await
                .is_none()
        );

        let writer = crate::tag_index::TagIndexWriter::new(pool, schema, "ws-backend");
        writer
            .upsert_entries(&[crate::tag_index::TagEntry {
                tag: "http.method".into(),
                last_seen_ns: 42,
                source_mask: crate::tag_index::TAG_SOURCE_SPAN_OR_RESOURCE,
            }])
            .await
            .unwrap();
        assert_eq!(
            try_read_tag_index(Some(&reader), TraceLookupBounds::default(), 100).await,
            Some(vec!["http.method".to_string()])
        );
        assert!(
            !should_scan_event_link_tags(Some(&reader), "http.method").await,
            "span-only tag must skip event/link DuckLake scan"
        );
        assert!(
            should_scan_event_link_tags(Some(&reader), "unindexed.tag").await,
            "unindexed tag must fall back to event/link scan"
        );
        writer
            .upsert_entries(&[crate::tag_index::TagEntry {
                tag: "http.method".into(),
                last_seen_ns: 43,
                source_mask: crate::tag_index::TAG_SOURCE_EVENT_OR_LINK,
            }])
            .await
            .unwrap();
        assert!(
            should_scan_event_link_tags(Some(&reader), "http.method").await,
            "tag with event/link bit must scan event/link tags"
        );
    }

    #[tokio::test]
    async fn should_scan_event_link_tags_defaults_to_true_when_absent_or_unreachable() {
        assert!(should_scan_event_link_tags(None, "http.method").await);
        let mut pg = tokio_postgres::Config::new();
        pg.host("127.0.0.1");
        pg.port(1);
        pg.dbname("x");
        pg.user("x");
        let mgr = deadpool_postgres::Manager::from_config(
            pg,
            tokio_postgres::NoTls,
            deadpool_postgres::ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            },
        );
        let pool = deadpool_postgres::Pool::builder(mgr)
            .max_size(1)
            .build()
            .unwrap();
        let reader = crate::tag_index::TagIndexReader::new(pool, "s", "ws");
        assert!(should_scan_event_link_tags(Some(&reader), "http.method").await);
    }

    #[test]
    fn trailing_tag_index_min_last_seen_ns_handles_live_and_historical_windows() {
        let now_ns = 1_700_000_000_000_000_000i64;
        assert_eq!(
            trailing_tag_index_min_last_seen_ns(TraceLookupBounds::default(), now_ns),
            Some(i64::MIN)
        );
        let start = now_ns - 3_600_000_000_000;
        assert_eq!(
            trailing_tag_index_min_last_seen_ns(
                TraceLookupBounds {
                    start_ns: Some(start),
                    end_ns: Some(now_ns - 30_000_000_000),
                },
                now_ns,
            ),
            Some(start - crate::tag_index::TAG_FLUSH_WINDOW_NS)
        );
        assert_eq!(
            trailing_tag_index_min_last_seen_ns(
                TraceLookupBounds {
                    start_ns: Some(start),
                    end_ns: None,
                },
                now_ns,
            ),
            Some(start - crate::tag_index::TAG_FLUSH_WINDOW_NS)
        );
        assert_eq!(
            trailing_tag_index_min_last_seen_ns(
                TraceLookupBounds {
                    start_ns: Some(start),
                    end_ns: Some(now_ns - 300_000_000_000),
                },
                now_ns,
            ),
            None
        );
        assert_eq!(
            trailing_tag_index_min_last_seen_ns(
                TraceLookupBounds {
                    start_ns: None,
                    end_ns: Some(now_ns - 300_000_000_000),
                },
                now_ns,
            ),
            None
        );
    }
}
