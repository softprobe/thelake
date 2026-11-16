//! Sample recent DuckLake attribute statistics for promotion recommendations.

use crate::sql::bounds::push_otlp_ns_window_predicates;
use crate::sql::literal::sql_string_literal;
use crate::sql::trusted::{approved_query, TrustedSql, TrustedSqlError};

const PROMOTION_STATS_TEMPLATE: &str = include_str!("promotion_stats.sql");
const SAMPLE_ROW_LIMIT: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum StatsTable {
    Traces,
    Logs,
}

impl StatsTable {
    pub(crate) fn table_name(self) -> &'static str {
        match self {
            Self::Traces => "traces",
            Self::Logs => "logs",
        }
    }

    pub(crate) fn time_column(self) -> &'static str {
        match self {
            Self::Traces => "start_time",
            Self::Logs => "timestamp",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum StatsSource {
    Attribute,
    ResourceAttribute,
}

impl StatsSource {
    fn map_column(self) -> &'static str {
        match self {
            Self::Attribute => "attributes",
            Self::ResourceAttribute => "resource_attributes",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatsCandidateKey {
    pub source: StatsSource,
    pub key: String,
}

pub(crate) fn compile_attribute_stats_query(
    table: StatsTable,
    start_ns: i64,
    end_ns: i64,
    candidates: &[StatsCandidateKey],
) -> Result<TrustedSql, TrustedSqlError> {
    if candidates.is_empty() {
        return Err(TrustedSqlError::Empty);
    }

    let mut time_predicates = Vec::new();
    push_otlp_ns_window_predicates(
        &mut time_predicates,
        start_ns,
        end_ns,
        [table.time_column().to_string()],
    )
    .map_err(|_| TrustedSqlError::Forbidden("invalid time window"))?;
    let where_clause = if time_predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", time_predicates.join(" AND "))
    };

    let mut sample_projections = Vec::with_capacity(candidates.len());
    let mut agg_projections = Vec::with_capacity(1 + candidates.len() * 6);
    agg_projections.push("CAST(COUNT(*) AS BIGINT) AS total_rows".to_string());

    for (idx, candidate) in candidates.iter().enumerate() {
        let map_col = candidate.source.map_column();
        let key_lit = sql_string_literal(&candidate.key);
        let col_alias = format!("c_{idx}");
        sample_projections.push(format!(
            "(TRY_CAST({map_col} AS MAP(VARCHAR, VARCHAR)))[{key_lit}] AS {col_alias}"
        ));
        agg_projections.push(format!(
            "CAST(COUNT({col_alias}) AS BIGINT) AS non_null_{idx}"
        ));
        agg_projections.push(format!(
            "CAST(approx_count_distinct({col_alias}) AS BIGINT) AS distinct_{idx}"
        ));
        agg_projections.push(format!(
            "CAST(COALESCE(AVG(strlen({col_alias})), 0.0) AS DOUBLE) AS avg_bytes_{idx}"
        ));
        agg_projections.push(format!(
            "CAST(COUNT(CASE WHEN {col_alias} IN ('true', 'false') THEN 1 END) AS BIGINT) AS bool_{idx}"
        ));
        agg_projections.push(format!(
            "CAST(COUNT(CASE WHEN TRY_CAST({col_alias} AS BIGINT) IS NOT NULL AND NOT regexp_matches({col_alias}, '^-?0[0-9]+$') THEN 1 END) AS BIGINT) AS int_{idx}"
        ));
        agg_projections.push(format!(
            "CAST(COUNT(CASE WHEN TRY_CAST({col_alias} AS DOUBLE) IS NOT NULL AND isfinite(TRY_CAST({col_alias} AS DOUBLE)) THEN 1 END) AS BIGINT) AS double_{idx}"
        ));
    }

    let sql = PROMOTION_STATS_TEMPLATE
        .replace("{{sample_cols}}", &sample_projections.join(", "))
        .replace("{{table_name}}", table.table_name())
        .replace("{{where_clause}}", &where_clause)
        .replace("{{sample_limit}}", &SAMPLE_ROW_LIMIT.to_string())
        .replace("{{agg_cols}}", &agg_projections.join(", "));
    approved_query(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiles_bounded_attribute_stats_query_with_escaped_keys() {
        let query = compile_attribute_stats_query(
            StatsTable::Traces,
            1_700_000_000_000_000_000,
            1_700_086_400_000_000_000,
            &[
                StatsCandidateKey {
                    source: StatsSource::Attribute,
                    key: "tenant.tier".to_string(),
                },
                StatsCandidateKey {
                    source: StatsSource::ResourceAttribute,
                    key: "o'reilly.env".to_string(),
                },
            ],
        )
        .expect("trusted sql");
        let sql = query.as_str();
        assert!(sql.contains("FROM traces"));
        assert!(sql.contains("WHERE "));
        assert!(sql.contains("start_time"));
        assert!(sql.contains("LIMIT 10000"));
        assert!(
            sql.contains("TRY_CAST(attributes AS MAP(VARCHAR, VARCHAR)))['tenant.tier'] AS c_0")
        );
        assert!(sql.contains(
            "TRY_CAST(resource_attributes AS MAP(VARCHAR, VARCHAR)))['o''reilly.env'] AS c_1"
        ));
        assert!(sql.contains("strlen(c_0)"));
        assert!(sql.contains("approx_count_distinct(c_1)"));
    }
}
