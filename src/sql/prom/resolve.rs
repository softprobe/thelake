//! Postings resolve + discovery + single-day posting list SQL.

use chrono::NaiveDate;

use crate::sql::literal::sql_string_literal;
use crate::sql::prom::day_range::PostingsDayRange;
use crate::sql::schema::{qualified_table_name, table_spec};

fn metric_table(catalog: &str, name: &str) -> String {
    qualified_table_name(catalog, table_spec(name).expect("registered metric table"))
}

/// One equality posting constraint (`label_name` / candidate values).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EqualityPosting {
    pub label_name: String,
    pub values: Vec<String>,
}

fn sql_in_list(values: &[String]) -> String {
    values
        .iter()
        .map(|v| sql_string_literal(v))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn sql_series_id_list(series_ids: &[u64]) -> String {
    if series_ids.is_empty() {
        "NULL".to_string()
    } else {
        series_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// SQL that resolves `series_id`s via postings intersect (AC-Q7 / §9.1 steps 3–4).
///
/// Returns at most `max_series + 1` ids so callers can fail loud without a sample scan.
pub fn resolve_series_ids_sql(
    catalog: &str,
    days: PostingsDayRange,
    equality: &[EqualityPosting],
    max_series: usize,
) -> String {
    let postings = metric_table(catalog, "metric_postings");
    let lim = max_series.saturating_add(1);
    let day_pred = days.sql_predicate("");
    let name_day_pred = days.sql_predicate("p.");
    let name_day_and = if name_day_pred.is_empty() {
        String::new()
    } else {
        format!(" AND {name_day_pred}")
    };
    if equality.is_empty() {
        // No equality → cardinality of all series in the window; fail loud at max_series.
        let where_clause = if day_pred.is_empty() {
            String::new()
        } else {
            format!("WHERE {day_pred}")
        };
        return format!(
            "SELECT DISTINCT series_id \
             FROM {postings} \
             {where_clause} \
             LIMIT {lim}"
        );
    }
    // INTERSECT smallest posting first (__name__ is usually tighter than job/service).
    let mut ordered = equality.to_vec();
    ordered.sort_by(|a, b| {
        let ar = if a.label_name == "__name__" { 0 } else { 1 };
        let br = if b.label_name == "__name__" { 0 } else { 1 };
        ar.cmp(&br).then_with(|| a.label_name.cmp(&b.label_name))
    });
    let parts: Vec<String> = ordered
        .iter()
        .map(|eq| {
            let name = sql_string_literal(&eq.label_name);
            format!(
                "SELECT DISTINCT p.series_id FROM {postings} p \
                 WHERE p.label_name = {name} AND p.label_value IN ({}){name_day_and}",
                sql_in_list(&eq.values)
            )
        })
        .collect();
    if parts.len() == 1 {
        format!("{} LIMIT {lim}", parts[0])
    } else {
        format!("{} LIMIT {lim}", parts.join(" INTERSECT "))
    }
}

/// Discovery SQL for `GET /api/v1/label/__name__/values` (AC-Q6).
///
/// Reads postings (not `GROUP BY metric_samples`). Joins `metric_series` only for
/// classic histogram/summary Prom name expansion.
pub fn discover_name_values_sql(
    catalog: &str,
    days: PostingsDayRange,
    max_series: usize,
) -> String {
    let postings = metric_table(catalog, "metric_postings");
    let series = metric_table(catalog, "metric_series");
    let lim = max_series.saturating_add(1);
    let posting_series_day = crate::sql::same_utc_calendar_day("p.timestamp", "s.timestamp");
    let day_pred = days.sql_predicate("p.");
    let day_and = if day_pred.is_empty() {
        String::new()
    } else {
        format!(" AND {day_pred}")
    };
    format!(
        "SELECT p.label_value, any_value(s.metric_type) AS metric_type \
         FROM {postings} p \
         JOIN {series} s \
           ON p.series_id = s.series_id AND {posting_series_day} \
         WHERE p.label_name = '__name__'{day_and} \
         GROUP BY p.label_value \
         ORDER BY p.label_value \
         LIMIT {lim}"
    )
}

/// SQL for one day-scoped equality posting list (cache fill path).
pub fn single_posting_sql(
    catalog: &str,
    day: NaiveDate,
    label_name: &str,
    label_value: &str,
) -> String {
    let postings = metric_table(catalog, "metric_postings");
    let day_pred = PostingsDayRange {
        start: Some(day),
        end: Some(day),
    }
    .sql_predicate("");
    format!(
        "SELECT DISTINCT series_id FROM {postings} \
         WHERE label_name = {} AND label_value = {} AND {day_pred} \
         ORDER BY series_id",
        sql_string_literal(label_name),
        sql_string_literal(label_value),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discover_sql_uses_postings_not_samples() {
        let days = PostingsDayRange {
            start: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
        };
        let sql = discover_name_values_sql("softprobe", days, 10_000);
        assert!(sql.contains("metric_postings"), "{sql}");
        assert!(sql.contains("label_name = '__name__'"), "{sql}");
        assert!(!sql.contains("metric_samples"), "{sql}");
        assert!(sql.contains("p.timestamp >="), "{sql}");
    }

    #[test]
    fn single_posting_uses_timestamp_day_bounds() {
        let sql = single_posting_sql(
            "softprobe",
            NaiveDate::from_ymd_opt(2026, 8, 15).unwrap(),
            "__name__",
            "layout_wide",
        );
        assert!(sql.contains("metric_postings"), "{sql}");
        assert!(sql.contains("timestamp >="), "{sql}");
        assert!(!sql.contains("CAST(timestamp AS DATE)"), "{sql}");
        assert!(!sql.contains("record_date"), "{sql}");
    }

    #[test]
    fn resolve_intersect_orders_name_first() {
        let days = PostingsDayRange {
            start: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
        };
        let eq = [
            EqualityPosting {
                label_name: "instance".into(),
                values: vec!["i-1".into()],
            },
            EqualityPosting {
                label_name: "__name__".into(),
                values: vec!["layout_wide".into()],
            },
        ];
        let sql = resolve_series_ids_sql("softprobe", days, &eq, 10_000);
        let name_pos = sql.find("label_name = '__name__'").expect("name");
        let inst_pos = sql.find("label_name = 'instance'").expect("instance");
        assert!(
            name_pos < inst_pos,
            "expected __name__ before instance: {sql}"
        );
        assert!(sql.contains("INTERSECT"), "{sql}");
    }
}
