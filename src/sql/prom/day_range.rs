//! Calendar-day window for Prom postings resolve (cache keys + timestamp SQL bounds).

use chrono::{DateTime, NaiveDate, TimeZone, Utc};

/// Grafana label discovery often omits start/end. Always emit a finite window so
/// D12 never sees an unbound `metric_postings` scan (one-clock law).
/// Ten years matches Loki discovery lookback so fixed fixture timestamps (2023…)
/// still resolve under CI "now".
const PROM_DISCOVERY_DEFAULT_LOOKBACK_MS: i64 = 10 * 365 * 86_400_000;

/// Calendar-day window derived from a Prom ms range (for day-scoped cache keys).
///
/// SQL emit uses **timestamp** bounds covering the inclusive days — never a DATE column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostingsDayRange {
    pub start: Option<NaiveDate>,
    pub end: Option<NaiveDate>,
}

impl PostingsDayRange {
    pub fn from_ms(start_ms: Option<i64>, end_ms: Option<i64>) -> Self {
        match (start_ms.and_then(ms_to_utc), end_ms.and_then(ms_to_utc)) {
            (None, None) => {
                let end = Utc::now();
                let start =
                    end - chrono::Duration::milliseconds(PROM_DISCOVERY_DEFAULT_LOOKBACK_MS);
                Self {
                    start: Some(start.date_naive()),
                    end: Some(end.date_naive()),
                }
            }
            (Some(s), Some(e)) => {
                let mut start = s.date_naive();
                let mut end = e.date_naive();
                if start > end {
                    std::mem::swap(&mut start, &mut end);
                }
                Self {
                    start: Some(start),
                    end: Some(end),
                }
            }
            (Some(s), None) => {
                // A one-sided Prom range still needs a finite resolve window.
                // Using only the endpoint day loses persistent series whose
                // latest observation is earlier than that day.
                let end = s + chrono::Duration::milliseconds(PROM_DISCOVERY_DEFAULT_LOOKBACK_MS);
                Self {
                    start: Some(s.date_naive()),
                    end: Some(end.date_naive()),
                }
            }
            (None, Some(e)) => {
                let start = e - chrono::Duration::milliseconds(PROM_DISCOVERY_DEFAULT_LOOKBACK_MS);
                Self {
                    start: Some(start.date_naive()),
                    end: Some(e.date_naive()),
                }
            }
        }
    }

    /// SQL fragment for WHERE as `timestamp` lower/upper covering the inclusive days.
    pub fn sql_predicate(&self, column_prefix: &str) -> String {
        match (self.start, self.end) {
            (Some(start), Some(end)) => {
                let from = start.and_hms_opt(0, 0, 0).expect("midnight").and_utc();
                let to = (end + chrono::Duration::days(1))
                    .and_hms_opt(0, 0, 0)
                    .expect("next midnight")
                    .and_utc()
                    - chrono::Duration::nanoseconds(1);
                let col = if column_prefix.is_empty() {
                    "timestamp".to_string()
                } else {
                    format!("{column_prefix}timestamp")
                };
                format!(
                    "{col} >= TIMESTAMPTZ '{}' AND {col} <= TIMESTAMPTZ '{}'",
                    from.format("%Y-%m-%d %H:%M:%S%.6f+00"),
                    to.format("%Y-%m-%d %H:%M:%S%.6f+00"),
                )
            }
            _ => String::new(),
        }
    }

    /// Inclusive calendar days covered by this range, or `None` when unbounded.
    ///
    /// Day-scoped posting cache keys require an explicit date; unbounded resolve
    /// falls back to a single DuckDB INTERSECT (no cache).
    pub fn inclusive_days(&self) -> Option<Vec<NaiveDate>> {
        match (self.start, self.end) {
            (Some(start), Some(end)) => {
                let mut out = Vec::new();
                let mut d = start;
                while d <= end {
                    out.push(d);
                    d = d.succ_opt()?;
                }
                Some(out)
            }
            _ => None,
        }
    }
}

pub(crate) fn ms_to_utc(ms: i64) -> Option<DateTime<Utc>> {
    let secs = ms.div_euclid(1000);
    let nsecs = (ms.rem_euclid(1000) * 1_000_000) as u32;
    Utc.timestamp_opt(secs, nsecs).single()
}

/// Timestamptz literal for zone-map-friendly predicates (§9.1 step 8).
pub fn timestamptz_literal_ms(ms: i64) -> String {
    let dt = ms_to_utc(ms).unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().unwrap());
    format!("TIMESTAMPTZ '{}'", dt.format("%Y-%m-%d %H:%M:%S%.3f+00"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_predicate_emits_timestamp_bounds_only() {
        let days = PostingsDayRange {
            start: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2026, 8, 15).unwrap()),
        };
        let pred = days.sql_predicate("p.");
        assert!(pred.contains("p.timestamp >="), "{pred}");
        assert!(pred.contains("TIMESTAMPTZ "), "{pred}");
        assert!(!pred.contains("record_date"), "{pred}");
        assert!(!pred.contains("AS DATE"), "{pred}");
    }

    #[test]
    fn inclusive_days_walks_closed_range() {
        let days = PostingsDayRange {
            start: Some(NaiveDate::from_ymd_opt(2026, 8, 14).unwrap()),
            end: Some(NaiveDate::from_ymd_opt(2026, 8, 16).unwrap()),
        };
        assert_eq!(days.inclusive_days().unwrap().len(), 3);
    }

    #[test]
    fn from_ms_none_defaults_to_finite_lookback() {
        let days = PostingsDayRange::from_ms(None, None);
        let pred = days.sql_predicate("");
        assert!(pred.contains("timestamp >="), "{pred}");
        assert!(pred.contains("timestamp <="), "{pred}");
        assert!(days.start.is_some() && days.end.is_some());
    }

    #[test]
    fn one_sided_ranges_cover_a_finite_lookback_instead_of_one_day() {
        let endpoint = Utc.with_ymd_and_hms(2026, 9, 20, 12, 0, 0).unwrap();
        let ms = endpoint.timestamp_millis();
        let start_only = PostingsDayRange::from_ms(Some(ms), None);
        let end_only = PostingsDayRange::from_ms(None, Some(ms));
        assert!(start_only.end.unwrap() > endpoint.date_naive());
        assert!(end_only.start.unwrap() < endpoint.date_naive());
        assert!(start_only.inclusive_days().unwrap().len() > 1);
        assert!(end_only.inclusive_days().unwrap().len() > 1);
    }
}
