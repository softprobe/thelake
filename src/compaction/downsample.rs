//! Downsample ladder SQL (one clock: `timestamp` only).
//!
//! SQL recipes live in [`crate::sql::compaction`].

pub use crate::sql::compaction::{
    count_sql, downsample_1h_from_5m_for_day_sql, downsample_1h_from_5m_pending_days_sql,
    downsample_1h_from_5m_sql, downsample_1h_from_raw_for_day_sql,
    downsample_1h_from_raw_pending_days_sql, downsample_1h_from_raw_sql, downsample_5m_for_day_sql,
    downsample_5m_pending_days_sql, downsample_5m_sql, hist_downsample_1h_from_5m_for_day_sql,
    hist_downsample_1h_from_5m_pending_days_sql, hist_downsample_1h_from_5m_sql,
    hist_downsample_1h_from_raw_for_day_sql, hist_downsample_1h_from_raw_pending_days_sql,
    hist_downsample_1h_from_raw_sql, hist_downsample_5m_for_day_sql,
    hist_downsample_5m_pending_days_sql, hist_downsample_5m_sql, DOWNSAMPLE_1H_LAG,
    DOWNSAMPLE_5M_LAG, HIST_DOWNSAMPLE_MAX_DAYS_PER_PASS, METRICS_LADDER_MAX_DAYS_PER_PASS,
};
