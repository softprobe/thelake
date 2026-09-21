//! Prometheus / postings SQL recipes.

mod day_range;
mod meta;
mod resolve;
mod samples;

pub use day_range::{timestamptz_literal_ms, PostingsDayRange};
pub use meta::{
    active_telemetry_promotions_sql, metrics_metadata_scan_sql, series_meta_sql,
    variant_identity_keys_sql, SeriesMetaDayScope, METRICS_PROBE_SQL,
};
pub use resolve::{
    discover_name_values_sql, resolve_series_ids_sql, single_posting_sql, EqualityPosting,
};
pub use samples::{
    samples_scan_sql, samples_scan_sql_for_window, samples_time_predicates,
    samples_time_predicates_bounded, samples_time_predicates_for_column,
};
