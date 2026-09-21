//! Typed query backends shared by protocol adapters.

pub mod ducklake_logs;
pub mod ducklake_traces;
pub mod label_match;
pub mod logs;
pub mod traces;

pub use ducklake_traces::DuckLakeTraceBackend;
pub use label_match::{labels_match, labels_match_any, LabelMatcher, MatcherOp};
pub use logs::{LogHit, LogsQueryBackend, LogsQueryRequest};
pub use traces::{TraceQueryBackend, TraceSearchHit, TraceSearchRequest};
