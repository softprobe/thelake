//! Shared Grafana/Loki/Tempo compatibility layer.
//!
//! Protocol HTTP adapters stay thin: parse the wire request,
//! call typed backends with a [`CompatWorkspaceContext`], and encode protocol responses.
//! Auth, projection, ordering, and error classes live here — not under a
//! single protocol module.

pub mod backends;
pub mod capability;
pub mod envelopes;
pub mod errors;
pub mod loki;
pub mod ordering;
pub mod projection;
pub mod query_string;
pub mod stubs;
pub mod tempo;
pub mod workspace;

pub use capability::{load_capability_v0, CapabilityManifest};
pub use errors::{CompatError, CompatErrorCode};
pub use workspace::{CompatWorkspaceContext, ProtocolScope, QueryLimits};
