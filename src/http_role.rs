//! Process HTTP role for split ingest/query deployments.
//!
//! Demo Astronomy Shop runs two `softprobe-runtime` processes that share the
//! same DuckLake catalog + data path: one accepts OTLP writes, the other serves
//! PromQL/Loki/Tempo. Overlap is allowed; each process stays on its own core
//! budget (`SOFTPROBE_HTTP_ROLE`).

/// Which HTTP surfaces this process serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpRole {
    /// Default: ingest + query on one process (production / tests).
    All,
    /// OTLP + control-plane writes; no Prom/Loki/Tempo.
    Ingest,
    /// Prom/Loki/Tempo + SQL/telemetry reads; no OTLP ingest.
    Query,
}

impl HttpRole {
    /// `SOFTPROBE_HTTP_ROLE=all|ingest|write|query|read` (default `all`).
    pub fn from_env() -> Self {
        match std::env::var("SOFTPROBE_HTTP_ROLE")
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "ingest" | "write" => Self::Ingest,
            "query" | "read" => Self::Query,
            _ => Self::All,
        }
    }

    pub fn serves_ingest(self) -> bool {
        matches!(self, Self::All | Self::Ingest)
    }

    pub fn serves_query(self) -> bool {
        matches!(self, Self::All | Self::Query)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Ingest => "ingest",
            Self::Query => "query",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_aliases() {
        assert!(HttpRole::Ingest.serves_ingest());
        assert!(!HttpRole::Ingest.serves_query());
        assert!(HttpRole::Query.serves_query());
        assert!(!HttpRole::Query.serves_ingest());
        assert!(HttpRole::All.serves_ingest() && HttpRole::All.serves_query());
    }
}
