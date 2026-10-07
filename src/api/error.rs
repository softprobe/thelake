use axum::http::StatusCode;
use axum::Json;
use serde_json::{json, Value};
use tracing::warn;

pub type ApiError = (StatusCode, Json<Value>);

pub fn bad_request(message: impl Into<String>) -> ApiError {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "error": message.into() })),
    )
}

pub fn not_found() -> ApiError {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" })))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageErrorKind {
    pub code: &'static str,
    pub retryable: bool,
}

/// Map a storage-layer failure to a response the caller can act on without leaking internals.
///
/// 1. Never echo the raw error: DuckDB surfaces the full ATTACH target on connection failure,
///    and for a Postgres catalog that string is the DSN (including plaintext password).
///    GCS HMAC secrets reach it the same way via `CREATE SECRET` statement echo.
/// 2. Classify on the error prefix, not on a substring of it, so user literals in echoed SQL
///    cannot flip error classification.
pub fn storage_error(error: anyhow::Error) -> ApiError {
    let raw = error.to_string();

    let error_id = {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        raw.hash(&mut h);
        format!("{:016x}", h.finish())
    };
    warn!("query failed [{}]: {}", error_id, raw);

    let kind = classify_storage_error_chain(&error);
    let status = if kind.retryable {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };

    (
        status,
        Json(json!({
            "error": kind.code,
            "retryable": kind.retryable,
            "error_id": error_id,
        })),
    )
}

/// Classify using the full chain: our `.context()` prefixes must not hide DuckDB
/// `IO Error` / `HTTP Error` markers.
pub fn classify_storage_error_chain(error: &anyhow::Error) -> StorageErrorKind {
    let top = error.to_string();
    let top_kind = classify_storage_error(&top);
    if top_kind.code != "query_failed" {
        return top_kind;
    }
    let root = error.root_cause().to_string();
    if root != top {
        return classify_storage_error(&root);
    }
    top_kind
}

/// Match only on the leading marker DuckDB emits, so echoed SQL cannot influence classification.
pub fn classify_storage_error(raw: &str) -> StorageErrorKind {
    let head = raw.trim_start();

    for marker in [
        "DuckDB worker",
        "DuckDB query engine failed to start",
        "DuckDB open failed",
        "DuckDB ATTACH failed",
        "DuckLake attach failed",
    ] {
        if head.starts_with(marker) {
            return StorageErrorKind {
                code: "query_unavailable",
                retryable: true,
            };
        }
    }

    if head.starts_with("FATAL Error") {
        return StorageErrorKind {
            code: "query_unavailable",
            retryable: true,
        };
    }

    for marker in [
        "Connection Error",
        "IO Error",
        "HTTP Error",
        "Network Error",
    ] {
        if head.starts_with(marker) {
            return StorageErrorKind {
                code: "query_unavailable",
                retryable: true,
            };
        }
    }

    StorageErrorKind {
        code: "query_failed",
        retryable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_retryable_io_and_network_errors() {
        assert!(classify_storage_error("IO Error: connection lost").retryable);
        assert!(classify_storage_error("HTTP Error: 503 Service Unavailable").retryable);
        assert!(classify_storage_error("Connection Error: reset by peer").retryable);
        assert!(classify_storage_error("DuckLake attach failed: pg timeout").retryable);
        assert!(classify_storage_error("FATAL Error: database invalidated").retryable);
    }

    #[test]
    fn classifies_deterministic_query_defects() {
        let err = classify_storage_error("Binder Error: column 'foo' not found");
        assert_eq!(err.code, "query_failed");
        assert!(!err.retryable);

        let err = classify_storage_error("Catalog Error: table does not exist");
        assert_eq!(err.code, "query_failed");
        assert!(!err.retryable);
    }

    #[test]
    fn storage_error_redacts_dsn_and_echoes_error_id() {
        let raw =
            anyhow::anyhow!("DuckLake attach failed: postgresql://user:secretpass@host:5432/db");
        let (status, Json(val)) = storage_error(raw);
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(val["error"], "query_unavailable");
        assert_eq!(val["retryable"], true);
        assert!(val["error_id"].is_string());
        let str_rep = serde_json::to_string(&val).unwrap();
        assert!(!str_rep.contains("secretpass"));
    }
}
