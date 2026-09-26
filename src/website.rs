//! Embedded open-source landing page (`website/`), served at `/` on the HTTP host.

use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "website/"]
struct WebsiteAssets;

pub fn routes() -> Router {
    Router::new()
        .route("/", get(|| async { asset_response("index.html") }))
        .route(
            "/styles.css",
            get(|| async { asset_response("styles.css") }),
        )
        .route("/script.js", get(|| async { asset_response("script.js") }))
        .route("/assets/{*path}", get(nested_asset))
}

async fn nested_asset(Path(path): Path<String>) -> Response {
    if !is_safe_asset_path(&path) {
        return StatusCode::NOT_FOUND.into_response();
    }
    asset_response(&format!("assets/{path}"))
}

fn is_safe_asset_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\0')
        && !path
            .split('/')
            .any(|seg| seg.is_empty() || seg == "." || seg == "..")
}

fn asset_response(path: &str) -> Response {
    match WebsiteAssets::get(path) {
        Some(file) => {
            let mime = content_type(path);
            (
                [
                    (header::CONTENT_TYPE, mime),
                    (header::CACHE_CONTROL, cache_control(path)),
                ],
                file.data,
            )
                .into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        _ => "application/octet-stream",
    }
}

fn cache_control(path: &str) -> &'static str {
    if path == "index.html" {
        "no-cache"
    } else {
        "public, max-age=3600"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn embedded_utf8(path: &str) -> String {
        let file = WebsiteAssets::get(path).unwrap_or_else(|| panic!("{path} must be embedded"));
        String::from_utf8(file.data.to_vec())
            .unwrap_or_else(|e| panic!("{path} must be utf-8: {e}"))
    }

    #[test]
    fn embeds_landing_assets_as_utf8() {
        let html = embedded_utf8("index.html");
        assert!(html.contains("thelake"));
        assert!(html.contains("cargo run --bin thelake"));
        assert!(!html.contains("softprobe-runtime"));
        assert!(html.contains("The Open Source LangSmith / Datadog Alternative Built on DuckDB."));
        assert!(html.contains("200x cheaper"));
        assert!(html.contains("long-term storage"));
        assert!(html.contains("https://www.softprobe.ai/"));
        assert!(html.contains("https://github.com/softprobe/thelake"));
        assert!(html.contains("/swagger"));
        assert!(html.contains("/styles.css"));
        assert!(html.contains("/script.js"));
        assert!(html.contains("/assets/hero.svg"));

        let css = embedded_utf8("styles.css");
        assert!(css.contains(".hero"));
        assert!(css.contains("--rail"));
        assert!(css.contains("--accent"));

        let js = embedded_utf8("script.js");
        assert!(js.contains("IntersectionObserver"));

        let svg = embedded_utf8("assets/hero.svg");
        assert!(svg.contains("thelake evidence depth"));
        assert!(svg.contains("open SQL on your lake"));
    }

    #[test]
    fn safe_asset_path_rejects_traversal() {
        assert!(is_safe_asset_path("hero.svg"));
        assert!(!is_safe_asset_path("../styles.css"));
        assert!(!is_safe_asset_path(".."));
        assert!(!is_safe_asset_path("/hero.svg"));
        assert!(!is_safe_asset_path(""));
        assert!(!is_safe_asset_path("a//b"));
    }

    #[test]
    fn content_type_covers_shipped_extensions_only() {
        assert_eq!(content_type("index.html"), "text/html; charset=utf-8");
        assert_eq!(content_type("styles.css"), "text/css; charset=utf-8");
        assert_eq!(
            content_type("script.js"),
            "application/javascript; charset=utf-8"
        );
        assert_eq!(content_type("assets/hero.svg"), "image/svg+xml");
        assert_eq!(content_type("assets/logos/grpc.png"), "image/png");
        assert_eq!(content_type("unknown.bin"), "application/octet-stream");
    }

    #[test]
    fn embeds_brand_logos() {
        for path in [
            "assets/logos/thelake.svg",
            "assets/logos/softprobe.svg",
            "assets/logos/opentelemetry.svg",
            "assets/logos/duckdb.svg",
            "assets/logos/ducklake.svg",
            "assets/logos/grafana.svg",
            "assets/logos/loki.svg",
            "assets/logos/tempo.svg",
            "assets/logos/parquet.svg",
            "assets/logos/grpc.png",
        ] {
            assert!(
                WebsiteAssets::get(path).is_some(),
                "{path} must be embedded"
            );
        }
    }
}
