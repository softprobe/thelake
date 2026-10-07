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
        .route(
            "/robots.txt",
            get(|| async { asset_response("robots.txt") }),
        )
        .route(
            "/sitemap.xml",
            get(|| async { asset_response("sitemap.xml") }),
        )
        .route("/llms.txt", get(|| async { asset_response("llms.txt") }))
        .route("/assets/{*path}", get(nested_asset))
}

/// Shared trace/session SPA embedded into the thelake release binary. The
/// package build writes these files before Cargo runs (see `make build`).
#[derive(Embed)]
#[folder = "packages/thelake-explorer/embedded/"]
struct ExplorerAssets;

pub fn explorer_routes() -> Router {
    Router::new()
        .route(
            "/explorer",
            get(|| async { axum::response::Redirect::permanent("/explorer/") }),
        )
        .route("/explorer/", get(|| async { explorer_index() }))
        .route("/explorer/{*path}", get(explorer_asset))
}

async fn explorer_asset(Path(path): Path<String>) -> Response {
    if !is_safe_asset_path(&path) {
        return StatusCode::NOT_FOUND.into_response();
    }
    match ExplorerAssets::get(&path) {
        Some(_) => explorer_asset_response(&path),
        None => explorer_index(),
    }
}

fn explorer_index() -> Response {
    explorer_asset_response("index.html")
}

fn explorer_asset_response(path: &str) -> Response {
    match ExplorerAssets::get(path) {
        Some(file) => (
            [
                (header::CONTENT_TYPE, content_type(path)),
                (header::CACHE_CONTROL, cache_control(path)),
            ],
            file.data,
        )
            .into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
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
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("webp") => "image/webp",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("map") => "application/json",
        Some("txt") => "text/plain; charset=utf-8",
        Some("xml") => "application/xml; charset=utf-8",
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
    use tower::ServiceExt;

    fn embedded_utf8(path: &str) -> String {
        let file = WebsiteAssets::get(path).unwrap_or_else(|| panic!("{path} must be embedded"));
        String::from_utf8(file.data.to_vec())
            .unwrap_or_else(|e| panic!("{path} must be utf-8: {e}"))
    }

    #[test]
    fn embeds_landing_assets_as_utf8() {
        let html = embedded_utf8("index.html");
        assert!(html.contains("thelake"));
        assert!(html.contains("make run"));
        assert!(html.contains("200x cheaper"));
        assert!(html.contains("long-term storage"));
        assert!(html.contains("https://www.softprobe.ai/"));
        assert!(html.contains("https://github.com/softprobe/thelake"));
        assert!(html.contains("/swagger"));
        assert!(html.contains("/styles.css"));
        assert!(html.contains("/script.js"));
        assert!(html.contains("/assets/hero.svg"));
        assert!(html.contains(r#"property="og:image""#));
        assert!(html.contains("https://thelake.softprobe.ai/assets/og-image.png"));
        assert!(html.contains(r#"name="twitter:card""#));
        assert!(html.contains(r#"application/ld+json"#));
        assert!(html.contains("SoftwareApplication"));

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
    fn embeds_standalone_explorer_and_hashed_assets() {
        let html = ExplorerAssets::get("index.html").expect("Explorer index must be embedded");
        let html = String::from_utf8(html.data.to_vec()).unwrap();
        assert!(html.contains("/explorer/assets/"));
        let asset = ExplorerAssets::iter()
            .find(|path| path.starts_with("assets/") && path.ends_with(".js"))
            .expect("Explorer JS bundle must be embedded");
        assert_eq!(
            content_type(asset.as_ref()),
            "application/javascript; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn explorer_routes_serve_index_assets_and_client_routes() {
        let router = explorer_routes();
        for path in ["/explorer/", "/explorer/sessions/session-1"] {
            let response = router
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(path)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                "text/html; charset=utf-8"
            );
        }

        let js_path = ExplorerAssets::iter()
            .find(|path| path.starts_with("assets/") && path.ends_with(".js"))
            .expect("built JS bundle");
        let path = format!("/explorer/{js_path}");
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri(path)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/javascript; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn explorer_mount_does_not_capture_root_api_or_health_routes() {
        let router = Router::new()
            .route("/v1/test", get(|| async { "api" }))
            .route("/health", get(|| async { "health" }))
            .merge(explorer_routes())
            .merge(routes());

        for (path, expected) in [("/", "thelake"), ("/v1/test", "api"), ("/health", "health")] {
            let response = router
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(path)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(
                String::from_utf8_lossy(&body).contains(expected),
                "{path} should be handled by its own route"
            );
        }
    }

    #[test]
    fn embeds_seo_crawl_and_llm_surface() {
        let robots = embedded_utf8("robots.txt");
        assert!(robots.contains("User-agent: *"));
        assert!(robots.contains("Allow: /"));
        assert!(robots.contains("Disallow: /v1/"));
        assert!(robots.contains("Sitemap: https://thelake.softprobe.ai/sitemap.xml"));

        let sitemap = embedded_utf8("sitemap.xml");
        assert!(sitemap.contains("https://thelake.softprobe.ai/"));
        assert!(sitemap.contains("<urlset"));

        let llms = embedded_utf8("llms.txt");
        assert!(llms.contains("# thelake"));
        assert!(llms.contains("https://thelake.softprobe.ai/"));
        assert!(llms.contains("https://github.com/softprobe/thelake"));
        assert!(llms.contains("DuckDB"));
        assert!(llms.contains("200x"));
        assert!(llms.contains("make run"));

        assert!(
            WebsiteAssets::get("assets/og-image.png").is_some(),
            "assets/og-image.png must be embedded"
        );
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
        assert_eq!(content_type("robots.txt"), "text/plain; charset=utf-8");
        assert_eq!(content_type("llms.txt"), "text/plain; charset=utf-8");
        assert_eq!(
            content_type("sitemap.xml"),
            "application/xml; charset=utf-8"
        );
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
