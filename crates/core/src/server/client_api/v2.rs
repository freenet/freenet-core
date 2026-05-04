use axum::{Extension, Router, extract::Path, response::Redirect, routing::get};

use super::*;

/// Registers V2-specific HTTP client API routes.
///
/// Currently identical in behavior to V1; exists as a routing seam for
/// future V2-specific protocol changes.
pub(super) fn routes(config: Config) -> Router {
    Router::new()
        .route("/v2", get(home))
        // No-trailing-slash redirect — see v1.rs for the rationale.
        .route("/v2/contract/web/{key}", get(web_root_redirect_v2))
        .route("/v2/contract/web/{key}/", get(web_home_v2))
        .with_state(config)
        .route("/v2/contract/web/{key}/{*path}", get(web_subpages_v2))
}

async fn web_home_v2(
    key: Path<String>,
    rs: Extension<HttpClientApiRequest>,
    config: axum::extract::State<Config>,
    headers: axum::http::HeaderMap,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_home(key, rs, config, headers, ApiVersion::V2, query).await
}

async fn web_subpages_v2(
    Path((key, last_path)): Path<(String, String)>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    headers: axum::http::HeaderMap,
    Extension(rs): Extension<HttpClientApiRequest>,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_subpages(key, last_path, ApiVersion::V2, query, headers, rs).await
}

/// Redirect `/v2/contract/web/{key}` (no trailing slash) to
/// `/v2/contract/web/{key}/` — see v1.rs for the rationale.
async fn web_root_redirect_v2(Path(key): Path<String>) -> Redirect {
    Redirect::permanent(&format!("/v2/contract/web/{key}/"))
}
