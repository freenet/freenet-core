use axum::{extract::Path, routing::get, Extension, Router};

use super::*;

/// Registers V2-specific HTTP client API routes.
///
/// Currently identical in behavior to V1; exists as a routing seam for
/// future V2-specific protocol changes.
pub(super) fn routes(config: Config) -> Router {
    Router::new()
        .route("/v2", get(home))
        .route("/v2/contract/web/{key}/", get(web_home_v2))
        .with_state(config)
        .route("/v2/contract/web/{key}/{*path}", get(web_subpages_v2))
}

async fn web_home_v2(
    key: Path<String>,
    rs: Extension<HttpClientApiRequest>,
    config: axum::extract::State<Config>,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_home(key, rs, config, ApiVersion::V2).await
}

async fn web_subpages_v2(
    Path((key, last_path)): Path<(String, String)>,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_subpages(key, last_path, ApiVersion::V2).await
}
