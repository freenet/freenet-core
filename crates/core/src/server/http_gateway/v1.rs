use axum::{extract::Path, routing::get, Extension, Router};

use super::*;

/// Registers V1-specific HTTP gateway routes.
pub(super) fn routes(config: Config) -> Router {
    Router::new()
        .route("/v1", get(home))
        .route("/v1/contract/web/{key}/", get(web_home_v1))
        .with_state(config)
        .route("/v1/contract/web/{key}/{*path}", get(web_subpages_v1))
}

async fn web_home_v1(
    key: Path<String>,
    rs: Extension<HttpGatewayRequest>,
    config: axum::extract::State<Config>,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_home(key, rs, config, ApiVersion::V1).await
}

async fn web_subpages_v1(
    Path((key, last_path)): Path<(String, String)>,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_subpages(key, last_path, ApiVersion::V1).await
}
