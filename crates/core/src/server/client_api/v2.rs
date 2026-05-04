use axum::{
    Extension, Router,
    extract::{Path, RawQuery},
    response::IntoResponse,
    routing::get,
};

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
        .route("/v2/contract/web/{key}/{*path}", get(web_subpages_v2))
        .with_state(config)
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
async fn web_root_redirect_v2(
    Path(key): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<axum::response::Response, WebSocketApiError> {
    let canonical = build_canonical_shell_url(&key, ApiVersion::V2, query.as_deref())?;
    Ok(axum::response::Redirect::permanent(&canonical).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{StatusCode, header::LOCATION};

    fn valid_key() -> &'static str {
        "EqJ5YpEEV3XLqEvKWLQHFhGAac2qXzSUoE6k2zbdnXBr"
    }

    /// Smoke test pinning that the v2 redirect carries the v2 prefix
    /// and applies the same validation + filtering that v1 does. The
    /// behaviour is otherwise identical, so the v1 tests cover the
    /// detailed cases — but a copy-paste mistake in v2.rs (e.g.
    /// `ApiVersion::V1` left over from copy) is exactly the kind of
    /// regression a one-shot smoke test catches.
    #[tokio::test]
    async fn no_trailing_slash_redirect_uses_v2_prefix_and_validates_v2() {
        // Happy path: v2 prefix + query preservation + sensitive-param stripping.
        let response = web_root_redirect_v2(
            Path(valid_key().to_string()),
            RawQuery(Some("invite=abc&authToken=evil".into())),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
        let location = response.headers().get(LOCATION).unwrap().to_str().unwrap();
        assert!(
            location.starts_with("/v2/contract/web/"),
            "v2 handler must produce a /v2/ prefix, not /v1/: {location}"
        );
        assert!(location.contains("invite=abc"), "query lost: {location}");
        assert!(
            !location.contains("authToken"),
            "authToken kept: {location}"
        );

        // Invalid-key path: same validation as v1.
        assert!(matches!(
            web_root_redirect_v2(Path("AAAA\r\nInjected: x".into()), RawQuery(None)).await,
            Err(WebSocketApiError::InvalidParam { .. })
        ));
    }
}
