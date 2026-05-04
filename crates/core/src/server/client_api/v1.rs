use axum::{Extension, Router, extract::Path, response::Redirect, routing::get};

use super::*;

/// Registers V1-specific HTTP client API routes.
pub(super) fn routes(config: Config) -> Router {
    Router::new()
        .route("/v1", get(home))
        // No-trailing-slash redirect to the canonical contract root URL.
        // Without this, pasting `/v1/contract/web/<key>` (no slash)
        // into the address bar 404s — common HTTP UX is to either
        // accept both forms or redirect, so we 308 to the canonical
        // form. 308 (Permanent Redirect) preserves the request method
        // and is the modern equivalent of 301 for GETs.
        // (freenet/freenet-core#4019)
        .route("/v1/contract/web/{key}", get(web_root_redirect_v1))
        .route("/v1/contract/web/{key}/", get(web_home_v1))
        .with_state(config)
        .route("/v1/contract/web/{key}/{*path}", get(web_subpages_v1))
}

async fn web_home_v1(
    key: Path<String>,
    rs: Extension<HttpClientApiRequest>,
    config: axum::extract::State<Config>,
    headers: axum::http::HeaderMap,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_home(key, rs, config, headers, ApiVersion::V1, query).await
}

async fn web_subpages_v1(
    Path((key, last_path)): Path<(String, String)>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    headers: axum::http::HeaderMap,
    Extension(rs): Extension<HttpClientApiRequest>,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_subpages(key, last_path, ApiVersion::V1, query, headers, rs).await
}

/// Redirect `/v1/contract/web/{key}` (no trailing slash) to
/// `/v1/contract/web/{key}/` (with trailing slash, the canonical form
/// the rest of the routing already understands). 308 preserves the
/// method and any query string the browser carries forward.
async fn web_root_redirect_v1(Path(key): Path<String>) -> Redirect {
    Redirect::permanent(&format!("/v1/contract/web/{key}/"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{StatusCode, header::LOCATION};
    use axum::response::IntoResponse;

    /// Regression test for freenet/freenet-core#4019.
    ///
    /// Pasting `/v1/contract/web/<key>` (no trailing slash) into a browser
    /// used to 404 because no route matched. Now the no-slash form 308s
    /// to the canonical trailing-slash form, which the rest of the
    /// routing recognises. 308 (not 301) is intentional: it preserves
    /// the request method and is the modern equivalent for permanent
    /// redirects of GETs.
    #[tokio::test]
    async fn no_trailing_slash_redirects_to_canonical_root_v1() {
        let key = "EqJ5YpEEV3XLqEvKWLQHFhGAac2qXzSUoE6k2zbdnXBr";
        let redirect = web_root_redirect_v1(Path(key.to_string())).await;
        let response = redirect.into_response();

        assert_eq!(
            response.status(),
            StatusCode::PERMANENT_REDIRECT,
            "must return 308 so the browser repeats the request method \
             at the canonical URL (#4019)"
        );
        let location = response
            .headers()
            .get(LOCATION)
            .expect("redirect must set Location header")
            .to_str()
            .unwrap();
        assert_eq!(
            location, "/v1/contract/web/EqJ5YpEEV3XLqEvKWLQHFhGAac2qXzSUoE6k2zbdnXBr/",
            "Location must be the canonical trailing-slash form so the \
             browser's follow-up request hits `/v1/contract/web/{{key}}/` \
             which web_home_v1 already handles"
        );
    }
}
