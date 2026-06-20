use axum::{
    Extension, Router,
    extract::{Path, RawQuery},
    response::IntoResponse,
    routing::get,
};

use super::*;

/// Registers V1-specific HTTP client API routes.
pub(super) fn routes(config: Config) -> Router {
    Router::new()
        .route("/v1", get(home))
        // Lightweight runtime-version probe. The homepage JS fetches this at
        // page load and compares the live answer against the version baked into
        // the served page; a mismatch means the browser is holding a stale
        // cached page from an older binary (#4289).
        .route("/v1/version", get(runtime_version))
        // No-trailing-slash redirect to the canonical contract root URL.
        // Without this, pasting `/v1/contract/web/<key>` (no slash)
        // into the address bar 404s — common HTTP UX is to either
        // accept both forms or redirect, so we 308 to the canonical
        // form. 308 (Permanent Redirect) preserves the request method
        // and is the modern equivalent of 301 for GETs.
        // (freenet/freenet-core#4019)
        .route("/v1/contract/web/{key}", get(web_root_redirect_v1))
        .route("/v1/contract/web/{key}/", get(web_home_v1))
        .route("/v1/contract/web/{key}/{*path}", get(web_subpages_v1))
        .with_state(config)
}

/// JSON returned by `GET /v1/version`.
#[derive(serde::Serialize)]
struct RuntimeVersion {
    /// Version of the binary currently serving requests
    /// (`CARGO_PKG_VERSION` of the running process).
    version: &'static str,
}

/// `GET /v1/version` — reports the running node's version so the homepage JS
/// can detect when it is rendering a stale, cached page from an older binary.
///
/// Returns `PCK_VERSION` (the compile-time version of *this* live process)
/// rather than the `network_status` snapshot: both are seeded from the same
/// constant, but reading the constant directly is always available, even
/// during the startup window before the snapshot exists.
async fn runtime_version() -> impl IntoResponse {
    axum::Json(RuntimeVersion {
        version: crate::config::PCK_VERSION,
    })
}

async fn web_home_v1(
    key: Path<String>,
    rs: Extension<HttpClientApiRequest>,
    // Tolerant: see `hosted_mode_or_default` — absent ⇒ hosted-off, so the
    // standalone `as_router` composition (no HostedMode layer) doesn't 500.
    hosted_mode: Option<Extension<crate::server::HostedMode>>,
    config: axum::extract::State<Config>,
    headers: axum::http::HeaderMap,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_home(
        key,
        rs,
        config,
        headers,
        ApiVersion::V1,
        query,
        hosted_mode_or_default(hosted_mode),
    )
    .await
}

async fn web_subpages_v1(
    Path((key, last_path)): Path<(String, String)>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    // Tolerant: see `hosted_mode_or_default`.
    hosted_mode: Option<Extension<crate::server::HostedMode>>,
    headers: axum::http::HeaderMap,
    axum::extract::State(config): axum::extract::State<Config>,
    Extension(rs): Extension<HttpClientApiRequest>,
) -> Result<axum::response::Response, WebSocketApiError> {
    web_subpages(
        key,
        last_path,
        ApiVersion::V1,
        query,
        headers,
        &config,
        rs,
        hosted_mode_or_default(hosted_mode),
    )
    .await
}

/// Redirect `/v1/contract/web/{key}` (no trailing slash) to
/// `/v1/contract/web/{key}/` (with trailing slash, the canonical form
/// the rest of the routing already understands).
///
/// Routes the redirect target through `build_canonical_shell_url` so
/// the same key validation (CRLF / path-traversal rejection) and
/// sensitive-query-param filter (`__sandbox`, `authToken`) that
/// `redirect_to_shell_root` applies are also applied here. Returning
/// a 308 (instead of 303 like the cross-contract subpage redirect)
/// preserves the request method and lets the browser cache the
/// redirect — the address bar lands on the canonical URL on
/// subsequent visits.
async fn web_root_redirect_v1(
    Path(key): Path<String>,
    RawQuery(query): RawQuery,
) -> Result<axum::response::Response, WebSocketApiError> {
    let canonical = build_canonical_shell_url(&key, ApiVersion::V1, query.as_deref())?;
    Ok(axum::response::Redirect::permanent(&canonical).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{StatusCode, header::LOCATION};

    fn valid_key() -> &'static str {
        "EqJ5YpEEV3XLqEvKWLQHFhGAac2qXzSUoE6k2zbdnXBr"
    }

    /// `GET /v1/version` must report the running binary's version as JSON so
    /// the homepage stale-assets check (#4289) has a live runtime version to
    /// compare against. The reported version must match the compiled-in
    /// `CARGO_PKG_VERSION`.
    #[tokio::test]
    async fn runtime_version_reports_running_pkg_version() {
        use axum::body::to_bytes;

        let response = runtime_version().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json.get("version").and_then(|v| v.as_str()),
            Some(env!("CARGO_PKG_VERSION")),
            "endpoint must report the running binary's CARGO_PKG_VERSION"
        );
    }

    /// Regression test for freenet/freenet-core#4019.
    ///
    /// Pasting `/v1/contract/web/<key>` (no trailing slash) into a browser
    /// used to 404. Now the no-slash form 308s to the canonical
    /// trailing-slash form. 308 preserves the request method and lets
    /// the browser cache the redirect.
    #[tokio::test]
    async fn no_trailing_slash_redirects_to_canonical_root_v1() {
        let response = web_root_redirect_v1(Path(valid_key().to_string()), RawQuery(None))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
        let location = response
            .headers()
            .get(LOCATION)
            .expect("redirect must set Location")
            .to_str()
            .unwrap();
        assert_eq!(
            location,
            format!("/v1/contract/web/{}/", valid_key()),
            "Location must be the canonical trailing-slash form"
        );
    }

    /// Regression test for the H1 review finding on PR #4020. The
    /// no-slash redirect MUST forward the original query string —
    /// pasting `/v1/contract/web/<key>?invite=abc` is exactly the
    /// flow River invite links produce, and dropping `?invite=abc`
    /// silently breaks them.
    #[tokio::test]
    async fn no_trailing_slash_redirect_preserves_query_string_v1() {
        let response = web_root_redirect_v1(
            Path(valid_key().to_string()),
            RawQuery(Some("invite=abc&room=42".into())),
        )
        .await
        .unwrap();

        let location = response
            .headers()
            .get(LOCATION)
            .expect("redirect must set Location")
            .to_str()
            .unwrap();
        assert_eq!(
            location,
            format!("/v1/contract/web/{}/?invite=abc&room=42", valid_key()),
            "query string must be carried through the redirect so \
             `?invite=…` no-slash links keep working"
        );
    }

    /// Regression test for the M1 review finding. The same
    /// `__sandbox` / `authToken` filter `redirect_to_shell_root`
    /// applies must apply here too — otherwise `/v1/contract/web/<key>?
    /// authToken=attacker` becomes a 308 to a URL that injects an
    /// attacker-chosen token into the destination shell.
    #[tokio::test]
    async fn no_trailing_slash_redirect_strips_sensitive_query_params_v1() {
        let response = web_root_redirect_v1(
            Path(valid_key().to_string()),
            RawQuery(Some("authToken=evil&invite=ok&__sandbox=1".into())),
        )
        .await
        .unwrap();

        let location = response.headers().get(LOCATION).unwrap().to_str().unwrap();
        assert!(
            location.contains("invite=ok"),
            "non-sensitive query params must survive: {location}"
        );
        assert!(
            !location.contains("authToken"),
            "authToken must be stripped: {location}"
        );
        assert!(
            !location.contains("__sandbox"),
            "__sandbox must be stripped: {location}"
        );
    }

    /// Regression test for the H2 review finding. The path parameter
    /// must be validated before being interpolated into a `Location`
    /// header. `redirect_to_shell_root` already does this on the
    /// sibling redirect; the no-slash redirect must too — without it,
    /// CRLF in `key` could inject arbitrary headers, and a
    /// path-traversal-style key would point the redirect at an
    /// attacker-chosen URL.
    #[tokio::test]
    async fn no_trailing_slash_redirect_rejects_invalid_key_v1() {
        // CRLF-bearing key — the header-injection case.
        assert!(matches!(
            web_root_redirect_v1(Path("AAAA\r\nInjected: x".into()), RawQuery(None)).await,
            Err(WebSocketApiError::InvalidParam { .. })
        ));
        // Obvious garbage.
        assert!(matches!(
            web_root_redirect_v1(Path("not-a-real-contract-key".into()), RawQuery(None)).await,
            Err(WebSocketApiError::InvalidParam { .. })
        ));
        // Empty string (axum would normally reject this at routing
        // time, but defend against an internal caller too).
        assert!(matches!(
            web_root_redirect_v1(Path(String::new()), RawQuery(None)).await,
            Err(WebSocketApiError::InvalidParam { .. })
        ));
    }
}
