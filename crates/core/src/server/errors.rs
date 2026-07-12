use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use freenet_stdlib::client_api::{ErrorKind, RequestError};
use freenet_stdlib::prelude::ContractInstanceId;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub(super) enum WebSocketApiError {
    /// Something went wrong when calling the user repo.
    InvalidParam {
        error_cause: String,
    },
    NodeError {
        error_cause: String,
    },
    AxumError {
        error: ErrorKind,
    },
    MissingContract {
        instance_id: ContractInstanceId,
    },
}

impl WebSocketApiError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            WebSocketApiError::InvalidParam { .. } => StatusCode::BAD_REQUEST,
            WebSocketApiError::NodeError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            WebSocketApiError::AxumError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            WebSocketApiError::MissingContract { .. } => StatusCode::NOT_FOUND,
        }
    }

    pub fn error_message(&self) -> String {
        match self {
            WebSocketApiError::InvalidParam { error_cause } => {
                format!("Invalid request params: {error_cause}")
            }
            WebSocketApiError::NodeError { error_cause } => format!("Node error: {error_cause}"),
            WebSocketApiError::AxumError { error } => format!("Server error: {error}"),
            WebSocketApiError::MissingContract { instance_id } => {
                format!("Missing contract {}", instance_id.encode())
            }
        }
    }
}

impl Display for WebSocketApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error_message())
    }
}

impl From<WebSocketApiError> for Response {
    fn from(error: WebSocketApiError) -> Self {
        let body = Html(error.error_message());
        (error.status_code(), body).into_response()
    }
}

impl IntoResponse for WebSocketApiError {
    fn into_response(self) -> Response {
        // Check for errors that indicate the peer is still connecting to the network
        if let WebSocketApiError::AxumError { ref error } = self {
            if matches!(error, ErrorKind::EmptyRing | ErrorKind::PeerNotJoined) {
                return (StatusCode::SERVICE_UNAVAILABLE, Html(connecting_page())).into_response();
            }
        }

        // Transient errors during contract fetch: the node has peers but the
        // GET hasn't completed yet (fresh node, sparse ring, etc.).  Show a
        // retry page that auto-refreshes the SAME URL so the user doesn't
        // have to manually reload (#3472).
        //
        // Note: `ErrorKind` is `#[non_exhaustive]` so new stdlib variants
        // will NOT match here.  Each new release must explicitly decide
        // whether the new variant is transient.  The wildcard `_` at the
        // bottom of the `match` falls through to 500.
        // NOTE: `ErrorKind::OperationError` is deliberately NOT listed here.
        // It is dual-use: the web GET path synthesizes it for transient
        // timeouts AND the node emits it for terminal failures (e.g. a locally
        // banned contract, exhausted GET). Treating it as transient would
        // serve an infinite auto-refresh page for those terminal errors. The
        // transient GET cases now use RequestError(Timeout) / ChannelClosed
        // (see handle_get_response in path_handlers.rs), which are caught below.
        let is_transient = matches!(
            &self,
            WebSocketApiError::AxumError {
                error:
                    // Dead in current core (no raisers), but stdlib can emit
                    // it; defensive include so a stdlib bump doesn't silently
                    // lose retry behaviour.
                    ErrorKind::FailedOperation
                    // Node-recovery races: channel teardown, cold-start.
                    | ErrorKind::ChannelClosed
                    | ErrorKind::TransportProtocolDisconnect
                    | ErrorKind::NodeUnavailable
                    // The 30s GET fetch wrapper elapsed — the canonical #3472
                    // transient case. Synthesized by handle_get_response.
                    | ErrorKind::RequestError(RequestError::Timeout)
            }
        );

        let (status, error_message) = if is_transient {
            // Log the cause so operators can distinguish a fast op error
            // from a slow-loading contract without changing the user-facing
            // retry page.
            if let WebSocketApiError::AxumError { error } = &self {
                tracing::info!(%error, "serving retry page for transient contract-fetch error");
            }
            (StatusCode::SERVICE_UNAVAILABLE, retry_loading_page())
        } else {
            match self {
                WebSocketApiError::InvalidParam { error_cause } => {
                    (StatusCode::BAD_REQUEST, error_cause)
                }
                WebSocketApiError::NodeError { error_cause }
                    if error_cause.starts_with("Contract not found") =>
                {
                    (StatusCode::NOT_FOUND, error_cause)
                }
                WebSocketApiError::NodeError { error_cause } => {
                    (StatusCode::INTERNAL_SERVER_ERROR, error_cause)
                }
                err @ WebSocketApiError::MissingContract { .. } => {
                    (StatusCode::NOT_FOUND, err.error_message())
                }
                WebSocketApiError::AxumError { error } => {
                    // Already handled transient cases above; remaining
                    // AxumErrors are infrastructure failures.
                    (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}"))
                }
            }
        };

        let body = Html(error_message);

        // Prevent intermediaries/service-workers from pinning a stale retry
        // page, and signal the client when it may retry.
        let mut response = (status, body).into_response();
        if is_transient {
            response.headers_mut().insert(
                axum::http::header::CACHE_CONTROL,
                axum::http::HeaderValue::from_static("no-store"),
            );
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_str(&RETRY_REFRESH_SECS.to_string())
                    .unwrap_or(axum::http::HeaderValue::from_static("60")),
            );
        }

        response
    }
}

/// Returns a short HTML page that redirects to the dashboard while the peer connects.
///
/// The homepage at `/` already shows full connection diagnostics, so rather than
/// duplicating that rendering here we redirect with a meta-refresh. The 503 status
/// code (set by the caller) tells programmatic clients the node is not yet ready.
fn connecting_page() -> String {
    include_str!("errors/assets/connecting.html").to_string()
}

/// How often the retry page reloads (seconds).  Long enough for a
/// cold GET to resolve on a sparse ring, short enough that the user
/// doesn't assume the page is dead.
const RETRY_REFRESH_SECS: u64 = 60;

/// Returns an HTML page that auto-refreshes the current URL every
/// [`RETRY_REFRESH_SECS`] seconds.
///
/// Used when a contract-fetch operation timed out — the node has peers and
/// is making progress, but the specific GET hasn't resolved yet.  Rather
/// than showing a dead error, the page reloads itself.  Once the contract
/// is cached, `contract_home` serves normally and the refresh loop stops
/// (the success response carries no `<meta http-equiv="refresh">`).
fn retry_loading_page() -> String {
    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="{refresh}">
    <title>Loading contract…</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               display: flex; justify-content: center; align-items: center; min-height: 100vh;
               margin: 0; background: #0c0d0f; color: #edeeef; }}
        .container {{ text-align: center; padding: 2rem; }}
        h1 {{ font-size: 1.2rem; font-weight: 500; margin-bottom: 0.5rem; }}
        p {{ color: #94969a; font-size: 0.85rem; margin-bottom: 0.3rem; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Fetching contract from the network…</h1>
        <p>This page will reload automatically.</p>
        <p style="font-size:0.7rem;color:#585a5e">If this persists, check the <a href="/" style="color:#0abab5">dashboard</a>.</p>
    </div>
</body>
</html>"##,
        refresh = RETRY_REFRESH_SECS,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ring_returns_service_unavailable() {
        let err = WebSocketApiError::AxumError {
            error: ErrorKind::EmptyRing,
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn connecting_page_redirects_to_root_only_at_top_level() {
        // Regression (peer-restart breaks a framed webapp): the connecting page
        // is served (503, EmptyRing/PeerNotJoined) while the peer rejoins the
        // ring, and it can render INSIDE a webapp's sandboxed shell iframe as
        // well as top-level. It must NOT unconditionally navigate to the node
        // root `/`: doing so inside the iframe yanks the running webapp to a
        // page that denies framing on hardened deployments ("Refused to
        // display ... X-Frame-Options: deny" -> broken tab, exactly the River
        // peer-restart symptom). The redirect to `/` must be conditioned on
        // being the top document; a framed instance reloads its OWN url in
        // place. See the head comment in connecting.html.
        let page = connecting_page();
        assert!(
            page.contains("window.top === window.self"),
            "connecting page must condition the /-redirect on being the top document"
        );
        assert!(
            page.contains("window.location.reload()"),
            "a framed connecting page must reload its own url instead of navigating to /"
        );
        // The only meta-refresh to `/` must be the <noscript> no-JS fallback,
        // never an unconditional redirect that also fires in a sandboxed frame.
        let noscript_fallback =
            r#"<noscript><meta http-equiv="refresh" content="3;url=/" /></noscript>"#;
        assert!(
            page.contains(noscript_fallback),
            "url=/ meta-refresh must be wrapped in <noscript> as the no-JS fallback"
        );
        assert!(
            !page.replace(noscript_fallback, "").contains("url=/"),
            "connecting page must not contain an unconditional top-level url=/ redirect"
        );
        // MAJOR #1 (PR #4781 review): a TOP-LEVEL connecting page that carries
        // the shell's `_freload` recovery marker is a recovery reload that
        // momentarily landed here during ring rejoin. It must NOT go to `/`
        // (that abandons the app for the dashboard — the app-loss this recovery
        // exists to prevent); it must retry the contract URL in place. The
        // decision is centralised in the pure `connectingRecoveryDecision`.
        assert!(
            page.contains("_freload"),
            "connecting page must recognize the shell's _freload recovery reload"
        );
        assert!(
            page.contains("function connectingRecoveryDecision("),
            "the redirect decision must be a pure, testable function of (_freload, atTop, now)"
        );
        // MAJOR #3 (PR #4781 review): the retry-in-place must be BOUNDED, not a
        // 3s-forever loop. `_freload` carries a start timestamp; after the window
        // the page stops looping (top-level -> `/`, framed -> a stop message).
        assert!(
            page.contains("RECOVERY_MAX_MS"),
            "the recovery retry must be time-bounded, not an unconditional forever loop"
        );
        assert!(
            page.contains("window.location.replace('/')"),
            "top-level recovery must fall back to the dashboard once the window elapses"
        );
    }

    #[test]
    fn peer_not_joined_returns_service_unavailable() {
        let err = WebSocketApiError::AxumError {
            error: ErrorKind::PeerNotJoined,
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn failed_operation_returns_retry_page() {
        let err = WebSocketApiError::AxumError {
            error: ErrorKind::FailedOperation,
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .map(|v| v.as_bytes()),
            Some(&b"no-store"[..]),
        );
        assert!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .is_some(),
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body);
        assert!(
            text.contains(r#"<meta http-equiv="refresh" content="60"#),
            "retry page must contain meta-refresh tag"
        );
        // The CSS must use single braces — a `format!` brace-escaping bug
        // ({{{{ instead of {{) would emit `body {{ … }}`, invalid CSS that
        // strips the page's styling.
        assert!(
            text.contains("body {") && !text.contains("body {{"),
            "retry page CSS must emit single braces, got: {text}"
        );
    }

    #[test]
    fn operation_error_returns_internal_server_error() {
        // OperationError is dual-use: the node emits it for TERMINAL failures
        // (banned contract, exhausted GET) as well as transient ones, so it
        // must NOT be classified transient — otherwise a banned contract would
        // infinite-reload instead of showing an error. The transient GET cases
        // use RequestError(Timeout)/ChannelClosed instead (#3472).
        let err = WebSocketApiError::AxumError {
            error: ErrorKind::OperationError {
                cause: "contract banned".into(),
            },
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        // And it must NOT carry the retry-page caching headers.
        assert!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .is_none(),
        );
    }

    #[test]
    fn channel_closed_returns_retry_page() {
        let err = WebSocketApiError::AxumError {
            error: ErrorKind::ChannelClosed,
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn transport_disconnect_returns_retry_page() {
        let err = WebSocketApiError::AxumError {
            error: ErrorKind::TransportProtocolDisconnect,
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn node_unavailable_returns_retry_page() {
        let err = WebSocketApiError::AxumError {
            error: ErrorKind::NodeUnavailable,
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn request_error_timeout_returns_retry_page() {
        // A RequestError(Timeout) is the same class of transient GET-fetch
        // failure as OperationError("…timed out…") and must serve the 503
        // auto-refresh page rather than a dead error (#3472).
        let err = WebSocketApiError::AxumError {
            error: ErrorKind::RequestError(RequestError::Timeout),
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn other_axum_error_returns_internal_server_error() {
        let err = WebSocketApiError::AxumError {
            error: ErrorKind::Unhandled {
                cause: "something broke".into(),
            },
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
