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
        let is_transient = matches!(
            &self,
            WebSocketApiError::AxumError {
                error:
                    // Timeout from the 30s GET fetch wrapper.
                    ErrorKind::OperationError { .. }
                    // Dead in current core (no raisers), but stdlib can emit
                    // it; defensive include so a stdlib bump doesn't silently
                    // lose retry behaviour.
                    | ErrorKind::FailedOperation
                    // Node-recovery races: channel teardown, cold-start.
                    | ErrorKind::ChannelClosed
                    | ErrorKind::TransportProtocolDisconnect
                    | ErrorKind::NodeUnavailable
                    // Symmetric with OperationError("…timed out…") —
                    // a RequestError(Timeout) is the same class of
                    // transient failure during GET fetch (#3472).
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
    r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="3;url=/">
    <title>Connecting to Freenet</title>
    <link rel="icon" href="https://freenet.org/favicon.ico">
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               display: flex; justify-content: center; align-items: center; min-height: 100vh;
               margin: 0; background: #f5f5f5; }
        .container { text-align: center; padding: 2rem; }
        .logo { width: 80px; height: 80px; margin-bottom: 1.5rem; }
        h1 { color: #333; font-size: 1.5rem; margin-bottom: 0.5rem; }
        p { color: #666; line-height: 1.5; }
        a { color: #1976D2; }
    </style>
</head>
<body>
    <div class="container">
        <img src="https://freenet.org/freenet_logo.svg" alt="Freenet" class="logo">
        <h1>Connecting to Freenet...</h1>
        <p>Redirecting to the <a href="/">dashboard</a>...</p>
    </div>
</body>
</html>"#
        .to_string()
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
        body {{{{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               display: flex; justify-content: center; align-items: center; min-height: 100vh;
               margin: 0; background: #0c0d0f; color: #edeeef; }}}}
        .container {{{{ text-align: center; padding: 2rem; }}}}
        h1 {{{{ font-size: 1.2rem; font-weight: 500; margin-bottom: 0.5rem; }}}}
        p {{{{ color: #94969a; font-size: 0.85rem; margin-bottom: 0.3rem; }}}}
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
    }

    #[tokio::test]
    async fn operation_error_returns_retry_page() {
        // Transient errors during contract fetch return 503 + auto-refresh.
        let err = WebSocketApiError::AxumError {
            error: ErrorKind::OperationError {
                cause: "timed out".into(),
            },
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .is_some(),
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
