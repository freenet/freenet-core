use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use freenet_stdlib::client_api::ErrorKind;
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

        let (status, error_message) = match self {
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
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}"))
            }
        };

        let body = Html(error_message);

        (status, body).into_response()
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

    #[test]
    fn other_axum_error_returns_internal_server_error() {
        let err = WebSocketApiError::AxumError {
            error: ErrorKind::OperationError {
                cause: "something broke".into(),
            },
        };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
