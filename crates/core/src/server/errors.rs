use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use freenet_stdlib::client_api::ErrorKind;
use freenet_stdlib::prelude::ContractInstanceId;
use std::fmt::{Display, Formatter};

/// Marker string used to identify EmptyRing errors without needing to modify freenet-stdlib.
/// If this string changes in ring/mod.rs, update it here too.
const EMPTY_RING_ERROR: &str = "No ring connections found";
const PEER_NOT_JOINED_ERROR: &str = "PEER_NOT_JOINED";

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
            let error_str = format!("{error}");
            if error_str.contains(EMPTY_RING_ERROR) || error_str.contains(PEER_NOT_JOINED_ERROR) {
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

/// Returns a user-friendly HTML page shown while the peer is connecting to the network.
/// Includes actionable diagnostics when connection failures have been recorded.
fn connecting_page() -> String {
    use crate::node::network_status;

    let diagnostics_html = match network_status::get_snapshot() {
        Some(snap) if !snap.failures.is_empty() => {
            let mut items = String::new();
            for f in &snap.failures {
                items.push_str(&format!(
                    "<li><code>{}</code>: {}</li>",
                    f.address, f.reason_html
                ));
            }
            format!(
                r#"<div class="diagnostics">
            <h2>Connection Issues</h2>
            <ul>{items}</ul>
            <p class="attempts">Attempted {attempts} connection(s) over {elapsed}s. Retrying...</p>
        </div>"#,
                attempts = snap.connection_attempts,
                elapsed = snap.elapsed_secs,
            )
        }
        Some(snap) if snap.connection_attempts > 0 => {
            format!(
                r#"<p class="attempts">Attempted {} connection(s) over {}s. Retrying...</p>"#,
                snap.connection_attempts, snap.elapsed_secs,
            )
        }
        _ => String::new(),
    };

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="3">
    <title>Connecting to Freenet</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            margin: 0;
            background: #f5f5f5;
        }}
        .container {{
            text-align: center;
            padding: 2rem;
            max-width: 600px;
        }}
        .logo {{
            width: 80px;
            height: 80px;
            margin-bottom: 1.5rem;
        }}
        h1 {{
            color: #333;
            font-size: 1.5rem;
            margin-bottom: 0.5rem;
        }}
        p {{
            color: #666;
            margin-bottom: 1rem;
        }}
        .spinner {{
            width: 24px;
            height: 24px;
            border: 3px solid #e0e0e0;
            border-top-color: #2196F3;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin: 0 auto;
        }}
        @keyframes spin {{
            to {{ transform: rotate(360deg); }}
        }}
        .diagnostics {{
            text-align: left;
            background: #fff3cd;
            border: 1px solid #ffc107;
            border-radius: 8px;
            padding: 1rem 1.5rem;
            margin-top: 1.5rem;
        }}
        .diagnostics h2 {{
            color: #856404;
            font-size: 1.1rem;
            margin: 0 0 0.5rem 0;
        }}
        .diagnostics ul {{
            padding-left: 1.2rem;
            margin: 0.5rem 0;
        }}
        .diagnostics li {{
            color: #555;
            margin-bottom: 0.5rem;
            font-size: 0.9rem;
        }}
        .attempts {{
            color: #888;
            font-size: 0.85rem;
            margin-top: 0.5rem;
        }}
        code {{
            background: #f0f0f0;
            padding: 0.1rem 0.3rem;
            border-radius: 3px;
            font-size: 0.85em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <img src="https://freenet.org/freenet_logo.svg" alt="Freenet" class="logo">
        <h1>Connecting to Freenet...</h1>
        <p>This peer is establishing network connections.<br>This page will refresh automatically.</p>
        <div class="spinner"></div>
        {diagnostics_html}
    </div>
</body>
</html>"#
    )
}
