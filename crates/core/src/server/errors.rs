use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use freenet_stdlib::client_api::ErrorKind;
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
}

impl WebSocketApiError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            WebSocketApiError::InvalidParam { .. } => StatusCode::BAD_REQUEST,
            WebSocketApiError::NodeError { .. } => StatusCode::BAD_GATEWAY,
            WebSocketApiError::AxumError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn error_message(&self) -> String {
        match self {
            WebSocketApiError::InvalidParam { error_cause } => {
                format!("Invalid request params: {}", error_cause)
            }
            WebSocketApiError::NodeError { error_cause } => format!("Node error: {}", error_cause),
            WebSocketApiError::AxumError { error } => format!("Axum error: {}", error),
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
        let (status, error_message) = match self {
            WebSocketApiError::InvalidParam { error_cause: cause } => {
                (StatusCode::BAD_REQUEST, cause)
            }
            WebSocketApiError::NodeError { error_cause: cause } => (StatusCode::BAD_GATEWAY, cause),
            WebSocketApiError::AxumError { error } => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}"))
            }
        };

        let body = Html(error_message);

        (status, body).into_response()
    }
}
