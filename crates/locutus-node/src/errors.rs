use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use locutus_stdlib::client_api::ErrorKind;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum WebSocketApiError {
    /// Something went wrong when calling the user repo.
    InvalidParam {
        error_cause: String,
    },
    NodeError {
        error_cause: String,
    },
    HttpError {
        code: StatusCode,
        cause: Option<String>,
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
            WebSocketApiError::HttpError { code, .. } => *code,
            WebSocketApiError::AxumError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn error_message(&self) -> String {
        match self {
            WebSocketApiError::InvalidParam { error_cause } => {
                format!("Invalid request params: {}", error_cause)
            }
            WebSocketApiError::NodeError { error_cause } => format!("Node error: {}", error_cause),
            WebSocketApiError::HttpError { code, cause } => {
                let error_message = match *code {
                    StatusCode::BAD_REQUEST => "Bad Request",
                    StatusCode::BAD_GATEWAY => "Bad Gateway",
                    StatusCode::NOT_FOUND => "Not Found",
                    _ => "Internal Server Error",
                };
                if let Some(cause) = cause {
                    format!("HTTP error: {} ({cause})", error_message)
                } else {
                    format!("HTTP error: {}", error_message)
                }
            }
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
            WebSocketApiError::HttpError { code, cause } => (
                code,
                cause.unwrap_or_else(|| "INTERNAL SERVER ERROR".to_owned()),
            ),
            WebSocketApiError::AxumError { error } => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}"))
            }
        };

        let body = Html(error_message);

        (status, body).into_response()
    }
}
