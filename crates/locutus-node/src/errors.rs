use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use locutus_stdlib::client_api::ErrorKind;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum Error {
    /// Something went wrong when calling the user repo.
    InvalidParam {
        error_cause: String,
    },
    Node {
        error_cause: String,
    },
    Http {
        code: StatusCode,
    },
    Axum {
        error: ErrorKind,
    },
}

impl Error {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidParam { .. } => StatusCode::BAD_REQUEST,
            Error::Node { .. } => StatusCode::BAD_GATEWAY,
            Error::Http { code } => *code,
            Error::Axum { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn error_message(&self) -> String {
        match self {
            Error::InvalidParam { error_cause } => {
                format!("Invalid request params: {}", error_cause)
            }
            Error::Node { error_cause } => format!("Node error: {}", error_cause),
            Error::Http { code } => {
                let error_message = match *code {
                    StatusCode::BAD_REQUEST => "Bad Request",
                    StatusCode::BAD_GATEWAY => "Bad Gateway",
                    StatusCode::NOT_FOUND => "Not Found",
                    _ => "Internal Server Error",
                };
                format!("HTTP error: {}", error_message)
            }
            Error::Axum { error } => format!("Axum error: {}", error),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error_message())
    }
}

impl From<Error> for Response {
    fn from(error: Error) -> Self {
        let body = Html(error.error_message());
        (error.status_code(), body).into_response()
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            Error::InvalidParam { error_cause: cause } => (StatusCode::BAD_REQUEST, cause),
            Error::Node { error_cause: cause } => (StatusCode::BAD_GATEWAY, cause),
            Error::Http { code } => (code, "INTERNAL SERVER ERROR".to_owned()),
            Error::Axum { error } => (StatusCode::INTERNAL_SERVER_ERROR, format!("{error}")),
        };

        let body = Html(error_message);

        (status, body).into_response()
    }
}
