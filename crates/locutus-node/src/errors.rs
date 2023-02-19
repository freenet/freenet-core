use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use locutus_stdlib::client_api::ErrorKind;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum Error {
    /// Something went wrong when calling the user repo.
    InvalidParam(String),
    NodeError,
    HttpError(StatusCode),
    AxumError(ErrorKind),
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            Error::InvalidParam(err) => (StatusCode::BAD_REQUEST, err),
            Error::NodeError => (StatusCode::BAD_GATEWAY, "Node unavailable".to_owned()),
            Error::HttpError(code) => (code, "INTERNAL SERVER ERROR".to_owned()),
            Error::AxumError(error_kind) => {
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{error_kind}"))
            }
        };

        let body = Html(error_message);

        (status, body).into_response()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidParam(err) => write!(f, "Invalid request params: {err}"),
            Error::NodeError => write!(f, "Node error: {}", StatusCode::BAD_GATEWAY.as_str()),
            Error::HttpError(code) => write!(f, "HttpError with status code {}", code.as_str()),
            Error::AxumError(error_kind) => {
                write!(f, "Axum error: {}", error_kind)
            }
        }
    }
}
