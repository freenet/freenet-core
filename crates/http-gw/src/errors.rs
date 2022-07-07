use warp::{hyper::StatusCode, reject::Reject, reply, Rejection, Reply};

use super::*;

pub(super) async fn handle_error(err: Rejection) -> Result<impl Reply, std::convert::Infallible> {
    if let Some(e) = err.find::<errors::InvalidParam>() {
        return Ok(reply::with_status(e.0.to_owned(), StatusCode::BAD_REQUEST));
    }
    if err.find::<errors::NodeError>().is_some() {
        return Ok(reply::with_status(
            "Node unavailable".to_owned(),
            StatusCode::BAD_GATEWAY,
        ));
    }
    Ok(reply::with_status(
        "INTERNAL SERVER ERROR".to_owned(),
        StatusCode::INTERNAL_SERVER_ERROR,
    ))
}

#[derive(Debug)]
pub(super) struct InvalidParam(pub String);

impl Reject for InvalidParam {}

#[derive(Debug)]
pub(super) struct NodeError;

impl Reject for NodeError {}

#[derive(Debug)]
pub(super) struct HttpError(pub warp::http::StatusCode);

impl Reject for HttpError {}
