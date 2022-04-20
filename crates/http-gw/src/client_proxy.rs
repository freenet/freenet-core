use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
};

use locutus_node::*;
use locutus_stdlib::prelude::ContractKey;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use warp::{
    filters::BoxedFilter,
    hyper::StatusCode,
    reject::{self, Reject},
    reply, Filter, Rejection, Reply,
};

type HostResult = Result<HostResponse, ClientError>;

const PARALLELISM: usize = 10; // TODO: get this from config, or whatever optimal way

pub struct HttpGateway {
    server_request: Receiver<(ClientRequest, oneshot::Sender<HostResult>)>,
    pending_responses: HashMap<ClientId, oneshot::Sender<HostResult>>,
}

impl HttpGateway {
    /// Returns the uninitialized warp filter to compose with other routing handling or websockets.
    pub fn as_filter() -> (Self, BoxedFilter<(impl Reply + 'static,)>) {
        let (request_sender, server_request) = channel(PARALLELISM);
        let filter = warp::path::path("contract")
            .map(move || request_sender.clone())
            .and(warp::path::param())
            .and(warp::path::end())
            .and_then(|rs, key: String| async move { handle_contract(key, rs).await })
            .or(warp::path::end().and_then(home))
            .recover(errors::handle_error)
            .with(warp::trace::request());
        (
            Self {
                server_request,
                pending_responses: HashMap::new(),
            },
            filter.boxed(),
        )
    }
}

/// Each request is unique so we don't keep track of a client session of any sort.
static ID: AtomicUsize = AtomicUsize::new(0);

async fn handle_contract(
    key: String,
    request_sender: Sender<(ClientRequest, oneshot::Sender<HostResult>)>,
) -> Result<impl Reply, Rejection> {
    let key = key.to_lowercase();
    let key = ContractKey::hex_decode(key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    let (tx, response) = oneshot::channel();
    request_sender
        .send((ClientRequest::Subscribe { key }, tx))
        .await
        .map_err(|_| reject::custom(errors::NodeError))?;
    let response = response
        .await
        .map_err(|_| reject::custom(errors::NodeError))?;
    match response {
        Ok(_r) => {
            // TODO: here we should pass the batton to the websocket interface
            Ok(reply::reply())
        }
        Err(err) => Err(err.kind().into()),
    }
}

async fn home() -> Result<impl Reply, Rejection> {
    Ok(reply::reply())
}

#[allow(clippy::needless_lifetimes)]
impl ClientEventsProxy for HttpGateway {
    fn recv<'a>(
        &'a mut self,
    ) -> Pin<
        Box<dyn Future<Output = Result<(ClientId, ClientRequest), ClientError>> + Send + Sync + '_>,
    > {
        Box::pin(async move {
            if let Some((req, response_ch)) = self.server_request.recv().await {
                tracing::debug!("received request: {req}");
                let cli_id = ClientId::new(ID.fetch_add(1, Ordering::SeqCst));
                self.pending_responses.insert(cli_id, response_ch);
                Ok((cli_id, req))
            } else {
                todo!()
            }
        })
    }

    fn send<'a>(
        &'a mut self,
        client: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ClientError>> + Send + Sync + '_>> {
        Box::pin(async move {
            // fixme: deal with unwraps()
            let ch = self.pending_responses.remove(&client).unwrap();
            ch.send(response).unwrap();
            Ok(())
        })
    }

    fn cloned(&self) -> BoxedClient {
        todo!()
    }
}

mod errors {
    use super::*;

    pub(super) async fn handle_error(
        err: Rejection,
    ) -> Result<impl Reply, std::convert::Infallible> {
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
}
