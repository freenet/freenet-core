use byteorder::{BigEndian, ReadBytesExt};
use locutus_node::WrappedState;
use locutus_runtime::{ContractKey, ContractStore, Parameters, StateStore, StateSummary};
use std::fs::File;
use std::path::PathBuf;

use locutus_dev::LocalNode;
use locutus_node::*;
use std::{
    collections::HashMap,
    future::Future,
    io::{Cursor, Read},
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
};
use tar::Archive;
use tokio::sync::{
    mpsc,
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use warp::{
    filters::BoxedFilter,
    hyper::StatusCode,
    reject::{self, Reject},
    reply, Filter, Rejection, Reply,
};
use xz2::bufread::XzDecoder;

use crate::DynError;

type HostResult = Result<HostResponse, ClientError>;

const PARALLELISM: usize = 10; // TODO: get this from config, or whatever optimal way

pub struct HttpGateway {
    server_request: Receiver<(ClientRequest, oneshot::Sender<HostResult>)>,
    pending_responses: HashMap<ClientId, oneshot::Sender<HostResult>>,
}

impl HttpGateway {
    /// Returns the uninitialized warp filter to compose with other routing handling or websockets.
    pub fn as_filter(
        contract_store: ContractStore,
        state_store: StateStore<SqlitePool>,
        local_node: LocalNode,
        peer_key: PeerKey,
    ) -> (Self, BoxedFilter<(impl Reply + 'static,)>) {
        let (request_sender, server_request) = channel(PARALLELISM);

        let cs = contract_store.clone();
        let cs2 = contract_store.clone();
        let ss = state_store.clone();
        let rs = request_sender.clone();
        let rs2 = request_sender.clone();

        let get_contract_web = warp::path::path("contract")
            .map(move || (rs.clone(), cs.clone()))
            .and(warp::path::param())
            .and(warp::path::end())
            .and_then(|(rs, cs), key: String| async move { handle_contract(key, rs, cs).await });

        let get_contract_state = warp::path::path("contract")
            .map(move || (peer_key, local_node.clone()))
            .and(warp::path::param())
            .and(warp::path::path("state"))
            .and_then(|(pk, ln), key: String| async move { handle_get_state(key, pk, ln).await });

        let put_contract_state = warp::path::path("contract")
            .map(move || (rs2.clone(), cs2.clone(), ss.clone()))
            .and(warp::path::param())
            .and(warp::path::path("state_put"))
            .and_then(|(rs, cs, ss), key: String| async move {
                handle_put_state(key, rs, cs, ss).await
            });

        let get_home = warp::path::end().and_then(home);

        let filters = get_contract_web
            .or(get_contract_state)
            .or(put_contract_state)
            .or(get_home)
            .recover(errors::handle_error)
            .with(warp::trace::request());

        (
            Self {
                server_request,
                pending_responses: HashMap::new(),
            },
            filters.boxed(),
        )
    }
}

/// Each request is unique so we don't keep track of a client session of any sort.
static ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
enum ExtractError {
    Io(std::io::Error),
    StripPrefixError(std::path::StripPrefixError),
}

impl From<std::io::Error> for ExtractError {
    fn from(error: std::io::Error) -> Self {
        ExtractError::Io(error)
    }
}

impl From<std::path::StripPrefixError> for ExtractError {
    fn from(error: std::path::StripPrefixError) -> Self {
        ExtractError::StripPrefixError(error)
    }
}

async fn handle_get_state(
    key: String,
    peer_key: PeerKey,
    mut local_node: LocalNode,
) -> Result<impl Reply, Rejection> {
    let (sender, mut receiver) = mpsc::unbounded_channel::<HostResponse>();
    let key = ContractKey::from_spec(key).unwrap();
    let state_summary = StateSummary::from(vec![]);
    local_node
        .register_contract_notifier(key, peer_key, sender, state_summary)
        .unwrap();
    Ok(reply::html(tokio::select! {
        msg = receiver.recv() => {
            match msg {
                Some(HostResponse::UpdateResponse { key, summary }) => {
                    summary.to_vec()
                }
                _ => vec![]
            }
        }
    }))
}

async fn handle_put_state(
    key: String,
    request_sender: Sender<(ClientRequest, oneshot::Sender<HostResult>)>,
    contract_store: ContractStore,
    state_store: StateStore<SqlitePool>,
) -> Result<impl Reply, Rejection> {
    let key = ContractKey::from_spec(key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    let params = Parameters::from(vec![]);
    let contract = contract_store.fetch_contract(&key, &params).unwrap();
    let state = state_store.get(&key).await.unwrap();
    let (tx, response) = oneshot::channel();
    request_sender
        .send((ClientRequest::Put { contract, state }, tx))
        .await
        .map_err(|_| reject::custom(errors::NodeError))?;

    Ok(reply::html("Ok"))
}

async fn handle_contract(
    key: String,
    request_sender: Sender<(ClientRequest, oneshot::Sender<HostResult>)>,
    mut contract_store: ContractStore,
) -> Result<impl Reply, Rejection> {
    let key = ContractKey::from_spec(key)
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
        Ok(r) => {
            match r {
                HostResponse::GetResponse { contract, state } => {
                    // TODO: here we should pass the batton to the websocket interface
                    match contract {
                        Some(c) => {
                            let contract_path =
                                contract_store.get_contract_path(c.key()).map_err(|_| {
                                    ErrorKind::RequestError(RequestError::Get {
                                        key,
                                        cause: "contract code hash key not specified".to_owned(),
                                    })
                                })?;
                            let web_body = get_web_body(state, contract_path).unwrap();
                            Ok(reply::html(web_body))
                        }
                        None => Ok(reply::html(hyper::Body::empty())),
                    }
                }
                _ => {
                    // TODO: here we should pass the batton to the websocket interface
                    Ok(reply::html(hyper::Body::empty()))
                }
            }
        }
        Err(err) => Err(err.kind().into()),
    }
}

fn get_web_path(state: WrappedState, path: PathBuf) -> Result<PathBuf, DynError> {
    // Decompose the state and extract the compressed web interface
    let mut state = Cursor::new(state.as_ref());
    let metadata_size = state.read_u64::<BigEndian>()?;
    let mut metadata = vec![0; metadata_size as usize];
    state.read_exact(&mut metadata)?;
    let web_size = state.read_u64::<BigEndian>()?;
    let mut web = vec![0; web_size as usize];
    state.read_exact(&mut web)?;

    // Decode tar.xz and build response body
    let decoder = XzDecoder::new(Cursor::new(&web));
    let mut files = Archive::new(decoder);
    let _ = files.unpack(path.clone());

    let web_path = path.join("web");

    Ok(web_path)
}

fn get_web_body(state: WrappedState, path: PathBuf) -> Result<hyper::Body, DynError> {
    // Decompose the state and extract the compressed web interface
    let mut state = Cursor::new(state.as_ref());
    let metadata_size = state.read_u64::<BigEndian>()?;
    let mut metadata = vec![0; metadata_size as usize];
    state.read_exact(&mut metadata)?;
    let web_size = state.read_u64::<BigEndian>()?;
    let mut web = vec![0; web_size as usize];
    state.read_exact(&mut web)?;

    // Decode tar.xz and unpack contract web
    let mut index = vec![];
    let decoder = XzDecoder::new(Cursor::new(&web));
    let mut files = Archive::new(decoder);
    files.unpack(path.clone())?;

    // Get and return web
    let web_path = path.join("web/index.html");
    let mut key_file = File::open(&web_path)
        .unwrap_or_else(|_| panic!("Failed to open key file: {}", &web_path.to_str().unwrap()));
    key_file.read_to_end(&mut index).unwrap();

    Ok(hyper::Body::from(index))
}

async fn get_state(
    contract_key: String,
    state_store: StateStore<SqlitePool>,
    ws: warp::ws::Ws,
) -> Result<impl Reply, Rejection> {
    let key = ContractKey::from_spec(contract_key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    let state = state_store.get(&key).await.unwrap().to_vec();

    Ok(reply::html(hyper::Body::from(state)))
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

#[cfg(test)]
pub(crate) mod test {
    use std::{fs::File, path::PathBuf};

    use super::*;

    fn _test_state() -> Result<WrappedState, std::io::Error> {
        const CRATE_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let path = PathBuf::from(CRATE_DIR).join("tests/encoded_state");
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        Ok(WrappedState::new(bytes))
    }

    // #[test]
    // fn test_get_ui_from_contract() -> Result<(), DynError> {
    //     let state = test_state()?;
    //     let body = get_web_body(state);
    //     assert!(body.is_ok());
    //     Ok(())
    // }
}
