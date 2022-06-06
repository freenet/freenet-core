use byteorder::{BigEndian, ReadBytesExt};
use locutus_node::{either::Either, WrappedState};
use locutus_runtime::{ContractKey, StateDelta};
use std::fs::File;
use std::path::{Path, PathBuf};

use futures::future::ready;
use futures::{SinkExt, StreamExt};
use locutus_node::*;
use std::{
    collections::HashMap,
    future::Future,
    io::{Cursor, Read},
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
};
use tar::Archive;
use tokio::sync::mpsc;
use warp::ws::{Message, WebSocket, Ws};
use warp::{
    filters::BoxedFilter,
    hyper::StatusCode,
    reject::{self, Reject},
    reply, Filter, Rejection, Reply,
};
use xz2::bufread::XzDecoder;

use crate::DynError;

use self::errors::NodeError;

type HostResult = (ClientId, Result<HostResponse, ClientError>);
type ClientHandlingMessage = (
    ClientRequest,
    Either<mpsc::UnboundedSender<HostResult>, ClientId>,
);

const PARALLELISM: usize = 10; // TODO: get this from config, or whatever optimal way

pub struct HttpGateway {
    server_request: mpsc::Receiver<ClientHandlingMessage>,
    response_channels: HashMap<ClientId, mpsc::UnboundedSender<HostResult>>,
}

impl HttpGateway {
    /// Returns the uninitialized warp filter to compose with other routing handling or websockets.
    pub fn as_filter() -> (Self, BoxedFilter<(impl Reply + 'static,)>) {
        let (request_sender, server_request) = mpsc::channel(PARALLELISM);
        let gateway = Self {
            server_request,
            response_channels: HashMap::new(),
        };

        let rs = request_sender.clone();
        let get_contract_web = warp::path::path("contract")
            .map(move || rs.clone())
            .and(warp::path::param())
            .and(warp::path::end())
            .and_then(|rs, key: String| async move { handle_contract(key, rs).await });

        let get_home = warp::path::end().and_then(home);

        let get_test_web = warp::path::path("contract")
            .and(warp::path::param())
            .and(warp::path::path("web"))
            .map(|key: String| {
                let mut index = vec![];
                let mut file = File::open(
                    Path::new("/Users/hectorsantos/workspace/contribute/locutus/crates/http-gw/examples/web.html")).unwrap();
                file.read_to_end(&mut index).unwrap();
                reply::html(warp::hyper::Body::from(index))
            });

        let rs = request_sender.clone();
        let state_update_notifications = warp::path::path("contract")
            .map(move || rs.clone())
            .and(warp::path::param())
            .and(warp::path!("state" / "updates"))
            .and(warp::ws())
            .map(|rs, key: String, ws: warp::ws::Ws| {
                ws.on_upgrade(move |websocket: WebSocket| {
                    state_updates_notification(key, rs, websocket)
                })
            });

        let rs = request_sender.clone();
        let update_contract_state = warp::path::path("contract")
            .map(move || rs.clone())
            .and(warp::path::param())
            .and(warp::path!("state" / "update"))
            .and(warp::body::json())
            .and_then(|rs, key: String, put_value| async move {
                handle_update_state(key, put_value, rs).await
            });

        let get_contract_state = warp::path::path("contract")
            .map(move || request_sender.clone())
            .and(warp::path::param())
            .and(warp::path!("state" / "get"))
            .and_then(|rs, key: String| async move { handle_get_state(key, rs).await });

        let filters = get_contract_web
            .or(get_home)
            .or(get_test_web)
            .or(state_update_notifications)
            .or(update_contract_state)
            .or(get_contract_state)
            .recover(errors::handle_error)
            .with(warp::trace::request());

        (gateway, filters.boxed())
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

async fn state_updates_notification(
    key: String,
    request_sender: mpsc::Sender<ClientHandlingMessage>,
    ws: WebSocket,
) {
    let contract_key = ContractKey::from_spec(key).unwrap();
    let (updates, mut updates_recv) = mpsc::unbounded_channel();
    let (response_sender, _response_recv) = mpsc::unbounded_channel();

    let (mut ws_sender, mut _ws_receiver) = {
        let (tx, rx) = ws.split();

        let str_sender = tx.with(|msg: serde_json::Value| {
            let res: Result<Message, warp::Error> = Ok(Message::text(
                serde_json::to_string(&msg).expect("Converting message to JSON"),
            ));
            ready(res)
        });

        (str_sender, rx)
    };

    request_sender
        .send((
            ClientRequest::Subscribe {
                key: contract_key,
                updates,
            },
            Either::Left(response_sender),
        ))
        .await
        .map_err(|_| reject::custom(errors::NodeError))
        .unwrap();
    // todo: await for some sort of confirmation through "response_recv"
    while let Some(response) = updates_recv.recv().await {
        if let HostResponse::UpdateNotification { key, update } = response {
            // todo: we assume that this is json, but it could be anything
            //       in reality we must send this back as bytes and let the client handle it
            //       but in order to test things out we send a json
            assert_eq!(key, contract_key);
            let json_str: serde_json::Value = serde_json::from_slice(update.as_ref()).unwrap();
            let _ = ws_sender.send(json_str).await;
        } else {
            break;
        }
    }
    request_sender
        .send((
            ClientRequest::Disconnect { cause: None },
            Either::Right(ClientId::new(0)),
        ))
        .await
        .map_err(|_| NodeError)
        .unwrap();
}

async fn handle_get_state(
    key: String,
    request_sender: mpsc::Sender<ClientHandlingMessage>,
) -> Result<impl Reply, Rejection> {
    let key = ContractKey::from_spec(key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send((
            ClientRequest::Get {
                key,
                fetch_contract: false,
            },
            Either::Left(response_sender),
        ))
        .await
        .map_err(|_| reject::custom(errors::NodeError))?;
    let (id, response) = match response_recv.recv().await {
        Some((id, Ok(HostResponse::GetResponse { state, .. }))) => {
            // todo: we assume that this is json, but it could be anything
            //       in reality we must send this back as bytes and let the client handle it
            //       but in order to test things out we send a json
            let json_str: serde_json::Value = serde_json::from_slice(state.as_ref()).unwrap();
            (id, Ok(reply::json(&json_str)))
        }
        None => {
            return Err(NodeError.into());
        }
        _ => unreachable!(),
    };
    request_sender
        .send((ClientRequest::Disconnect { cause: None }, Either::Right(id)))
        .await
        .map_err(|_| NodeError)?;
    response
}

// todo: assuming json but could be anything really, they have to send as bytes
async fn handle_update_state(
    key: String,
    update_value: serde_json::Value,
    request_sender: mpsc::Sender<ClientHandlingMessage>,
) -> Result<impl Reply, Rejection> {
    let contract_key = ContractKey::from_spec(key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    let delta = serde_json::to_vec(&update_value).unwrap();
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send((
            ClientRequest::Update {
                key: contract_key,
                delta: StateDelta::from(delta),
            },
            Either::Left(response_sender),
        ))
        .await
        .map_err(|_| reject::custom(errors::NodeError))?;
    let (id, response) = match response_recv.recv().await {
        Some((id, Ok(HostResponse::UpdateResponse { key, .. }))) => {
            assert_eq!(key, contract_key);
            (id, Ok(reply::json(&serde_json::json!({"result": "ok"}))))
        }
        None => {
            return Err(NodeError.into());
        }
        _ => unreachable!(),
    };
    request_sender
        .send((ClientRequest::Disconnect { cause: None }, Either::Right(id)))
        .await
        .map_err(|_| NodeError)?;
    response
}

async fn handle_contract(
    key: String,
    request_sender: mpsc::Sender<ClientHandlingMessage>,
) -> Result<impl Reply, Rejection> {
    let key = ContractKey::from_spec(key)
        .map_err(|err| reject::custom(errors::InvalidParam(format!("{err}"))))?;
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send((
            ClientRequest::Get {
                key,
                fetch_contract: false,
            },
            Either::Left(response_sender),
        ))
        .await
        .map_err(|_| reject::custom(errors::NodeError))?;
    let (id, response) = match response_recv.recv().await {
        Some((id, Ok(HostResponse::GetResponse { path, state, .. }))) => match path {
            Some(path) => {
                let web_body = get_web_body(state, path).unwrap();
                (id, Ok(reply::html(web_body)))
            }
            None => {
                todo!("error indicating the contract is not present");
            }
        },
        Some((_id, Err(err))) => {
            log::error!("error getting contract `{key}`: {err}");
            return Err(err.kind().into());
        }
        None => {
            return Err(NodeError.into());
        }
        other => unreachable!("received unexpected node response: {other:?}"),
    };
    request_sender
        .send((ClientRequest::Disconnect { cause: None }, Either::Right(id)))
        .await
        .map_err(|_| NodeError)?;
    response
}

fn get_web_body(state: WrappedState, path: PathBuf) -> Result<warp::hyper::Body, DynError> {
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

    Ok(warp::hyper::Body::from(index))
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
            if let Some((req, ch_or_id)) = self.server_request.recv().await {
                match ch_or_id {
                    Either::Left(new_client_ch) => {
                        // is a new client, assign an id and open a channel to communicate responses from the node
                        tracing::debug!("received request: {req}");
                        let cli_id = ClientId::new(ID.fetch_add(1, Ordering::SeqCst));
                        self.response_channels.insert(cli_id, new_client_ch);
                        Ok((cli_id, req))
                    }
                    Either::Right(existing_client) => {
                        // just forward the request to the node
                        Ok((existing_client, req))
                    }
                }
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
            if let Some(ch) = self.response_channels.remove(&client) {
                if ch.send((client, response)).is_ok() {
                    // still alive connection, keep it
                    self.response_channels.insert(client, ch);
                }
            }
            Ok(())
        })
    }

    fn cloned(&self) -> BoxedClient {
        unimplemented!()
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
