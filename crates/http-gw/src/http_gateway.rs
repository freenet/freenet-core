use futures::{SinkExt, StreamExt};
use locutus_node::either::Either;

use locutus_node::*;
use locutus_runtime::ContractKey;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::sync::mpsc;
use warp::ws::WebSocket;
use warp::{filters::BoxedFilter, reply, Filter, Rejection, Reply};
use warp::{hyper::body::Bytes, ws::Message};

use crate::{
    errors,
    state_handling::{get_state, state_changes_notification, update_state},
    ClientHandlingMessage, HostResult,
};

const PARALLELISM: usize = 10; // TODO: get this from config, or whatever optimal way

/// Each request is unique so we don't keep track of a client session of any sort.
static REQUEST_ID: AtomicUsize = AtomicUsize::new(0);

pub struct HttpGateway {
    server_request: mpsc::Receiver<ClientHandlingMessage>,
    response_channels: HashMap<ClientId, mpsc::UnboundedSender<HostResult>>,
}

impl HttpGateway {
    /// Returns the uninitialized warp filter to compose with other routing handling or websockets.
    pub fn as_filter() -> (Self, BoxedFilter<(impl Reply + 'static,)>) {
        let contract_web_path = std::env::temp_dir().join("locutus").join("webs");
        std::fs::create_dir_all(&contract_web_path).unwrap();

        let (request_sender, server_request) = mpsc::channel(PARALLELISM);
        let gateway = Self {
            server_request,
            response_channels: HashMap::new(),
        };

        let get_home = warp::path::end().and_then(home);
        let base_web_contract = warp::path!("contract" / "web");
        let data_contracts = warp::path!("contract" / "dependency");

        let rs = request_sender.clone();
        let websocket_commands = warp::path!("contract" / "command")
            .map(move || rs.clone())
            .and(warp::path::end())
            .and(warp::ws())
            .map(|rs, ws: warp::ws::Ws| {
                ws.on_upgrade(|ws: WebSocket| async {
                    if let Err(e) = websocket_interface(rs, ws).await {
                        log::error!("{e}");
                    }
                })
            });

        let rs = request_sender.clone();
        let web_home = base_web_contract
            .map(move || rs.clone())
            .and(warp::path::param())
            .and(warp::path::end())
            .and_then(|rs, key: String| async move {
                crate::web_handling::contract_home(key, rs).await
            });

        let web_subpages = base_web_contract
            .and(warp::path::param())
            .and(warp::filters::path::full())
            .and_then(|key: String, path| async move {
                crate::web_handling::variable_content(key, path).await
            });

        let rs = request_sender.clone();
        let get_state = data_contracts
            .map(move || rs.clone())
            .and(warp::path::param())
            .and(warp::path("get"))
            .and_then(|rs, key: String| async move {
                let (mut rx, id) = new_client_connection(&rs).await.unwrap();
                get_state(id, key, &rs, &mut rx).await
            });

        let rs = request_sender.clone();
        let state_changes = data_contracts
            .map(move || rs.clone())
            .and(warp::path::param())
            .and(warp::path("changes"))
            .and(warp::path::end())
            .and(warp::ws())
            .map(|rs, key: String, ws: warp::ws::Ws| {
                ws.on_upgrade(move |websocket: WebSocket| async move {
                    let (mut rx, id) = new_client_connection(&rs).await.unwrap();
                    state_changes_notification(id, key, websocket, &rs, &mut rx).await
                })
            });

        let state_update = data_contracts
            .map(move || request_sender.clone())
            .and(warp::path::param())
            .and(warp::path("update"))
            .and(warp::path::end())
            .and(warp::post())
            .and(warp::body::bytes())
            .and_then(move |rs, key: String, update_val: Bytes| async move {
                let (mut recv, id) = new_client_connection(&rs).await.unwrap();
                update_state(id, key, update_val.to_vec().into(), &rs, &mut recv).await
            });

        let filters = websocket_commands
            .or(get_home)
            .or(web_home)
            .or(web_subpages)
            .or(get_state)
            .or(state_changes)
            .or(state_update)
            .recover(errors::handle_error)
            .with(warp::trace::request());

        (gateway, filters.boxed())
    }

    pub fn next_client_id() -> ClientId {
        ClientId::new(REQUEST_ID.fetch_add(1, Ordering::SeqCst))
    }
}

async fn new_client_connection(
    request_sender: &mpsc::Sender<ClientHandlingMessage>,
) -> Result<(mpsc::UnboundedReceiver<HostResult>, ClientId), ClientError> {
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send(Either::Left(response_sender))
        .await
        .map_err(|_| ErrorKind::NodeUnavailable)?;
    match response_recv.recv().await {
        Some(HostResult::NewId(client_id)) => Ok((response_recv, client_id)),
        None => Err(ErrorKind::NodeUnavailable.into()),
        other => unreachable!("received unexpected message: {other:?}"),
    }
}

async fn home() -> Result<impl Reply, Rejection> {
    Ok(reply::reply())
}

async fn websocket_interface(
    request_sender: mpsc::Sender<ClientHandlingMessage>,
    ws: WebSocket,
) -> Result<(), Box<dyn std::error::Error>> {
    let (mut response_recv, client_id) = new_client_connection(&request_sender).await?;
    let (mut tx, mut rx) = ws.split();
    while let Some(msg) = rx.next().await {
        let msg = match msg {
            Ok(m) if m.is_binary() || m.is_text() => m.into_bytes(),
            Ok(m) if m.is_close() => break,
            Ok(m) if m.is_ping() => {
                if let Err(err) = tx.send(Message::pong(vec![0, 3, 2])).await {
                    log::debug!("{err}");
                }
                continue;
            }
            Ok(_) => continue,
            Err(_err) => todo!(),
        };
        let msg: ClientRequest = {
            match rmp_serde::from_read(std::io::Cursor::new(msg)) {
                Ok(r) => r,
                Err(e) => {
                    let result_error = rmp_serde::to_vec(&Err::<HostResponse, ClientError>(
                        ErrorKind::DeserializationError {
                            cause: format!("{e}"),
                        }
                        .into(),
                    ))?;
                    tx.send(Message::binary(result_error)).await?;
                    continue;
                }
            }
        };
        request_sender.send(Either::Right((client_id, msg))).await?;
        if let Some(HostResult::Result { id, result }) = response_recv.recv().await {
            debug_assert_eq!(id, client_id);
            let res = rmp_serde::to_vec(&result)?;
            tx.send(Message::binary(res)).await?;
        } else {
            let result_error = rmp_serde::to_vec(&Err::<HostResponse, ClientError>(
                ErrorKind::NodeUnavailable.into(),
            ))?;
            tx.send(Message::binary(result_error)).await?;
            tx.send(Message::close()).await?;
            log::warn!("node shut down while handling responses for {client_id}");
            break;
        }
    }
    Ok(())
}

#[allow(clippy::needless_lifetimes)]
impl ClientEventsProxy for HttpGateway {
    fn recv<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<OpenRequest, ClientError>> + Send + Sync + '_>> {
        Box::pin(async move {
            loop {
                if let Some(msg) = self.server_request.recv().await {
                    match msg {
                        Either::Left(new_client_ch) => {
                            // is a new client, assign an id and open a channel to communicate responses from the node
                            let cli_id = Self::next_client_id();
                            new_client_ch
                                .send(HostResult::NewId(cli_id))
                                .map_err(|_e| ErrorKind::NodeUnavailable)?;
                            self.response_channels.insert(cli_id, new_client_ch);
                        }
                        Either::Right((existing_client, req)) => {
                            // just forward the request to the node
                            break Ok(OpenRequest::new(existing_client, req));
                        }
                    }
                } else {
                    todo!()
                }
            }
        })
    }

    fn send<'a>(
        &'a mut self,
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ClientError>> + Send + Sync + '_>> {
        Box::pin(async move {
            if let Some(ch) = self.response_channels.remove(&id) {
                let should_rm = result
                    .as_ref()
                    .map_err(|err| matches!(err.kind(), ErrorKind::Disconnect))
                    .err()
                    .unwrap_or(false);
                if ch.send(HostResult::Result { id, result }).is_ok() && !should_rm {
                    // still alive connection, keep it
                    self.response_channels.insert(id, ch);
                }
            } else {
                log::warn!("client: {id} not found");
            }
            Ok(())
        })
    }
}
