use futures::{future::BoxFuture, stream::SplitSink, FutureExt, SinkExt, StreamExt};

use locutus_core::locutus_runtime::TryFromTsStd;
use locutus_core::*;
use locutus_runtime::ContractKey;
use locutus_stdlib::api::{
    ClientError, ClientRequest, ContractRequest, ContractResponse, ErrorKind, HostResponse,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{
    mpsc::{self, error::TryRecvError, UnboundedReceiver},
    Mutex,
};
use warp::ws::Message;
use warp::ws::WebSocket;
use warp::{filters::BoxedFilter, reply, Filter, Rejection, Reply};

use crate::{errors, ClientConnection, DynError, HostCallbackResult};

const PARALLELISM: usize = 10; // TODO: get this from config, or whatever optimal way

/// Each request is unique so we don't keep track of a client session of any sort.
static REQUEST_ID: AtomicUsize = AtomicUsize::new(0);

/// A gateway to access and interact with contracts through an HTTP interface.
///
/// Contracts initially accessed through the gateway have to be compliant with the container contract
/// [specification](https://docs.freenet.org/glossary.html#container-contract) for Locutus.
///
/// Check the Locutus book for [more information](https://docs.freenet.org/dev-guide.html).
pub struct HttpGateway {
    server_request: mpsc::Receiver<ClientConnection>,
    response_channels: HashMap<ClientId, mpsc::UnboundedSender<HostCallbackResult>>,
}

impl HttpGateway {
    /// Returns the uninitialized warp filter to compose with other routing handling or websockets.
    pub fn as_filter() -> (Self, BoxedFilter<(impl Reply + 'static,)>) {
        let contract_web_path = std::env::temp_dir().join("locutus").join("webs");
        std::fs::create_dir_all(contract_web_path).unwrap();

        let (request_sender, server_request) = mpsc::channel(PARALLELISM);
        let gateway = Self {
            server_request,
            response_channels: HashMap::new(),
        };

        let get_home = warp::path::end().and_then(home);
        let base_web_contract = warp::path::path("contract").and(warp::path::path("web"));

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

        let web_home = base_web_contract
            .map(move || request_sender.clone())
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

        let filters = websocket_commands
            .or(get_home)
            .or(web_home)
            .or(web_subpages)
            .recover(errors::handle_error)
            .with(warp::trace::request());

        (gateway, filters.boxed())
    }

    pub fn next_client_id() -> ClientId {
        internal_next_client_id()
    }
}

fn internal_next_client_id() -> ClientId {
    ClientId::new(REQUEST_ID.fetch_add(1, Ordering::SeqCst))
}

async fn new_client_connection(
    request_sender: &mpsc::Sender<ClientConnection>,
) -> Result<(mpsc::UnboundedReceiver<HostCallbackResult>, ClientId), ClientError> {
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send(ClientConnection::NewConnection(response_sender))
        .await
        .map_err(|_| ErrorKind::NodeUnavailable)?;
    match response_recv.recv().await {
        Some(HostCallbackResult::NewId(client_id)) => Ok((response_recv, client_id)),
        None => Err(ErrorKind::NodeUnavailable.into()),
        other => unreachable!("received unexpected message: {other:?}"),
    }
}

async fn home() -> Result<impl Reply, Rejection> {
    Ok(reply::reply())
}

async fn websocket_interface(
    request_sender: mpsc::Sender<ClientConnection>,
    ws: WebSocket,
) -> Result<(), DynError> {
    let (mut response_rx, client_id) = new_client_connection(&request_sender).await?;
    let (mut tx, mut rx) = ws.split();
    let listeners: Arc<Mutex<Vec<(_, UnboundedReceiver<HostResult>)>>> =
        Arc::new(Mutex::new(Vec::new()));
    loop {
        let active_listeners = listeners.clone();
        let listeners_task = async move {
            loop {
                let active_listeners = &mut *active_listeners.lock().await;
                for _ in 0..active_listeners.len() {
                    let (key, mut listener) = active_listeners.swap_remove(0);
                    match listener.try_recv() {
                        Ok(r) => {
                            active_listeners.push((key, listener));
                            return Ok(r);
                        }
                        Err(TryRecvError::Empty) => {
                            active_listeners.push((key, listener));
                        }
                        Err(err @ TryRecvError::Disconnected) => {
                            return Err(Box::new(err) as DynError)
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        let client_req_task = async {
            let next_msg = match rx
                .next()
                .await
                .ok_or_else::<ClientError, _>(|| ErrorKind::Disconnect.into())
            {
                Err(err) => {
                    tracing::debug!(err = %err, "client channel error");
                    return Err(Some(err.into()));
                }
                Ok(v) => v,
            };
            process_client_request(client_id, next_msg, &request_sender).await
        };

        let active_listeners = listeners.clone();
        tokio::select! { biased;
            msg = async { process_host_response(response_rx.recv().await, client_id, &mut tx).await } => {
                if let Some(NewSubscription { key, callback }) = msg? {
                    let active_listeners = &mut *active_listeners.lock().await;
                    active_listeners.push((key, callback));
                }
            }
            process_client_request = client_req_task => {
                match process_client_request {
                    Ok(Some(response)) => {
                        tx.send(response).await?;
                    }
                    Ok(None) => continue,
                    Err(None) => return Ok(()),
                    Err(Some(err)) => return Err(err),
                }
            }
            response = listeners_task => {
                let response = response?;
                match &response {
                    Ok(res) => tracing::debug!(response = %res, cli_id = %client_id, "sending notification"),
                    Err(err) => tracing::debug!(response = %err, cli_id = %client_id, "sending notification error"),
                }
                let msg = rmp_serde::to_vec(&response)?;
                tx.send(Message::binary(msg)).await?;
            }
        }
    }
}

struct NewSubscription {
    key: ContractKey,
    callback: UnboundedReceiver<HostResult>,
}

async fn process_client_request(
    client_id: ClientId,
    msg: Result<Message, warp::Error>,
    request_sender: &mpsc::Sender<ClientConnection>,
) -> Result<Option<Message>, Option<DynError>> {
    let msg = match msg {
        Ok(m) if m.is_binary() || m.is_text() => m.into_bytes(),
        Ok(m) if m.is_close() => return Err(None),
        Ok(m) if m.is_ping() => {
            return Ok(Some(Message::pong(vec![0, 3, 2])));
        }
        Ok(m) => {
            tracing::debug!(msg = ?m, "received random message");
            return Ok(None);
        }
        Err(err) => return Err(Some(err.into())),
    };
    let req: ClientRequest = {
        match ContractRequest::try_decode(&msg) {
            Ok(r) => r.into(),
            Err(e) => {
                let result_error = rmp_serde::to_vec(&Err::<HostResponse, ClientError>(
                    ErrorKind::DeserializationError {
                        cause: format!("{e}"),
                    }
                    .into(),
                ))
                .map_err(|err| Some(err.into()))?;
                return Ok(Some(Message::binary(result_error)));
            }
        }
    };
    tracing::debug!(req = %req, "received client request");
    request_sender
        .send(ClientConnection::Request { client_id, req })
        .await
        .map_err(|err| Some(err.into()))?;
    Ok(None)
}

async fn process_host_response(
    msg: Option<HostCallbackResult>,
    client_id: ClientId,
    tx: &mut SplitSink<WebSocket, Message>,
) -> Result<Option<NewSubscription>, DynError> {
    match msg {
        Some(HostCallbackResult::Result { id, result }) => {
            debug_assert_eq!(id, client_id);
            let result = match result {
                Ok(res) => {
                    tracing::debug!(response = %res, cli_id = %id, "sending response");
                    match res {
                        HostResponse::ContractResponse(ContractResponse::GetResponse {
                            contract,
                            state,
                        }) => Ok(ContractResponse::GetResponse { contract, state }.into()),
                        other => Ok(other),
                    }
                }
                Err(err) => {
                    tracing::debug!(response = %err, cli_id = %id, "sending response error");
                    Err(err)
                }
            };
            let res = rmp_serde::to_vec(&result)?;
            tx.send(Message::binary(res)).await?;
            Ok(None)
        }
        Some(HostCallbackResult::SubscriptionChannel { key, id, callback }) => {
            debug_assert_eq!(id, client_id);
            Ok(Some(NewSubscription { key, callback }))
        }
        _ => {
            let result_error = rmp_serde::to_vec(&Err::<HostResponse, ClientError>(
                ErrorKind::NodeUnavailable.into(),
            ))?;
            tx.send(Message::binary(result_error)).await?;
            tx.send(Message::close()).await?;
            log::warn!("node shut down while handling responses for {client_id}");
            Err(format!("node shut down while handling responses for {client_id}").into())
        }
    }
}

impl HttpGateway {
    async fn internal_proxy_recv(
        &mut self,
        msg: ClientConnection,
    ) -> Result<Option<OpenRequest>, ClientError> {
        match msg {
            ClientConnection::NewConnection(new_client_ch) => {
                // is a new client, assign an id and open a channel to communicate responses from the node
                let cli_id = internal_next_client_id();
                new_client_ch
                    .send(HostCallbackResult::NewId(cli_id))
                    .map_err(|_e| ErrorKind::NodeUnavailable)?;
                self.response_channels.insert(cli_id, new_client_ch);
                Ok(None)
            }
            ClientConnection::Request {
                client_id,
                req: ClientRequest::ContractOp(ContractRequest::Subscribe { key }),
            } => {
                // intercept subscription messages because they require a callback subscription channel
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                if let Some(ch) = self.response_channels.get(&client_id) {
                    ch.send(HostCallbackResult::SubscriptionChannel {
                        key: key.clone(),
                        id: client_id,
                        callback: rx,
                    })
                    .map_err(|_| ErrorKind::ChannelClosed)?;
                    Ok(Some(
                        OpenRequest::new(client_id, ContractRequest::Subscribe { key }.into())
                            .with_notification(tx),
                    ))
                } else {
                    log::warn!("client: {client_id} not found");
                    Err(ErrorKind::UnknownClient(client_id.into()).into())
                }
            }
            ClientConnection::Request { client_id, req } => {
                // just forward the request to the node
                Ok(Some(OpenRequest::new(client_id, req)))
            }
        }
    }
}

impl ClientEventsProxy for HttpGateway {
    fn recv(&mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
        async move {
            loop {
                let msg = self.server_request.recv().await;
                if let Some(msg) = msg {
                    if let Some(reply) = self.internal_proxy_recv(msg).await? {
                        break Ok(reply.into_owned());
                    }
                } else {
                    todo!()
                }
            }
        }
        .boxed()
    }

    fn send(
        &mut self,
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    ) -> BoxFuture<Result<(), ClientError>> {
        async move {
            if let Some(ch) = self.response_channels.remove(&id) {
                let should_rm = result
                    .as_ref()
                    .map_err(|err| matches!(err.kind(), ErrorKind::Disconnect))
                    .err()
                    .unwrap_or(false);
                if ch.send(HostCallbackResult::Result { id, result }).is_ok() && !should_rm {
                    // still alive connection, keep it
                    self.response_channels.insert(id, ch);
                }
            } else {
                log::warn!("client: {id} not found");
            }
            Ok(())
        }
        .boxed()
    }
}
