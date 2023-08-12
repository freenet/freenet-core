use futures::{future::BoxFuture, stream::SplitSink, FutureExt, SinkExt, StreamExt};

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Path, WebSocketUpgrade};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Router};
use locutus_core::*;
use locutus_runtime::ContractKey;
use locutus_stdlib::client_api::{
    ClientError, ClientRequest, ContractRequest, ContractResponse, DelegateRequest, ErrorKind,
    HostResponse, TryFromFbs, TryFromTsStd,
};
use locutus_stdlib::client_request_generated::client_request::{
    root_as_client_request, ClientRequest as FbsClientRequest,
    DelegateRequest as FbsDelegateRequest, *,
};

use std::collections::VecDeque;
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

use crate::errors::WebSocketApiError;
use crate::{web_handling, ClientConnection, DynError, HostCallbackResult};

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
    /// Returns the uninitialized axum router to compose with other routing handling or websockets.
    pub fn as_router() -> (Self, Router) {
        let contract_web_path = std::env::temp_dir().join("locutus").join("webs");
        std::fs::create_dir_all(contract_web_path).unwrap();

        let (request_sender, server_request) = mpsc::channel(PARALLELISM);
        let gateway = Self {
            server_request,
            response_channels: HashMap::new(),
        };

        let router = Router::new()
            .route("/", get(home))
            .route("/contract/command/", get(websocket_commands))
            .route("/contract/web/:key/", get(web_home))
            .route("/contract/web/:key/*path", get(web_subpages))
            .layer(Extension(request_sender));

        (gateway, router)
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

async fn home() -> axum::response::Response {
    axum::response::Response::default()
}

async fn web_home(
    Path(key): Path<String>,
    Extension(rs): Extension<mpsc::Sender<ClientConnection>>,
) -> Result<axum::response::Response, WebSocketApiError> {
    let contract_idx = web_handling::contract_home(key, rs).await?;
    Ok(contract_idx.into_response())
}

async fn web_subpages(
    Path((key, last_path)): Path<(String, String)>,
) -> Result<axum::response::Response, WebSocketApiError> {
    let full_path: String = format!("/contract/web/{}/{}", key, last_path);
    web_handling::variable_content(key, full_path)
        .await
        .map_err(|e| *e)
        .map(|r| r.into_response())
}

async fn websocket_commands(
    ws: WebSocketUpgrade,
    Extension(rs): Extension<mpsc::Sender<ClientConnection>>,
) -> Response {
    let on_upgrade = move |ws: WebSocket| async move {
        if let Err(e) = websocket_interface(rs.clone(), ws).await {
            tracing::error!("{e}");
        }
    };
    ws.on_upgrade(on_upgrade)
}

async fn websocket_interface(
    request_sender: mpsc::Sender<ClientConnection>,
    ws: WebSocket,
) -> Result<(), DynError> {
    let (mut response_rx, client_id) = new_client_connection(&request_sender).await?;
    let (mut tx, mut rx) = ws.split();
    let listeners: Arc<Mutex<VecDeque<(_, UnboundedReceiver<HostResult>)>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    loop {
        let active_listeners = listeners.clone();
        let listeners_task = async move {
            loop {
                let mut lock = active_listeners.lock().await;
                let active_listeners = &mut *lock;
                for _ in 0..active_listeners.len() {
                    if let Some((key, mut listener)) = active_listeners.pop_front() {
                        match listener.try_recv() {
                            Ok(r) => {
                                active_listeners.push_back((key, listener));
                                return Ok(r);
                            }
                            Err(TryRecvError::Empty) => {
                                active_listeners.push_back((key, listener));
                            }
                            Err(err @ TryRecvError::Disconnected) => {
                                return Err(Box::new(err) as DynError)
                            }
                        }
                    }
                }
                std::mem::drop(lock);
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

        tokio::select! { biased;
            msg = async { process_host_response(response_rx.recv().await, client_id, &mut tx).await } => {
                let active_listeners = listeners.clone();
                if let Some(NewSubscription { key, callback }) = msg? {
                    tracing::debug!(cli_id = %client_id, contract = %key, "added new notification listener");
                    let active_listeners = &mut *active_listeners.lock().await;
                    active_listeners.push_back((key, callback));
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
                let msg = bincode::serialize(&response)?;
                tx.send(Message::Binary(msg)).await?;
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
    msg: Result<Message, axum::Error>,
    request_sender: &mpsc::Sender<ClientConnection>,
) -> Result<Option<Message>, Option<DynError>> {
    let msg = match msg {
        Ok(Message::Binary(data)) => data,
        Ok(Message::Text(data)) => data.into_bytes(),
        Ok(Message::Close(_)) => return Err(None),
        Ok(Message::Ping(_)) => return Ok(Some(Message::Pong(vec![0, 3, 2]))),
        Ok(m) => {
            tracing::debug!(msg = ?m, "received random message");
            return Ok(None);
        }
        Err(err) => return Err(Some(err.into())),
    };

    // Try to deserialize the ClientRequest message
    let req = {
        if let Ok(client_request) = bincode::deserialize::<ClientRequest<'_>>(&msg) {
            client_request
        } else if let Ok(client_request) = root_as_client_request(&msg) {
            match client_request.client_request_type() {
                ClientRequestType::ContractRequest => {
                    let contract_request =
                        client_request.client_request_as_contract_request().unwrap();
                    ContractRequest::try_decode_fbs(&contract_request)?.into()
                }
                ClientRequestType::DelegateRequest => {
                    let delegate_request =
                        client_request.client_request_as_delegate_request().unwrap();
                    DelegateRequest::try_decode_fbs(&delegate_request)?.into()
                }
                ClientRequestType::GenerateRandData => {
                    let delegate_request = client_request
                        .client_request_as_generate_rand_data()
                        .unwrap();
                    ClientRequest::GenerateRandData {
                        bytes: delegate_request.data() as usize,
                    }
                    .into()
                }
                ClientRequestType::Disconnect => {
                    let delegate_request = client_request.client_request_as_disconnect().unwrap();
                    let cause = if let Some(cuase_msg) = delegate_request.cause() {
                        Some(cuase_msg.to_string())
                    } else {
                        None
                    };
                    ClientRequest::Disconnect { cause }.into()
                }
                _ => unreachable!(),
            }
        } else {
            match ContractRequest::try_decode(&msg) {
                Ok(r) => r.to_owned().into(),
                Err(e) => {
                    let result_error = bincode::serialize(&Err::<HostResponse, ClientError>(
                        ErrorKind::DeserializationError {
                            cause: format!("{e}"),
                        }
                        .into(),
                    ))
                    .map_err(|err| Some(err.into()))?;
                    return Ok(Some(Message::Binary(result_error)));
                }
            }
        }
    };

    tracing::debug!(req = %req, "received client request");
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(req),
        })
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
                            key,
                            contract,
                            state,
                        }) => Ok(ContractResponse::GetResponse {
                            key,
                            contract,
                            state,
                        }
                        .into()),
                        other => Ok(other),
                    }
                }
                Err(err) => {
                    tracing::debug!(response = %err, cli_id = %id, "sending response error");
                    Err(err)
                }
            };
            let res = bincode::serialize(&result)?;
            tx.send(Message::Binary(res)).await?;
            Ok(None)
        }
        Some(HostCallbackResult::SubscriptionChannel { key, id, callback }) => {
            debug_assert_eq!(id, client_id);
            Ok(Some(NewSubscription { key, callback }))
        }
        Some(HostCallbackResult::NewId(cli_id)) => {
            tracing::debug!(%cli_id, "new client registered");
            Ok(None)
        }
        None => {
            let result_error = bincode::serialize(&Err::<HostResponse, ClientError>(
                ErrorKind::NodeUnavailable.into(),
            ))?;
            tx.send(Message::Binary(result_error)).await?;
            tx.send(Message::Close(None)).await?;
            tracing::warn!("node shut down while handling responses for {client_id}");
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
            ClientConnection::Request { client_id, req } => {
                match *req.clone() {
                    ClientRequest::ContractOp(ContractRequest::Subscribe { key, summary }) => {
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
                                OpenRequest::new(
                                    client_id,
                                    Box::new(ContractRequest::Subscribe { key, summary }.into()),
                                )
                                .with_notification(tx),
                            ))
                        } else {
                            tracing::warn!("client: {client_id} not found");
                            Err(ErrorKind::UnknownClient(client_id.into()).into())
                        }
                    }
                    _ => {
                        // just forward the request to the node
                        Ok(Some(OpenRequest::new(client_id, req)))
                    }
                }
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
                } else {
                    tracing::info!("dropped connection to client #{id}");
                }
            } else {
                tracing::warn!("client: {id} not found");
            }
            Ok(())
        }
        .boxed()
    }
}
