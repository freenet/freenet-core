use futures::{future::BoxFuture, stream::SplitSink, FutureExt, SinkExt, StreamExt};

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Path, Query, WebSocketUpgrade};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Router};
use locutus_core::*;
use locutus_stdlib::{
    client_api::{
        ClientError, ClientRequest, ContractRequest, ContractResponse, ErrorKind, HostResponse,
    },
    prelude::*,
};
use serde::Deserialize;

use std::collections::VecDeque;
use std::fmt::Display;
use std::sync::OnceLock;
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
use crate::{web_handling, AuthToken, ClientConnection, DynError, HostCallbackResult};

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
    pub(crate) attested_contracts: HashMap<AuthToken, (ContractInstanceId, ClientId)>,
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
            attested_contracts: HashMap::new(),
        };

        let router = Router::new()
            .route("/", get(home))
            .route("/contract/command", get(websocket_commands))
            .route("/contract/web/:key/", get(web_home))
            .route("/contract/web/:key/*path", get(web_subpages))
            .layer(axum::middleware::from_fn(connection_info))
            .layer(Extension(request_sender));

        (gateway, router)
    }

    pub fn next_client_id() -> ClientId {
        internal_next_client_id()
    }
}

async fn connection_info<B>(
    encoding_protoc: Result<
        axum::TypedHeader<EncodingProtocol>,
        axum::extract::rejection::TypedHeaderRejection,
    >,
    auth_token: Result<
        axum::TypedHeader<axum::headers::Authorization<axum::headers::authorization::Bearer>>,
        axum::extract::rejection::TypedHeaderRejection,
    >,
    Query(ConnectionInfo {
        auth_token: auth_token_q,
        encoding_protocol,
    }): Query<ConnectionInfo>,
    mut req: axum::http::Request<B>,
    next: axum::middleware::Next<B>,
) -> Response {
    // tracing::info!(
    //     "headers: {:?}",
    //     req.headers()
    //         .iter()
    //         .flat_map(|(k, v)| v.to_str().ok().map(|v| format!("{k}: {v}")))
    //         .collect::<Vec<_>>()
    // );
    let encoding_protoc = match encoding_protoc {
        Ok(protoc) => protoc.0,
        Err(err)
            if matches!(
                err.reason(),
                axum::extract::rejection::TypedHeaderRejectionReason::Missing
            ) =>
        {
            encoding_protocol.unwrap_or(EncodingProtocol::Flatbuffers)
        }
        Err(other) => return other.into_response(),
    };

    let auth_token = match auth_token {
        Ok(auth_token) => Some(AuthToken::from(auth_token.token().to_owned())),
        Err(err)
            if matches!(
                err.reason(),
                axum::extract::rejection::TypedHeaderRejectionReason::Missing
            ) =>
        {
            auth_token_q
        }
        Err(other) => return other.into_response(),
    };

    tracing::debug!(
        "establishing connection with encoding protocol: {encoding_protoc}, authenticated: {auth}",
        auth = auth_token.is_some()
    );
    req.extensions_mut().insert(encoding_protoc);
    req.extensions_mut().insert(auth_token);

    next.run(req).await
}

fn internal_next_client_id() -> ClientId {
    ClientId::new(REQUEST_ID.fetch_add(1, Ordering::SeqCst))
}

async fn new_client_connection(
    request_sender: &mpsc::Sender<ClientConnection>,
) -> Result<(mpsc::UnboundedReceiver<HostCallbackResult>, ClientId), ClientError> {
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send(ClientConnection::NewConnection {
            notifications: response_sender,
            assigned_token: None,
        })
        .await
        .map_err(|_| ErrorKind::NodeUnavailable)?;
    match response_recv.recv().await {
        Some(HostCallbackResult::NewId { id: client_id, .. }) => Ok((response_recv, client_id)),
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
    use axum::headers::HeaderMapExt;
    let token = AuthToken::generate();
    let token_header = axum::headers::Authorization::bearer(token.as_str()).unwrap();
    let contract_idx = web_handling::contract_home(key, rs, token).await?;
    let mut response = contract_idx.into_response();
    response.headers_mut().typed_insert(token_header);
    Ok(response)
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

#[derive(Clone, Copy, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum EncodingProtocol {
    /// Flatbuffers
    Flatbuffers,
    /// Rust native types
    Native,
}

impl Display for EncodingProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodingProtocol::Flatbuffers => write!(f, "flatbuffers"),
            EncodingProtocol::Native => write!(f, "native"),
        }
    }
}

impl axum::headers::Header for EncodingProtocol {
    fn name() -> &'static axum::http::HeaderName {
        static HEADER: OnceLock<axum::http::HeaderName> = OnceLock::new();
        HEADER.get_or_init(|| axum::http::HeaderName::from_static("encoding-protocol"))
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, axum::headers::Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i axum::http::HeaderValue>,
    {
        values
            .next()
            .and_then(|val| match val.to_str().ok()? {
                "native" => Some(EncodingProtocol::Native),
                "flatbuffers" => Some(EncodingProtocol::Flatbuffers),
                _ => None,
            })
            .ok_or_else(axum::headers::Error::invalid)
    }

    fn encode<E: Extend<axum::http::HeaderValue>>(&self, values: &mut E) {
        let header = match self {
            EncodingProtocol::Native => axum::http::HeaderValue::from_static("native"),
            EncodingProtocol::Flatbuffers => axum::http::HeaderValue::from_static("flatbuffers"),
        };
        values.extend([header]);
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConnectionInfo {
    auth_token: Option<AuthToken>,
    encoding_protocol: Option<EncodingProtocol>,
}

async fn websocket_commands(
    ws: WebSocketUpgrade,
    Extension(auth_token): Extension<Option<AuthToken>>,
    Extension(encoding_protoc): Extension<EncodingProtocol>,
    Extension(rs): Extension<mpsc::Sender<ClientConnection>>,
) -> Response {
    let on_upgrade = move |ws: WebSocket| async move {
        if let Err(e) = websocket_interface(rs.clone(), auth_token, encoding_protoc, ws).await {
            tracing::error!("{e}");
        }
    };
    ws.on_upgrade(on_upgrade)
}

async fn websocket_interface(
    request_sender: mpsc::Sender<ClientConnection>,
    mut auth_token: Option<AuthToken>,
    encoding_protoc: EncodingProtocol,
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
            process_client_request(
                client_id,
                next_msg,
                &request_sender,
                &mut auth_token,
                encoding_protoc,
            )
            .await
        };

        tokio::select! { biased;
            msg = async { process_host_response(response_rx.recv().await, client_id, encoding_protoc, &mut tx).await } => {
                let active_listeners = listeners.clone();
                if let Some(NewSubscription { key, callback }) = msg? {
                    tracing::debug!(cli_id = %client_id, contract = %key, "added new notification listener");
                    let active_listeners = &mut *active_listeners.lock().await;
                    active_listeners.push_back((key, callback));
                }
            }
            process_client_request = client_req_task => {
                match process_client_request {
                    Ok(Some(error)) => {
                        tx.send(error).await?;
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
    auth_token: &mut Option<AuthToken>,
    encoding_protoc: EncodingProtocol,
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
        match encoding_protoc {
            EncodingProtocol::Flatbuffers => match ClientRequest::try_decode_fbs(&msg) {
                Ok(decoded) => decoded.into_owned(),
                Err(err) => return Ok(Some(Message::Binary(err.into_fbs_bytes()))),
            },
            EncodingProtocol::Native => match bincode::deserialize::<ClientRequest>(&msg) {
                Ok(decoded) => decoded.into_owned(),
                Err(err) => {
                    let result_error = bincode::serialize(&Err::<HostResponse, ClientError>(
                        ErrorKind::DeserializationError {
                            cause: format!("{err}"),
                        }
                        .into(),
                    ))
                    .map_err(|err| Some(err.into()))?;
                    return Ok(Some(Message::Binary(result_error)));
                }
            },
        }
    };
    if let ClientRequest::Authenticate { token } = &req {
        *auth_token = Some(AuthToken::from(token.clone()));
    }

    tracing::debug!(req = %req, "received client request");
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(req),
            auth_token: auth_token.clone(),
        })
        .await
        .map_err(|err| Some(err.into()))?;
    Ok(None)
}

async fn process_host_response(
    msg: Option<HostCallbackResult>,
    client_id: ClientId,
    encoding_protoc: EncodingProtocol,
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
            let serialized_res = match encoding_protoc {
                EncodingProtocol::Flatbuffers => match result {
                    Ok(res) => res.into_fbs_bytes()?,
                    Err(err) => {
                        tracing::warn!("add an error type for client errors");
                        return Err(err.into()); // FIXME: add to schema ClientError
                    }
                },
                EncodingProtocol::Native => bincode::serialize(&result)?,
            };
            tx.send(Message::Binary(serialized_res)).await?;
            Ok(None)
        }
        Some(HostCallbackResult::SubscriptionChannel { key, id, callback }) => {
            debug_assert_eq!(id, client_id);
            Ok(Some(NewSubscription { key, callback }))
        }
        Some(HostCallbackResult::NewId { id: cli_id }) => {
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
            ClientConnection::NewConnection {
                notifications,
                assigned_token,
            } => {
                // is a new client, assign an id and open a channel to communicate responses from the node
                let cli_id = internal_next_client_id();
                notifications
                    .send(HostCallbackResult::NewId { id: cli_id })
                    .map_err(|_e| ErrorKind::NodeUnavailable)?;
                self.response_channels.insert(cli_id, notifications);
                if let Some((assigned_token, contract)) = assigned_token {
                    self.attested_contracts
                        .insert(assigned_token, (contract, cli_id));
                }
                Ok(None)
            }
            ClientConnection::Request {
                client_id,
                req,
                auth_token,
            } => {
                let open_req = match &*req {
                    ClientRequest::ContractOp(ContractRequest::Subscribe { key, .. }) => {
                        // intercept subscription messages because they require a callback subscription channel
                        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                        if let Some(ch) = self.response_channels.get(&client_id) {
                            ch.send(HostCallbackResult::SubscriptionChannel {
                                key: key.clone(),
                                id: client_id,
                                callback: rx,
                            })
                            .map_err(|_| ErrorKind::ChannelClosed)?;
                            OpenRequest::new(client_id, req)
                                .with_notification(tx)
                                .with_token(auth_token)
                        } else {
                            tracing::warn!("client: {client_id} not found");
                            return Err(ErrorKind::UnknownClient(client_id.into()).into());
                        }
                    }
                    _ => {
                        // just forward the request to the node
                        OpenRequest::new(client_id, req).with_token(auth_token)
                    }
                };
                Ok(Some(open_req))
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
