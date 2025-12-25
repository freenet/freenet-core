use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, OnceLock},
    time::Duration,
};

use dashmap::DashMap;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Query, WebSocketUpgrade,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Extension, Router,
};
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, ErrorKind, HostResponse},
    prelude::*,
};
use futures::{future::BoxFuture, stream::SplitSink, FutureExt, SinkExt, StreamExt};
use headers::Header;
use serde::Deserialize;
use tokio::sync::{mpsc, Mutex};

use crate::{
    client_events::AuthToken,
    server::{ClientConnection, HostCallbackResult},
    util::EncodingProtocol,
};

use super::{ClientError, ClientEventsProxy, ClientId, HostResult, OpenRequest};
use crate::server::http_gateway::AttestedContractMap;

#[derive(Clone)]
struct WebSocketRequest(mpsc::Sender<ClientConnection>);

impl std::ops::Deref for WebSocketRequest {
    type Target = mpsc::Sender<ClientConnection>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct WebSocketProxy {
    proxy_server_request: mpsc::Receiver<ClientConnection>,
    response_channels: HashMap<ClientId, mpsc::UnboundedSender<HostCallbackResult>>,
}

const PARALLELISM: usize = 10; // TODO: get this from config, or whatever optimal way

impl WebSocketProxy {
    pub fn create_router(server_routing: Router) -> (Self, Router) {
        // Create a default empty attested contracts map
        let attested_contracts = Arc::new(DashMap::new());
        Self::create_router_with_attested_contracts(server_routing, attested_contracts)
    }

    pub fn create_router_with_attested_contracts(
        server_routing: Router,
        attested_contracts: AttestedContractMap,
    ) -> (Self, Router) {
        let (proxy_request_sender, proxy_server_request) = mpsc::channel(PARALLELISM);

        // Using Extension instead of with_state to avoid changing the Router's type parameter
        let router = server_routing
            .route("/v1/contract/command", get(websocket_commands))
            .layer(Extension(attested_contracts))
            .layer(Extension(WebSocketRequest(proxy_request_sender)))
            .layer(axum::middleware::from_fn(connection_info));

        (
            WebSocketProxy {
                proxy_server_request,
                response_channels: HashMap::new(),
            },
            router,
        )
    }

    async fn internal_proxy_recv(
        &mut self,
        msg: ClientConnection,
    ) -> Result<Option<OpenRequest<'_>>, ClientError> {
        match msg {
            ClientConnection::NewConnection { callbacks, .. } => {
                // is a new client, assign an id and open a channel to communicate responses from the node
                let cli_id = ClientId::next();
                callbacks
                    .send(HostCallbackResult::NewId { id: cli_id })
                    .map_err(|_e| ErrorKind::NodeUnavailable)?;
                self.response_channels.insert(cli_id, callbacks);
                Ok(None)
            }
            ClientConnection::Request {
                client_id,
                req,
                auth_token,
                attested_contract,
            } => {
                let open_req = match &*req {
                    ClientRequest::ContractOp(ContractRequest::Subscribe { key, .. }) => {
                        tracing::debug!(%client_id, contract = %key, "subscribing to contract");
                        // intercept subscription messages because they require a callback subscription channel
                        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                        if let Some(ch) = self.response_channels.get(&client_id) {
                            ch.send(HostCallbackResult::SubscriptionChannel {
                                key: *key,
                                id: client_id,
                                callback: rx,
                            })
                            .map_err(|_| ErrorKind::ChannelClosed)?;
                            OpenRequest::new(client_id, req)
                                .with_notification(tx)
                                .with_token(auth_token)
                                .with_attested_contract(attested_contract)
                        } else {
                            tracing::warn!(
                                client_id = %client_id,
                                "Client not found for request"
                            );
                            return Err(ErrorKind::UnknownClient(client_id.into()).into());
                        }
                    }
                    ClientRequest::ContractOp(ContractRequest::Get {
                        key,
                        subscribe: true,
                        ..
                    }) => {
                        tracing::debug!(%client_id, contract = %key, "get with auto-subscribe");
                        // intercept GET with subscribe=true because they also require a callback subscription channel
                        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                        if let Some(ch) = self.response_channels.get(&client_id) {
                            ch.send(HostCallbackResult::SubscriptionChannel {
                                key: *key,
                                id: client_id,
                                callback: rx,
                            })
                            .map_err(|_| ErrorKind::ChannelClosed)?;
                            OpenRequest::new(client_id, req)
                                .with_notification(tx)
                                .with_token(auth_token)
                                .with_attested_contract(attested_contract)
                        } else {
                            tracing::warn!(
                                client_id = %client_id,
                                "Client not found for request"
                            );
                            return Err(ErrorKind::UnknownClient(client_id.into()).into());
                        }
                    }
                    ClientRequest::ContractOp(ContractRequest::Put {
                        contract,
                        subscribe: true,
                        ..
                    }) => {
                        tracing::debug!(%client_id, contract = %contract.key(), "put with auto-subscribe");
                        // intercept PUT with subscribe=true because they also require a callback subscription channel
                        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                        if let Some(ch) = self.response_channels.get(&client_id) {
                            ch.send(HostCallbackResult::SubscriptionChannel {
                                key: *contract.key().id(),
                                id: client_id,
                                callback: rx,
                            })
                            .map_err(|_| ErrorKind::ChannelClosed)?;
                            OpenRequest::new(client_id, req)
                                .with_notification(tx)
                                .with_token(auth_token)
                                .with_attested_contract(attested_contract)
                        } else {
                            tracing::warn!(
                                client_id = %client_id,
                                "Client not found for request"
                            );
                            return Err(ErrorKind::UnknownClient(client_id.into()).into());
                        }
                    }
                    _ => {
                        // just forward the request to the node
                        OpenRequest::new(client_id, req)
                            .with_token(auth_token)
                            .with_attested_contract(attested_contract)
                    }
                };
                Ok(Some(open_req))
            }
        }
    }
}

struct EncodingProtocolExt(EncodingProtocol);

impl headers::Header for EncodingProtocolExt {
    fn name() -> &'static axum::http::HeaderName {
        static HEADER: OnceLock<axum::http::HeaderName> = OnceLock::new();
        HEADER.get_or_init(|| axum::http::HeaderName::from_static("encoding-protocol"))
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        Self: Sized,
        I: Iterator<Item = &'i axum::http::HeaderValue>,
    {
        values
            .next()
            .and_then(|val| match val.to_str().ok()? {
                "native" => Some(EncodingProtocolExt(EncodingProtocol::Native)),
                "flatbuffers" => Some(EncodingProtocolExt(EncodingProtocol::Flatbuffers)),
                _ => None,
            })
            .ok_or_else(headers::Error::invalid)
    }

    fn encode<E: Extend<axum::http::HeaderValue>>(&self, values: &mut E) {
        let header = match self.0 {
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

async fn connection_info(
    Query(ConnectionInfo {
        auth_token: auth_token_q,
        encoding_protocol,
    }): Query<ConnectionInfo>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    use headers::{
        authorization::{Authorization, Bearer},
        HeaderMapExt,
    };
    // tracing::info!(
    //     "headers: {:?}",
    //     req.headers()
    //         .iter()
    //         .flat_map(|(k, v)| v.to_str().ok().map(|v| format!("{k}: {v}")))
    //         .collect::<Vec<_>>()
    // );

    let encoding_protoc = match req.headers().typed_try_get::<EncodingProtocolExt>() {
        Ok(Some(protoc)) => protoc.0,
        Ok(None) => encoding_protocol.unwrap_or(EncodingProtocol::Flatbuffers),
        Err(_error) => {
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "Incorrect `{header}` header specification",
                    header = EncodingProtocolExt::name()
                ),
            )
                .into_response()
        }
    };

    let auth_token = match req.headers().typed_try_get::<Authorization<Bearer>>() {
        Ok(Some(value)) => Some(AuthToken::from(value.token().to_owned())),
        Ok(None) => auth_token_q.clone(),
        Err(_error) => {
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "Incorrect Bearer `{header}` header specification",
                    header = Authorization::<Bearer>::name()
                ),
            )
                .into_response()
        }
    };

    tracing::debug!(
        ?auth_token_q, ?auth_token, request_uri = ?req.uri(), "connection_info middleware extracting auth token and encoding protocol",
    );
    req.extensions_mut().insert(encoding_protoc);
    req.extensions_mut().insert(auth_token);

    next.run(req).await
}

async fn websocket_commands(
    ws: WebSocketUpgrade,
    Extension(auth_token): Extension<Option<AuthToken>>,
    Extension(encoding_protoc): Extension<EncodingProtocol>,
    Extension(rs): Extension<WebSocketRequest>,
    Extension(attested_contracts): Extension<AttestedContractMap>,
) -> Response {
    let on_upgrade = move |ws: WebSocket| async move {
        // Get the data we need from the DashMap
        let auth_and_instance = if let Some(token) = auth_token.as_ref() {
            // Only collect and log map contents when trace is enabled
            if tracing::enabled!(tracing::Level::TRACE) {
                let map_contents: Vec<_> =
                    attested_contracts.iter().map(|e| e.key().clone()).collect();
                tracing::trace!(?token, "attested_contracts map keys: {:?}", map_contents);
            }

            if let Some(entry) = attested_contracts.get(token) {
                let attested = entry.value();
                tracing::trace!(?token, contract_id = ?attested.contract_id, "Found token in attested_contracts map");
                Some((token.clone(), attested.contract_id))
            } else {
                tracing::warn!(?token, "Auth token not found in attested_contracts map");
                None
            }
        } else {
            tracing::trace!("No auth token provided in WebSocket request");
            None
        };

        // Only evaluate auth_and_instance for trace when trace is enabled
        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(protoc = ?ws.protocol(), ?auth_and_instance, "websocket connection established");
        } else {
            tracing::trace!(protoc = ?ws.protocol(), "websocket connection established");
        }
        if let Err(error) =
            websocket_interface(rs.clone(), auth_and_instance, encoding_protoc, ws).await
        {
            // Client-side disconnects (e.g., closing without handshake) are expected
            // and should not be logged as errors. These occur when clients timeout,
            // crash, or close connections abruptly.
            let error_msg = error.to_string();
            if error_msg.contains("Connection reset without closing handshake")
                || error_msg.contains("connection was aborted")
                || error_msg.contains("connection reset by peer")
            {
                tracing::warn!("WebSocket client disconnect: {error}");
            } else {
                tracing::error!("WebSocket protocol error: {error}");
            }
        }
    };

    // Increase max message size to 100MB to handle contract uploads
    // Default is ~64KB which is too small for WASM contracts
    ws.max_message_size(100 * 1024 * 1024)
        .on_upgrade(on_upgrade)
}

async fn websocket_interface(
    request_sender: WebSocketRequest,
    mut auth_token: Option<(AuthToken, ContractInstanceId)>,
    encoding_protoc: EncodingProtocol,
    ws: WebSocket,
) -> anyhow::Result<()> {
    let (mut response_rx, client_id) =
        new_client_connection(&request_sender, auth_token.clone()).await?;
    let (mut server_sink, mut client_stream) = ws.split();
    let contract_updates: Arc<Mutex<VecDeque<(_, mpsc::UnboundedReceiver<HostResult>)>>> =
        Arc::new(Mutex::new(VecDeque::new()));
    loop {
        let contract_updates_cp = contract_updates.clone();
        let listeners_task = async move {
            loop {
                let mut lock = contract_updates_cp.lock().await;
                let active_listeners = &mut *lock;
                for _ in 0..active_listeners.len() {
                    if let Some((key, mut listener)) = active_listeners.pop_front() {
                        match listener.try_recv() {
                            Ok(r) => {
                                active_listeners.push_back((key, listener));
                                return Ok::<_, anyhow::Error>(r);
                            }
                            Err(mpsc::error::TryRecvError::Empty) => {
                                active_listeners.push_back((key, listener));
                            }
                            Err(mpsc::error::TryRecvError::Disconnected) => {
                                tracing::debug!(contract = %key, "listener removed");
                            }
                        }
                    }
                }
                std::mem::drop(lock);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        let client_req_task = async {
            let next_msg = match client_stream
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
                &mut auth_token.as_mut().map(|t| t.0.clone()),
                auth_token.as_mut().map(|t| t.1),
                encoding_protoc,
            )
            .await
        };

        tokio::select! { biased;
            msg = async { process_host_response(response_rx.recv().await, client_id, encoding_protoc, &mut server_sink).await } => {
                let active_listeners = contract_updates.clone();
                if let Some(NewSubscription { key, callback }) = msg? {
                    tracing::debug!(cli_id = %client_id, contract = %key, "added new notification listener");
                    let active_listeners = &mut *active_listeners.lock().await;
                    active_listeners.push_back((key, callback));
                }
            }
            process_client_request = client_req_task => {
                match process_client_request {
                    Ok(Some(error)) => {
                        server_sink.send(error).await.inspect_err(|err| {
                            tracing::debug!(err = %err, "error sending message to client");
                        })?;
                    }
                    Ok(None) => continue,
                    Err(None) => {
                        tracing::debug!(%client_id, "Client channel closed, notifying node for subscription cleanup");
                        // Notify node about client disconnect to trigger subscription cleanup
                        let _ = request_sender
                            .send(ClientConnection::Request {
                                client_id,
                                req: Box::new(ClientRequest::Disconnect { cause: None }),
                                auth_token: auth_token.as_ref().map(|t| t.0.clone()),
                                attested_contract: auth_token.as_ref().map(|t| t.1),
                            })
                            .await;
                        let _ = server_sink.send(Message::Close(None)).await;
                        return Ok(())
                    },
                    Err(Some(err)) => {
                        tracing::debug!(%client_id, err = %err, "Client channel error, notifying node for subscription cleanup");
                        // Notify node about client disconnect to trigger subscription cleanup even on error
                        let _ = request_sender
                            .send(ClientConnection::Request {
                                client_id,
                                req: Box::new(ClientRequest::Disconnect { cause: None }),
                                auth_token: auth_token.as_ref().map(|t| t.0.clone()),
                                attested_contract: auth_token.as_ref().map(|t| t.1),
                            })
                            .await;
                        return Err(err)
                    },
                }
            }
            response = listeners_task => {
                let response = response?;
                match &response {
                    Ok(res) => tracing::debug!(response = %res, cli_id = %client_id, "sending notification"),
                    Err(err) => tracing::debug!(response = %err, cli_id = %client_id, "sending notification error"),
                }
                let serialized_res = match encoding_protoc {
                    EncodingProtocol::Flatbuffers => match response {
                        Ok(res) => res.into_fbs_bytes()?,
                        Err(err) => err.into_fbs_bytes()?,
                    },
                    EncodingProtocol::Native => bincode::serialize(&response)?,
                };
                server_sink.send(Message::Binary(serialized_res.into())).await.inspect_err(|err| {
                    tracing::debug!(err = %err, "error sending message to client");
                })?;
            }
        }
    }
}

async fn new_client_connection(
    request_sender: &WebSocketRequest,
    assigned_token: Option<(AuthToken, ContractInstanceId)>,
) -> Result<(mpsc::UnboundedReceiver<HostCallbackResult>, ClientId), ClientError> {
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    tracing::debug!(?assigned_token, "sending new client connection request");
    request_sender
        .send(ClientConnection::NewConnection {
            callbacks: response_sender,
            assigned_token,
        })
        .await
        .map_err(|_| ErrorKind::NodeUnavailable)?;
    match response_recv.recv().await {
        Some(HostCallbackResult::NewId { id: client_id, .. }) => Ok((response_recv, client_id)),
        None => Err(ErrorKind::NodeUnavailable.into()),
        other => unreachable!("received unexpected message after NewConnection: {other:?}"),
    }
}

struct NewSubscription {
    key: ContractInstanceId,
    callback: mpsc::UnboundedReceiver<HostResult>,
}

async fn process_client_request(
    client_id: ClientId,
    msg: Result<Message, axum::Error>,
    request_sender: &mpsc::Sender<ClientConnection>,
    auth_token: &mut Option<AuthToken>,
    attested_contract: Option<ContractInstanceId>,
    encoding_protoc: EncodingProtocol,
) -> Result<Option<Message>, Option<anyhow::Error>> {
    let msg = match msg {
        Ok(Message::Binary(data)) => data.to_vec(),
        Ok(Message::Text(data)) => data.as_bytes().to_vec(),
        Ok(Message::Close(_)) => return Err(None),
        Ok(Message::Ping(ping)) => return Ok(Some(Message::Pong(ping))),
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
                Err(err) => return Ok(Some(Message::Binary(err.into_fbs_bytes().into()))),
            },
            EncodingProtocol::Native => match bincode::deserialize::<ClientRequest>(&msg) {
                Ok(decoded) => decoded.into_owned(),
                Err(err) => {
                    let result_error = bincode::serialize(&Err::<HostResponse, ClientError>(
                        ErrorKind::DeserializationError {
                            cause: format!("{err}").into(),
                        }
                        .into(),
                    ))
                    .map_err(|err| Some(err.into()))?;
                    return Ok(Some(Message::Binary(result_error.into())));
                }
            },
        }
    };

    // Intercept explicit disconnect requests sent by the client as data messages
    if matches!(req, ClientRequest::Disconnect { .. }) {
        // Treat this like a WebSocket close message
        tracing::debug!("Client explicitly sent a Disconnect request, closing connection.");
        return Err(None); // Signal graceful closure to websocket_interface
    }

    if let ClientRequest::Authenticate { token } = &req {
        *auth_token = Some(AuthToken::from(token.clone()));
    }

    tracing::debug!(req = %req, "received client request");
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(req),
            auth_token: auth_token.clone(),
            attested_contract,
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
) -> anyhow::Result<Option<NewSubscription>> {
    match msg {
        Some(HostCallbackResult::Result { id, result }) => {
            debug_assert_eq!(id, client_id);
            let result = match result {
                Ok(res) => {
                    let response_type = match res {
                        HostResponse::ContractResponse { .. } => "ContractResponse",
                        HostResponse::DelegateResponse { .. } => "DelegateResponse",
                        HostResponse::QueryResponse(_) => "QueryResponse",
                        HostResponse::Ok => "HostResponse::Ok",
                        _ => "Unknown",
                    };

                    // Enhanced logging for UPDATE responses
                    match &res {
                        HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                            key,
                            summary,
                        }) => {
                            tracing::debug!(
                                client_id = %id,
                                contract = %key,
                                summary_size = summary.size(),
                                phase = "update_response",
                                "Processing UpdateResponse for WebSocket delivery"
                            );
                        }
                        _ => {
                            tracing::debug!(response = %res, response_type, cli_id = %id, "sending response");
                        }
                    }

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
            // Log when UPDATE response is about to be sent over WebSocket
            let is_update_response = match &result {
                Ok(HostResponse::ContractResponse(ContractResponse::UpdateResponse {
                    key,
                    ..
                })) => {
                    tracing::debug!(
                        client_id = %client_id,
                        contract = %key,
                        phase = "serializing",
                        "Serializing UpdateResponse for WebSocket delivery"
                    );
                    Some(*key)
                }
                _ => None,
            };

            let serialized_res = match encoding_protoc {
                EncodingProtocol::Flatbuffers => match result {
                    Ok(res) => res.into_fbs_bytes()?,
                    Err(err) => err.into_fbs_bytes()?,
                },
                EncodingProtocol::Native => bincode::serialize(&result)?,
            };

            // Log serialization completion for UPDATE responses
            if let Some(key) = is_update_response {
                tracing::debug!(
                    client_id = %client_id,
                    contract = %key,
                    size_bytes = serialized_res.len(),
                    phase = "serialized",
                    "Serialized UpdateResponse for WebSocket delivery"
                );
            }

            let send_result = tx.send(Message::Binary(serialized_res.into())).await;

            // Log WebSocket send result for UPDATE responses
            if let Some(key) = is_update_response {
                match &send_result {
                    Ok(()) => {
                        tracing::debug!(
                            client_id = %client_id,
                            contract = %key,
                            phase = "sent",
                            "Successfully sent UpdateResponse over WebSocket"
                        );
                    }
                    Err(err) => {
                        tracing::error!(
                            client_id = %client_id,
                            contract = %key,
                            error = ?err,
                            phase = "error",
                            "Failed to send UpdateResponse over WebSocket"
                        );
                    }
                }
            }

            send_result?;
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
            tx.send(Message::Binary(result_error.into())).await?;
            tx.send(Message::Close(None)).await?;
            tracing::warn!(
                client_id = %client_id,
                "Node shut down while handling responses"
            );
            Err(anyhow::anyhow!(
                "node shut down while handling responses for client {}",
                client_id
            ))
        }
    }
}

impl ClientEventsProxy for WebSocketProxy {
    fn recv(&mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
        async move {
            loop {
                let msg = self.proxy_server_request.recv().await;
                if let Some(msg) = msg {
                    if let Some(reply) = self.internal_proxy_recv(msg).await? {
                        break Ok(reply.into_owned());
                    }
                } else {
                    break Err(ClientError::from(ErrorKind::ChannelClosed));
                }
            }
        }
        .boxed()
    }

    fn send(
        &mut self,
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>> {
        async move {
            // Log UPDATE responses specifically
            match &result {
                Ok(HostResponse::ContractResponse(
                    freenet_stdlib::client_api::ContractResponse::UpdateResponse { key, summary },
                )) => {
                    tracing::debug!(
                        client_id = %id,
                        contract = %key,
                        summary_size = summary.size(),
                        "WebSocket send() called with UpdateResponse"
                    );
                }
                Ok(other_response) => {
                    tracing::debug!(
                        client_id = %id,
                        response = ?other_response,
                        "WebSocket send() called with response"
                    );
                }
                Err(error) => {
                    tracing::debug!(
                        client_id = %id,
                        error = ?error,
                        "WebSocket send() called with error"
                    );
                }
            }

            if let Some(ch) = self.response_channels.remove(&id) {
                // Log success/failure of sending UPDATE responses
                if let Ok(HostResponse::ContractResponse(
                    freenet_stdlib::client_api::ContractResponse::UpdateResponse { key, .. },
                )) = &result
                {
                    tracing::debug!(
                        client_id = %id,
                        contract = %key,
                        "Found WebSocket channel, sending UpdateResponse"
                    );
                }

                // Check if this is an UPDATE response and extract key before moving result
                let update_key = match &result {
                    Ok(HostResponse::ContractResponse(
                        freenet_stdlib::client_api::ContractResponse::UpdateResponse {
                            key, ..
                        },
                    )) => Some(*key),
                    _ => None,
                };

                let should_rm = result
                    .as_ref()
                    .map_err(|err| matches!(err.kind(), ErrorKind::Disconnect))
                    .err()
                    .unwrap_or(false);

                let send_result = ch.send(HostCallbackResult::Result { id, result });

                // Log UPDATE response send result
                if let Some(key) = update_key {
                    match send_result.is_ok() {
                        true => {
                            tracing::debug!(
                                client_id = %id,
                                contract = %key,
                                phase = "sent",
                                "Successfully sent UpdateResponse to client"
                            );
                        }
                        false => {
                            tracing::error!(
                                client_id = %id,
                                contract = %key,
                                phase = "error",
                                "Failed to send UpdateResponse - channel send failed"
                            );
                        }
                    }
                }

                if send_result.is_ok() && !should_rm {
                    // still alive connection, keep it
                    self.response_channels.insert(id, ch);
                } else {
                    tracing::info!(
                        client_id = %id,
                        "Dropped connection to client"
                    );
                }
            } else {
                // Log when client is not found for UPDATE responses
                match &result {
                    Ok(HostResponse::ContractResponse(
                        freenet_stdlib::client_api::ContractResponse::UpdateResponse {
                            key, ..
                        },
                    )) => {
                        tracing::error!(
                            client_id = %id,
                            contract = %key,
                            "Client not found in WebSocket response channels for UpdateResponse"
                        );
                    }
                    _ => {
                        tracing::warn!(
                            client_id = %id,
                            "Client not found in response channels"
                        );
                    }
                }
            }
            Ok(())
        }
        .boxed()
    }
}
