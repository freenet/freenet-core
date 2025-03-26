use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, OnceLock},
    time::Duration,
};

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

mod v1;

#[derive(Clone)]
struct WebSocketRequest(mpsc::Sender<ClientConnection>);

impl std::ops::Deref for WebSocketRequest {
    type Target = mpsc::Sender<ClientConnection>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub(crate) struct WebSocketProxy {
    proxy_server_request: mpsc::Receiver<ClientConnection>,
    response_channels: HashMap<ClientId, mpsc::UnboundedSender<HostCallbackResult>>,
}

const PARALLELISM: usize = 10; // TODO: get this from config, or whatever optimal way

impl WebSocketProxy {
    pub fn as_router(server_routing: Router) -> (Self, Router) {
        WebSocketProxy::as_router_v1(server_routing)
    }

    async fn internal_proxy_recv(
        &mut self,
        msg: ClientConnection,
    ) -> Result<Option<OpenRequest>, ClientError> {
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
            } => {
                let open_req = match &*req {
                    ClientRequest::ContractOp(ContractRequest::Subscribe { key, .. }) => {
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
        Ok(None) => auth_token_q,
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
        "establishing connection with encoding protocol: {encoding_protoc}, authenticated: {auth}",
        auth = auth_token.is_some()
    );
    req.extensions_mut().insert(encoding_protoc);
    req.extensions_mut().insert(auth_token);

    next.run(req).await
}

#[tracing::instrument(
    level = "debug",
    name = "websocket_handler",
    skip(ws, rs),
    fields(has_auth = auth_token.is_some(), protocol = ?encoding_protoc)
)]
async fn websocket_commands(
    ws: WebSocketUpgrade,
    Extension(auth_token): Extension<Option<AuthToken>>,
    Extension(encoding_protoc): Extension<EncodingProtocol>,
    Extension(rs): Extension<WebSocketRequest>,
) -> axum::response::Response {
    tracing::info!("Upgrading connection to WebSocket");
    
    let on_upgrade = move |ws: WebSocket| async move {
        let protocol = ws.protocol().and_then(|p| p.to_str().ok().map(|s| s.to_string()));
        tracing::info!(
            protocol = protocol.as_deref().unwrap_or("none"),
            "WebSocket connection established"
        );
        
        match websocket_interface(rs.clone(), auth_token, encoding_protoc, ws).await {
            Ok(_) => {
                tracing::info!("WebSocket interface completed normally");
            }
            Err(error) => {
                tracing::error!(error = %error, "WebSocket interface error");
                if let Some(source) = error.source() {
                    tracing::error!(source = %source, "Caused by");
                }
            }
        }
    };
    ws.on_upgrade(on_upgrade)
}

#[tracing::instrument(
    level = "debug",
    name = "websocket_interface",
    skip(request_sender, auth_token, ws),
    fields(protocol = ?encoding_protoc)
)]
async fn websocket_interface(
    request_sender: WebSocketRequest,
    mut auth_token: Option<AuthToken>,
    encoding_protoc: EncodingProtocol,
    ws: WebSocket,
) -> anyhow::Result<()> {
    tracing::debug!("Starting WebSocket interface handler");
    
    let (mut response_rx, client_id) = new_client_connection(&request_sender).await?;
    tracing::info!(client_id = %client_id, "Client connection established");

    let (mut server_sink, mut client_stream) = ws.split();
    tracing::debug!("WebSocket stream split for bidirectional communication");
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
                                return Ok(r);
                            }
                            Err(mpsc::error::TryRecvError::Empty) => {
                                active_listeners.push_back((key, listener));
                            }
                            Err(err @ mpsc::error::TryRecvError::Disconnected) => {
                                tracing::debug!(err = ?err, "listener channel disconnected");
                                return Err(anyhow::anyhow!(err));
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
                &mut auth_token,
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
                        tracing::debug!("client channel closed on request");
                        let _ = server_sink.send(Message::Close(None)).await;
                        return Ok(())
                    },
                    Err(Some(err)) => {
                        tracing::debug!(err = %err, "client channel error on request");
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
                server_sink.send(Message::Binary(serialized_res)).await.inspect_err(|err| {
                    tracing::debug!(err = %err, "error sending message to client");
                })?;
            }
        }
    }
}

#[tracing::instrument(
    level = "debug",
    name = "new_client_connection",
    skip(request_sender)
)]
async fn new_client_connection(
    request_sender: &WebSocketRequest,
) -> Result<(mpsc::UnboundedReceiver<HostCallbackResult>, ClientId), ClientError> {
    tracing::debug!("Creating new client connection");
    
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    
    match request_sender
        .send(ClientConnection::NewConnection {
            callbacks: response_sender,
            assigned_token: None,
        })
        .await
    {
        Ok(_) => tracing::trace!("Sent new connection request"),
        Err(_) => {
            tracing::error!("Failed to send new connection request");
            return Err(ErrorKind::NodeUnavailable.into());
        }
    }
    
    match response_recv.recv().await {
        Some(HostCallbackResult::NewId { id: client_id, .. }) => {
            tracing::info!(client_id = %client_id, "Received client ID for new connection");
            Ok((response_recv, client_id))
        },
        None => {
            tracing::error!("No response received for new connection");
            Err(ErrorKind::NodeUnavailable.into())
        },
        other => {
            tracing::error!(response = ?other, "Unexpected response for new connection");
            unreachable!("received unexpected message: {other:?}")
        },
    }
}

struct NewSubscription {
    key: ContractKey,
    callback: mpsc::UnboundedReceiver<HostResult>,
}

#[tracing::instrument(
    level = "debug",
    name = "process_client_request",
    skip(msg, request_sender, auth_token),
    fields(client_id = %client_id, protocol = ?encoding_protoc)
)]
async fn process_client_request(
    client_id: ClientId,
    msg: Result<Message, axum::Error>,
    request_sender: &mpsc::Sender<ClientConnection>,
    auth_token: &mut Option<AuthToken>,
    encoding_protoc: EncodingProtocol,
) -> Result<Option<Message>, Option<anyhow::Error>> {
    let msg = match msg {
        Ok(Message::Binary(data)) => {
            tracing::debug!(size = data.len(), "Received binary message");
            data
        }
        Ok(Message::Text(data)) => {
            tracing::debug!(size = data.len(), "Received text message");
            data.into_bytes()
        }
        Ok(Message::Close(frame)) => {
            tracing::info!(
                code = frame.as_ref().map(|f| f.code as u16),
                reason = frame.as_ref().map(|f| f.reason.to_string()),
                "Received close frame"
            );
            return Err(None);
        }
        Ok(Message::Ping(ping)) => {
            tracing::debug!(size = ping.len(), "Received ping, sending pong");
            return Ok(Some(Message::Pong(ping)));
        }
        Ok(m) => {
            tracing::debug!(message_type = ?m, "Received unexpected message type");
            return Ok(None);
        }
        Err(err) => {
            tracing::error!(error = %err, "Error receiving WebSocket message");
            return Err(Some(err.into()));
        }
    };

    // Try to deserialize the ClientRequest message
    tracing::trace!(message_size = msg.len(), "Attempting to deserialize client request");
    
    let req = {
        match encoding_protoc {
            EncodingProtocol::Flatbuffers => match ClientRequest::try_decode_fbs(&msg) {
                Ok(decoded) => {
                    tracing::debug!(
                        protocol = "flatbuffers",
                        "Successfully decoded client request"
                    );
                    decoded.into_owned()
                }
                Err(err) => {
                    tracing::error!(
                        error = %err, 
                        protocol = "flatbuffers", 
                        "Failed to decode client request"
                    );
                    let bytes = err.into_fbs_bytes();
                    return Ok(Some(Message::Binary(bytes)));
                }
            },
            EncodingProtocol::Native => match bincode::deserialize::<ClientRequest>(&msg) {
                Ok(decoded) => {
                    tracing::debug!(
                        protocol = "native", 
                        "Successfully decoded client request"
                    );
                    decoded.into_owned()
                }
                Err(err) => {
                    tracing::error!(
                        error = %err, 
                        protocol = "native", 
                        "Failed to decode client request"
                    );
                    
                    let error_response = Err::<HostResponse, ClientError>(
                        ErrorKind::DeserializationError {
                            cause: format!("{err}").into(),
                        }
                        .into(),
                    );
                    
                    match bincode::serialize(&error_response) {
                        Ok(bytes) => return Ok(Some(Message::Binary(bytes))),
                        Err(e) => {
                            tracing::error!(error = %e, "Failed to serialize error response");
                            return Err(Some(e.into()));
                        }
                    }
                }
            },
        }
    };
    // Handle authentication
    if let ClientRequest::Authenticate { token } = &req {
        *auth_token = Some(AuthToken::from(token.clone()));
        tracing::info!("Client authenticated with token");
    }

    // Log specific details for contract operations
    match &req {
        ClientRequest::ContractOp(op) => match op {
            ContractRequest::Put {
                contract, state, ..
            } => {
                tracing::info!(
                    contract_key = %contract.key(),
                    state_size = state.as_ref().len(),
                    state_hash = %blake3::hash(state.as_ref()),
                    "Received PUT contract request"
                );
            }
            ContractRequest::Get {
                key,
                return_contract_code,
                ..
            } => {
                tracing::info!(
                    contract_key = %key,
                    return_code = return_contract_code,
                    "Received GET contract request"
                );
            }
            ContractRequest::Subscribe { key, summary } => {
                tracing::info!(
                    contract_key = %key,
                    has_summary = summary.is_some(),
                    "Received SUBSCRIBE contract request"
                );
            }
            ContractRequest::Update { key, data } => {
                tracing::info!(
                    contract_key = %key,
                    "Received UPDATE contract request"
                );
            }
            _ => tracing::debug!(
                operation_type = ?std::any::type_name_of_val(op),
                "Received contract operation request"
            ),
        },
        ClientRequest::DelegateOp(op) => {
            tracing::info!(
                delegate_type = ?std::any::type_name_of_val(op),
                "Received delegate operation request"
            );
        }
        ClientRequest::Disconnect { cause } => {
            tracing::info!(
                cause = cause.as_deref().unwrap_or("none"),
                "Received disconnect request"
            );
        }
        _ => tracing::debug!(
            request_type = ?std::any::type_name_of_val(&req),
            "Received client request"
        ),
    }
    // Forward the request to the server
    tracing::debug!("Forwarding request to server");
    match request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(req),
            auth_token: auth_token.clone(),
        })
        .await
    {
        Ok(_) => {
            tracing::debug!("Request successfully forwarded");
            Ok(None)
        },
        Err(err) => {
            tracing::error!(error = %err, "Failed to forward request to server");
            Err(Some(err.into()))
        }
    }
}

#[tracing::instrument(
    level = "debug",
    name = "process_host_response",
    skip(msg, tx),
    fields(client_id = %client_id, protocol = ?encoding_protoc)
)]
async fn process_host_response(
    msg: Option<HostCallbackResult>,
    client_id: ClientId,
    encoding_protoc: EncodingProtocol,
    tx: &mut SplitSink<WebSocket, Message>,
) -> anyhow::Result<Option<NewSubscription>> {
    match msg {
        Some(HostCallbackResult::Result { id, result }) => {
            if id != client_id {
                tracing::warn!(
                    expected_id = %client_id,
                    actual_id = %id,
                    "Client ID mismatch in response"
                );
            }
            
            let result = match result {
                Ok(res) => {
                    match &res {
                        HostResponse::ContractResponse(resp) => match resp {
                            ContractResponse::GetResponse {
                                key,
                                contract,
                                state,
                            } => {
                                tracing::info!(
                                    contract_key = %key,
                                    has_contract = contract.is_some(),
                                    state_size = state.as_ref().len(),
                                    state_hash = %blake3::hash(state.as_ref()),
                                    "Sending GET response"
                                );
                            }
                            ContractResponse::PutResponse { key } => {
                                tracing::info!(
                                    contract_key = %key,
                                    "Sending PUT response"
                                );
                            }
                            ContractResponse::UpdateResponse { key, summary } => {
                                tracing::info!(
                                    contract_key = %key,
                                    has_summary = !summary.is_empty(),
                                    "Sending UPDATE response"
                                );
                            }
                            ContractResponse::SubscribeResponse { key, subscribed } => {
                                tracing::info!(
                                    contract_key = %key,
                                    subscribed = subscribed,
                                    "Sending SUBSCRIBE response"
                                );
                            }
                            ContractResponse::UpdateNotification { key, .. } => {
                                tracing::info!(
                                    contract_key = %key,
                                    "Sending update notification"
                                );
                            }
                            _ => tracing::debug!(
                                response_type = ?std::any::type_name_of_val(resp),
                                "Sending contract response"
                            ),
                        },
                        HostResponse::DelegateResponse { key, values } => {
                            tracing::info!(
                                delegate_key = %key,
                                values_count = values.len(),
                                "Sending delegate response"
                            );
                        }
                        _ => tracing::debug!(
                            response_type = ?std::any::type_name_of_val(&res),
                            "Sending response"
                        ),
                    }
                    Ok(res)
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        error_kind = ?err.kind(),
                        "Sending error response"
                    );
                    Err(err)
                }
            };
            // Serialize the response based on protocol
            tracing::debug!("Serializing response");
            let serialized_res = match encoding_protoc {
                EncodingProtocol::Flatbuffers => {
                    match result {
                        Ok(res) => res.into_fbs_bytes(),
                        Err(err) => err.into_fbs_bytes(),
                    }
                },
                EncodingProtocol::Native => {
                    match bincode::serialize(&result) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            tracing::error!(error = %e, "Failed to serialize response with bincode");
                            return Err(e.into());
                        }
                    }
                },
            };
            
            // Send the response
            tracing::debug!(response_size = serialized_res.len(), "Sending response to client");
            match tx.send(Message::Binary(serialized_res)).await {
                Ok(_) => {
                    tracing::debug!("Response sent successfully");
                    Ok(None)
                },
                Err(e) => {
                    tracing::error!(error = %e, "Failed to send response to client");
                    Err(e.into())
                }
            }
        }
        Some(HostCallbackResult::SubscriptionChannel { key, id, callback }) => {
            if id != client_id {
                tracing::warn!(
                    expected_id = %client_id,
                    actual_id = %id,
                    "Client ID mismatch in subscription channel"
                );
            }
            
            tracing::info!(
                contract_key = %key,
                "Received subscription channel for contract"
            );
            Ok(Some(NewSubscription { key, callback }))
        }
        Some(HostCallbackResult::NewId { id: cli_id }) => {
            tracing::info!(client_id = %cli_id, "New client registered");
            Ok(None)
        }
        None => {
            tracing::error!("Node shut down while handling responses");
            
            // Create error response
            let error_response = Err::<HostResponse, ClientError>(
                ErrorKind::NodeUnavailable.into()
            );
            
            // Serialize and send error
            match bincode::serialize(&error_response) {
                Ok(bytes) => {
                    if let Err(e) = tx.send(Message::Binary(bytes)).await {
                        tracing::error!(error = %e, "Failed to send error response");
                    }
                },
                Err(e) => {
                    tracing::error!(error = %e, "Failed to serialize error response");
                }
            }
            
            // Send close frame
            if let Err(e) = tx.send(Message::Close(None)).await {
                tracing::error!(error = %e, "Failed to send close frame");
            }
            
            Err(anyhow::anyhow!(
                "Node shut down while handling responses for client {client_id}"
            ))
        }
    }
}

impl ClientEventsProxy for WebSocketProxy {
    #[tracing::instrument(
        level = "debug",
        name = "websocket_proxy_recv",
        skip(self),
        fields(active_connections = %self.response_channels.len())
    )]
    fn recv(&mut self) -> BoxFuture<Result<OpenRequest<'static>, ClientError>> {
        async move {
            tracing::trace!("Waiting for incoming client request");
            
            loop {
                match self.proxy_server_request.recv().await {
                    Some(msg) => {
                        tracing::debug!("Received client connection message");
                        match self.internal_proxy_recv(msg).await {
                            Ok(Some(reply)) => {
                                tracing::debug!("Processing client request");
                                break Ok(reply.into_owned());
                            }
                            Ok(None) => {
                                tracing::trace!("Connection setup message, continuing");
                                continue;
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "Error processing client message");
                                break Err(e);
                            }
                        }
                    }
                    None => {
                        tracing::warn!("WebSocket proxy channel closed");
                        break Err(ClientError::from(ErrorKind::ChannelClosed));
                    }
                }
            }
        }
        .boxed()
    }

    #[tracing::instrument(
        level = "debug",
        name = "websocket_proxy_send",
        skip(self, result),
        fields(
            client_id = %id,
            result_type = ?result.as_ref().map(|_| "success").unwrap_or("error"),
            active_connections = %self.response_channels.len()
        )
    )]
    fn send(
        &mut self,
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    ) -> BoxFuture<Result<(), ClientError>> {
        async move {
            if let Some(ch) = self.response_channels.remove(&id) {
                // Check if this is a disconnect error
                let should_disconnect = match &result {
                    Err(err) if matches!(err.kind(), ErrorKind::Disconnect) => {
                        tracing::info!(client_id = %id, "Client disconnecting");
                        true
                    },
                    Err(err) => {
                        tracing::debug!(
                            client_id = %id,
                            error = %err,
                            error_kind = ?err.kind(),
                            "Sending error response"
                        );
                        false
                    },
                    Ok(_) => {
                        tracing::trace!(client_id = %id, "Sending successful response");
                        false
                    }
                };
                
                // Send the result
                match ch.send(HostCallbackResult::Result { id, result }) {
                    Ok(_) if !should_disconnect => {
                        tracing::debug!(client_id = %id, "Connection still active, preserving channel");
                        self.response_channels.insert(id, ch);
                    },
                    Ok(_) => {
                        tracing::info!(client_id = %id, "Response sent, client disconnected");
                    },
                    Err(_) => {
                        tracing::info!(client_id = %id, "Failed to send response, client disconnected");
                    }
                }
            } else {
                tracing::warn!(client_id = %id, "Client not found in response channels");
            }
            Ok(())
        }
        .boxed()
    }
}
