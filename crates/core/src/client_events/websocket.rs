use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, OnceLock, RwLock},
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
use crate::server::http_gateway::AttestedContractMap;

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
    pub fn create_router(server_routing: Router) -> (Self, Router) {
        // Create a default empty attested contracts map
        let attested_contracts = Arc::new(RwLock::new(HashMap::<
            AuthToken,
            (ContractInstanceId, ClientId),
        >::new()));
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
    ) -> Result<Option<OpenRequest>, ClientError> {
        match msg {
            ClientConnection::NewConnection { callbacks, assigned_token } => {
                // is a new client, assign an id and open a channel to communicate responses from the node
                let cli_id = ClientId::next();
                tracing::debug!(client_id = %cli_id, ?assigned_token, "New client connection received by proxy");
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
                tracing::debug!(%client_id, request = %req, ?auth_token, "Client request received by proxy");
                let open_req = match &*req {
                    ClientRequest::ContractOp(ContractRequest::Subscribe { key, .. }) => {
                        tracing::debug!(%client_id, %key, "Subscription request intercepted");
                        // intercept subscription messages because they require a callback subscription channel
                        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                        if let Some(ch) = self.response_channels.get(&client_id) {
                            tracing::debug!(%client_id, %key, "Sending subscription channel back to client handler");
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
                            tracing::warn!(%client_id, "Client not found for subscription request");
                            return Err(ErrorKind::UnknownClient(client_id.into()).into());
                        }
                    }
                    _ => {
                        // just forward the request to the node
                        tracing::debug!(%client_id, request = %req, "Forwarding non-subscription request to node");
                        OpenRequest::new(client_id, req).with_token(auth_token)
                    }
                };
                Ok(Some(open_req))
            }
        }
    }
} // Added missing closing brace for impl WebSocketProxy

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
    tracing::debug!(?encoding_protoc, ?auth_token, "Inserting connection info into request extensions");
    req.extensions_mut().insert(encoding_protoc);
    req.extensions_mut().insert(auth_token.clone()); // Clone auth_token for insertion

    let response = next.run(req).await;
    tracing::debug!(?auth_token, "Finished connection_info middleware");
    response
}

async fn websocket_commands(
    ws: WebSocketUpgrade,
    Extension(auth_token): Extension<Option<AuthToken>>,
    Extension(encoding_protoc): Extension<EncodingProtocol>,
    Extension(rs): Extension<WebSocketRequest>,
    Extension(attested_contracts): Extension<AttestedContractMap>,
) -> Response {
    tracing::debug!(?auth_token, ?encoding_protoc, "Received WebSocket upgrade request");
    let on_upgrade = move |ws: WebSocket| async move {
        tracing::debug!(?auth_token, ?encoding_protoc, "Executing WebSocket on_upgrade callback");
        // Get the data we need and immediately drop the lock
        let auth_and_instance = if let Some(token) = auth_token.as_ref() {
            let attested_contracts_read = attested_contracts.read().unwrap();

            // Only collect and log map contents when trace is enabled
            if tracing::enabled!(tracing::Level::TRACE) {
                let map_contents: Vec<_> = attested_contracts_read.keys().cloned().collect();
                tracing::trace!(?token, "attested_contracts map keys: {:?}", map_contents);
            }

            if let Some((cid, _)) = attested_contracts_read.get(token) {
                tracing::trace!(?token, ?cid, "Found token in attested_contracts map");
                Some((token.clone(), *cid))
            } else {
                tracing::warn!(?token, "Auth token not found in attested_contracts map");
                None
            }
        } else {
            tracing::trace!("No auth token provided in WebSocket request");
            None
        }; // RwLockReadGuard is dropped here

        // Only evaluate auth_and_instance for trace when trace is enabled
        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::debug!(protoc = ?ws.protocol(), ?auth_and_instance, "WebSocket connection established");
        } else {
            tracing::debug!(protoc = ?ws.protocol(), "WebSocket connection established (no auth token)");
        }
        if let Err(error) =
            websocket_interface(rs.clone(), auth_and_instance, encoding_protoc, ws).await
        {
            tracing::error!(%error, "WebSocket interface handler finished with error");
        } else {
            tracing::debug!("WebSocket interface handler finished successfully");
        }
    };

    ws.on_upgrade(on_upgrade)
}

async fn websocket_interface(
    request_sender: WebSocketRequest,
    mut auth_token: Option<(AuthToken, ContractInstanceId)>,
    encoding_protoc: EncodingProtocol,
    ws: WebSocket,
) -> anyhow::Result<()> {
    tracing::debug!(?auth_token, ?encoding_protoc, "Starting websocket_interface");
    let (mut response_rx, client_id) =
        new_client_connection(&request_sender, auth_token.clone()).await?;
    tracing::debug!(%client_id, "New client connection established for websocket interface");
    let (mut server_sink, mut client_stream) = ws.split();
    let contract_updates: Arc<Mutex<VecDeque<(_, mpsc::UnboundedReceiver<HostResult>)>>> =
        Arc::new(Mutex::new(VecDeque::new()));

    loop {
        tracing::trace!(%client_id, "Waiting for next event in websocket_interface loop");
        let contract_updates_cp = contract_updates.clone();
        let listeners_task = async move {
            loop {
                tracing::trace!(%client_id, "Checking subscription listeners");
                let mut lock = contract_updates_cp.lock().await;
                let active_listeners = &mut *lock;
                for _ in 0..active_listeners.len() {
                    if let Some((key, mut listener)) = active_listeners.pop_front() {
                        match listener.try_recv() {
                            Ok(r) => {
                                tracing::debug!(%client_id, %key, "Received subscription update");
                                active_listeners.push_back((key, listener));
                                return Ok((key, r)); // Return key along with result
                            }
                            Err(mpsc::error::TryRecvError::Empty) => {
                                // No message, put it back and try next
                                active_listeners.push_back((key, listener));
                            }
                            Err(err @ mpsc::error::TryRecvError::Disconnected) => {
                                // Channel disconnected, don't put it back
                                tracing::debug!(%client_id, %key, err = ?err, "Subscription listener channel disconnected");
                                return Err(anyhow::anyhow!(err));
                            }
                        }
                    } else {
                        // Should not happen if len > 0, but break defensively
                        break;
                    }
                }
                std::mem::drop(lock);
                // Avoid busy-waiting if there are no active listeners or no messages
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };

        let client_req_task = async {
            tracing::trace!(%client_id, "Awaiting message from client stream");
            let next_msg = match client_stream
                .next()
                .await
            {
                Some(Ok(msg)) => {
                    tracing::debug!(%client_id, "Received message from client stream");
                    Ok(msg)
                },
                Some(Err(err)) => {
                    tracing::debug!(%client_id, err = %err, "Client stream error");
                    Err(Some(err.into()))
                }
                None => {
                    tracing::debug!(%client_id, "Client stream closed");
                    Err(None) // Indicate clean closure
                }
            };

            let msg = match next_msg {
                Ok(msg) => msg,
                Err(e) => return Err(e),
            };

            process_client_request(
                client_id,
                msg,
                &request_sender,
                auth_token.as_ref().map(|(token, _)| token.clone()), // Pass current token immutably
                encoding_protoc,
            )
            .await
        };

        tokio::select! { biased;
            // Prioritize receiving and processing responses from the host
            host_msg = response_rx.recv() => {
                tracing::trace!(%client_id, "Received message from host channel");
                match process_host_response(host_msg, client_id, encoding_protoc, &mut server_sink).await {
                    Ok(Some(NewSubscription { key, callback })) => {
                        tracing::debug!(%client_id, %key, "Adding new subscription listener");
                        let mut active_listeners_guard = contract_updates.lock().await;
                        active_listeners_guard.push_back((key, callback));
                        // Explicitly drop the guard before potentially blocking again
                        drop(active_listeners_guard);
                    }
                    Ok(None) => {
                        tracing::trace!(%client_id, "Successfully processed host response");
                    }
                    Err(e) => {
                        tracing::error!(%client_id, error = %e, "Error processing host response");
                        // Attempt to close the WebSocket connection gracefully on error
                        let _ = server_sink.close().await;
                        return Err(e); // Propagate the error to terminate the interface
                    }
                }
            }
            // Handle requests coming from the client connection
            client_req_result = client_req_task => {
                tracing::trace!(%client_id, "Received message from client task");
                match client_req_result {
                    Ok(Some(error_msg)) => {
                        // Send error message back to client (e.g., deserialization error)
                        tracing::debug!(%client_id, "Sending error message to client");
                        server_sink.send(error_msg).await.inspect_err(|err| {
                            tracing::debug!(%client_id, err = %err, "Error sending error message to client");
                        })?;
                    }
                    Ok(None) => {
                        // Successfully processed client request, continue loop
                        tracing::trace!(%client_id, "Successfully processed client request");
                        continue;
                    }
                    Err(None) => {
                        // Client closed connection cleanly
                        tracing::debug!(%client_id, "Client closed WebSocket connection");
                        let _ = server_sink.close().await;
                        return Ok(()); // Terminate the interface cleanly
                    },
                    Err(Some(err)) => {
                        // Error processing client request or stream error
                        tracing::error!(%client_id, err = %err, "Error processing client request/stream");
                        let _ = server_sink.close().await;
                        return Err(err); // Propagate the error
                    },
                }
            }
            // Handle updates/notifications from subscribed contracts
            listener_result = listeners_task => {
                tracing::trace!(%client_id, "Received message from listener task");
                let (key, response) = listener_result?;
                match &response {
                    Ok(res) => tracing::debug!(%client_id, %key, response_type = %res.type_name(), "Sending subscription notification"),
                    Err(err) => tracing::debug!(%client_id, %key, error = %err, "Sending subscription notification error"),
                }
                let serialized_res = match encoding_protoc {
                    EncodingProtocol::Flatbuffers => match response {
                        Ok(res) => res.into_fbs_bytes()?,
                        Err(err) => err.into_fbs_bytes()?,
                    },
                    EncodingProtocol::Native => bincode::serialize(&response)?,
                };
                tracing::debug!(%client_id, %key, bytes = serialized_res.len(), "Sending serialized subscription notification");
                server_sink.send(Message::Binary(serialized_res)).await.inspect_err(|err| {
                    tracing::debug!(%client_id, %key, err = %err, "Error sending subscription notification to client");
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
    tracing::debug!(?assigned_token, "Requesting new client connection from WebSocket proxy");
    request_sender
        .send(ClientConnection::NewConnection {
            callbacks: response_sender,
            assigned_token,
        })
        .await
        .map_err(|_| ErrorKind::NodeUnavailable)?;
    tracing::debug!("Waiting for client ID assignment from WebSocket proxy");
    match response_recv.recv().await {
        Some(HostCallbackResult::NewId { id: client_id, .. }) => {
            tracing::debug!(%client_id, "Received new client ID assignment");
            Ok((response_recv, client_id))
        }
        None => {
            tracing::error!("WebSocket proxy channel closed before assigning client ID");
            Err(ErrorKind::NodeUnavailable.into())
        }
        other => {
            tracing::error!(message = ?other, "Received unexpected message while waiting for client ID");
            // This indicates a logic error, but we'll treat it as NodeUnavailable for now
            Err(ErrorKind::NodeUnavailable.into())
        }
    }
}

struct NewSubscription {
    key: ContractKey,
    callback: mpsc::UnboundedReceiver<HostResult>,
}

async fn process_client_request(
    client_id: ClientId,
    msg: Result<Message, axum::Error>,
    request_sender: &mpsc::Sender<ClientConnection>,
    current_auth_token: Option<AuthToken>, // Renamed and made immutable
    encoding_protoc: EncodingProtocol,
) -> Result<Option<Message>, Option<anyhow::Error>> {
    tracing::trace!(%client_id, ?encoding_protoc, "Processing client message");
    let msg_bytes = match msg {
        Ok(Message::Binary(data)) => {
            tracing::debug!(%client_id, bytes = data.len(), "Received Binary message");
            data
        }
        Ok(Message::Text(data)) => {
            tracing::debug!(%client_id, bytes = data.len(), "Received Text message");
            data.into_bytes()
        }
        Ok(Message::Close(frame)) => {
            tracing::debug!(%client_id, ?frame, "Received Close message");
            return Err(None); // Indicate clean closure
        }
        Ok(Message::Ping(ping)) => {
            tracing::debug!(%client_id, bytes = ping.len(), "Received Ping message");
            return Ok(Some(Message::Pong(ping))); // Respond with Pong
        }
        Ok(Message::Pong(pong)) => {
             tracing::debug!(%client_id, bytes = pong.len(), "Received Pong message (ignoring)");
             return Ok(None); // Ignore Pong
        }
        Err(err) => {
            tracing::error!(%client_id, %err, "WebSocket receive error");
            return Err(Some(err.into())); // Indicate error
        }
    };

    // Try to deserialize the ClientRequest message
    tracing::trace!(%client_id, "Attempting to deserialize client request");
    let req = {
        match encoding_protoc {
            EncodingProtocol::Flatbuffers => match ClientRequest::try_decode_fbs(&msg_bytes) {
                Ok(decoded) => {
                    tracing::debug!(%client_id, request_type = %decoded.type_name(), "Successfully deserialized Flatbuffers request");
                    decoded.into_owned()
                },
                Err(err) => {
                    tracing::warn!(%client_id, %err, "Failed to deserialize Flatbuffers request");
                    return Ok(Some(Message::Binary(err.into_fbs_bytes())));
                }
            },
            EncodingProtocol::Native => match bincode::deserialize::<ClientRequest>(&msg_bytes) {
                Ok(decoded) => {
                    tracing::debug!(%client_id, request_type = %decoded.type_name(), "Successfully deserialized Native request");
                    decoded.into_owned()
                },
                Err(err) => {
                    tracing::warn!(%client_id, %err, "Failed to deserialize Native request");
                    let client_err = ErrorKind::DeserializationError {
                        cause: format!("{err}").into(),
                    }.into();
                    let result_error = bincode::serialize(&Err::<HostResponse, ClientError>(client_err))
                        .map_err(|e| {
                            tracing::error!(%client_id, err = %e, "Failed to serialize deserialization error response");
                            Some(e.into())
                        })?;
                    return Ok(Some(Message::Binary(result_error)));
                }
            },
        }
    };

    // Removed the logic that updated auth_token based on Authenticate message.
    // The token state is managed in websocket_interface.

    tracing::debug!(%client_id, request = %req, ?current_auth_token, "Sending client request to proxy");
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(req),
            auth_token: current_auth_token.clone(), // Use the passed-in token
        })
        .await
        .map_err(|err| {
            tracing::error!(%client_id, %err, "Failed to send client request to proxy channel");
            Some(err.into())
        })?;
    Ok(None)
}

async fn process_host_response(
    msg: Option<HostCallbackResult>,
    client_id: ClientId,
    encoding_protoc: EncodingProtocol,
    tx: &mut SplitSink<WebSocket, Message>,
) -> anyhow::Result<Option<NewSubscription>> {
    tracing::debug!(%client_id, ?encoding_protoc, message_type = msg.as_ref().map(|m| m.variant_name()), "Processing host response");
    match msg {
        Some(HostCallbackResult::Result { id, result }) => {
            debug_assert_eq!(id, client_id, "Mismatched client ID in host response");
            let (response_type, is_error) = match &result {
                Ok(res) => (res.type_name(), false),
                Err(err) => (err.kind().variant_name(), true),
            };
            tracing::debug!(%client_id, %response_type, is_error, "Preparing to send host result to client");

            // Log specific response types if needed, e.g., ApplicationMessages
            if let Ok(HostResponse::ApplicationMessages { .. }) = &result {
                 tracing::debug!(%client_id, "Processing ApplicationMessages response");
            }

            let result_log = match result {
                Ok(res) => {
                    // Avoid logging potentially large state data in GetResponse
                    if let HostResponse::ContractResponse(ContractResponse::GetResponse { key, contract: _, state: _ }) = &res {
                        tracing::debug!(response = %format!("GetResponse(key={key})"), cli_id = %id, "Sending GetResponse");
                        Ok(ContractResponse::GetResponse { key: *key, contract: res.unwrap_get_response_contract().cloned(), state: res.unwrap_get_response_state().cloned() }.into())
                    } else {
                        tracing::debug!(response = %res, cli_id = %id, "Sending response");
                        Ok(res)
                    }
                }
                Err(err) => {
                    tracing::debug!(error = %err, cli_id = %id, "Sending error response");
                    Err(err)
                }
            };

            let serialized_res = match encoding_protoc {
                EncodingProtocol::Flatbuffers => match result_log {
                    Ok(res) => res.into_fbs_bytes()?,
                    Err(err) => err.into_fbs_bytes()?,
                },
                EncodingProtocol::Native => bincode::serialize(&result_log)?,
            };
            tracing::debug!(%client_id, bytes = serialized_res.len(), "Sending serialized host result to client");
            tx.send(Message::Binary(serialized_res)).await.inspect_err(|e| tracing::error!(%client_id, error=%e, "Failed sending host result"))?;
            Ok(None)
        }
        Some(HostCallbackResult::SubscriptionChannel { key, id, callback }) => {
            debug_assert_eq!(id, client_id, "Mismatched client ID in subscription channel");
            tracing::debug!(%client_id, %key, "Received subscription channel from host");
            Ok(Some(NewSubscription { key, callback }))
        }
        Some(HostCallbackResult::NewId { id: assigned_id }) => {
            // This case should ideally be handled only during initial connection setup,
            // but logging it here defensively.
            tracing::warn!(%client_id, assigned_id = %assigned_id, "Received unexpected NewId message during active session");
            Ok(None)
        }
        None => {
            tracing::warn!(%client_id, "Host response channel closed (node unavailable)");
            // Attempt to serialize and send a NodeUnavailable error before closing
            let node_unavailable_err = Err::<HostResponse, ClientError>(ErrorKind::NodeUnavailable.into());
            let serialized_err = match encoding_protoc {
                EncodingProtocol::Flatbuffers => node_unavailable_err.clone().into_fbs_bytes()?,
                EncodingProtocol::Native => bincode::serialize(&node_unavailable_err)?,
            };
            tx.send(Message::Binary(serialized_err)).await.inspect_err(|e| tracing::error!(%client_id, error=%e, "Failed sending NodeUnavailable error"))?;
            tx.close().await.inspect_err(|e| tracing::error!(%client_id, error=%e, "Failed closing WebSocket sink"))?;
            Err(anyhow::anyhow!("Host response channel closed for client {client_id}"))
        }
    }
}

// Helper to get variant name for logging
impl HostCallbackResult {
    fn variant_name(&self) -> &'static str {
        match self {
            HostCallbackResult::Result { .. } => "Result",
            HostCallbackResult::SubscriptionChannel { .. } => "SubscriptionChannel",
            HostCallbackResult::NewId { .. } => "NewId",
        }
    }
}


impl ClientEventsProxy for WebSocketProxy {
    fn recv(&mut self) -> BoxFuture<Result<OpenRequest<'static>, ClientError>> {
        async move {
            loop {
                tracing::trace!("WebSocketProxy waiting for message from server request channel");
                let msg = self.proxy_server_request.recv().await;
                if let Some(msg) = msg {
                    match self.internal_proxy_recv(msg).await {
                        Ok(Some(reply)) => {
                            tracing::debug!(client_id = %reply.id, request = %reply.request, "WebSocketProxy received OpenRequest");
                            break Ok(reply.into_owned());
                        }
                        Ok(None) => {
                            tracing::trace!("WebSocketProxy processed internal message (e.g., new connection), continuing loop");
                            continue; // Processed internally (e.g. new connection), wait for next
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "Error processing message in WebSocketProxy");
                            break Err(e);
                        }
                    }
                } else {
                    tracing::warn!("WebSocketProxy server request channel closed");
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
    ) -> BoxFuture<Result<(), ClientError>> {
        async move {
            let (response_type, is_error) = match &result {
                Ok(res) => (res.type_name(), false),
                Err(err) => (err.kind().variant_name(), true),
            };
            tracing::debug!(client_id = %id, %response_type, is_error, "WebSocketProxy sending result to client handler");

            if let Some(ch) = self.response_channels.get(&id) {
                let should_rm = result
                    .as_ref()
                    .map_err(|err| matches!(err.kind(), ErrorKind::Disconnect))
                    .err()
                    .unwrap_or(false);

                if let Err(e) = ch.send(HostCallbackResult::Result { id, result }) {
                    tracing::warn!(client_id = %id, error = %e, "Failed to send result to client handler channel, removing channel");
                    // Error sending means the receiver (websocket_interface) is gone. Remove the channel.
                    self.response_channels.remove(&id);
                } else if should_rm {
                    tracing::debug!(client_id = %id, "Removing client channel due to Disconnect error");
                    self.response_channels.remove(&id);
                } else {
                    // Successfully sent and not a disconnect error, keep the channel.
                    tracing::trace!(client_id = %id, "Successfully sent result to client handler");
                }
            } else {
                tracing::warn!(client_id = %id, %response_type, "Client channel not found for sending result");
            }
            Ok(())
        }
        .boxed()
    }
}

// Extension trait for cleaner logging
trait TypeName {
    fn type_name(&self) -> &'static str;
}

impl TypeName for HostResponse {
    fn type_name(&self) -> &'static str {
        match self {
            HostResponse::ContractResponse(_) => "ContractResponse",
            HostResponse::DelegateResponse(_) => "DelegateResponse",
            HostResponse::ApplicationMessages { .. } => "ApplicationMessages",
            HostResponse::Ok => "Ok",
        }
    }
}

impl TypeName for ClientRequest<'_> {
     fn type_name(&self) -> &'static str {
         match self {
            ClientRequest::ContractOp(_) => "ContractOp",
            ClientRequest::DelegateOp(_) => "DelegateOp",
            ClientRequest::Authenticate{..} => "Authenticate",
            ClientRequest::Disconnect => "Disconnect",
         }
     }
}
