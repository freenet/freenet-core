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

async fn websocket_commands(
    ws: WebSocketUpgrade,
    Extension(auth_token): Extension<Option<AuthToken>>,
    Extension(encoding_protoc): Extension<EncodingProtocol>,
    Extension(rs): Extension<WebSocketRequest>,
) -> axum::response::Response {
    let on_upgrade = move |ws: WebSocket| async move {
        tracing::debug!(protoc = ?ws.protocol(), "websocket connection established");
        match websocket_interface(rs.clone(), auth_token, encoding_protoc, ws).await {
            Ok(_) => {
                tracing::debug!("WebSocket interface completed normally");
            }
            Err(error) => {
                tracing::error!("WebSocket interface error: {}", error);
                if let Some(source) = error.source() {
                    tracing::error!("Caused by: {}", source);
                }
            }
        }
    };
    ws.on_upgrade(on_upgrade)
}

async fn websocket_interface(
    request_sender: WebSocketRequest,
    mut auth_token: Option<AuthToken>,
    encoding_protoc: EncodingProtocol,
    ws: WebSocket,
) -> anyhow::Result<()> {
    let (mut response_rx, client_id) = new_client_connection(&request_sender).await?;
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

async fn new_client_connection(
    request_sender: &WebSocketRequest,
) -> Result<(mpsc::UnboundedReceiver<HostCallbackResult>, ClientId), ClientError> {
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send(ClientConnection::NewConnection {
            callbacks: response_sender,
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

struct NewSubscription {
    key: ContractKey,
    callback: mpsc::UnboundedReceiver<HostResult>,
}

async fn process_client_request(
    client_id: ClientId,
    msg: Result<Message, axum::Error>,
    request_sender: &mpsc::Sender<ClientConnection>,
    auth_token: &mut Option<AuthToken>,
    encoding_protoc: EncodingProtocol,
) -> Result<Option<Message>, Option<anyhow::Error>> {
    let msg = match msg {
        Ok(Message::Binary(data)) => {
            tracing::debug!(size = data.len(), "received binary message");
            data
        }
        Ok(Message::Text(data)) => {
            tracing::debug!(size = data.len(), "received text message");
            data.into_bytes()
        }
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
                Ok(decoded) => {
                    tracing::debug!(protocol = "flatbuffers", "successfully decoded client request");
                    decoded.into_owned()
                }
                Err(err) => {
                    tracing::error!(error = %err, protocol = "flatbuffers", "failed to decode client request");
                    return Ok(Some(Message::Binary(err.into_fbs_bytes())))
                }
            },
            EncodingProtocol::Native => match bincode::deserialize::<ClientRequest>(&msg) {
                Ok(decoded) => {
                    tracing::debug!(protocol = "native", "successfully decoded client request");
                    decoded.into_owned()
                }
                Err(err) => {
                    tracing::error!(error = %err, protocol = "native", "failed to decode client request");
                    let result_error = bincode::serialize(&Err::<HostResponse, ClientError>(
                        ErrorKind::DeserializationError {
                            cause: format!("{err}").into(),
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
        tracing::debug!("client authenticated");
    }

    // Log specific details for contract operations
    match &req {
        ClientRequest::ContractOp(op) => {
            match op {
                ContractRequest::Put { contract, state, .. } => {
                    tracing::debug!(
                        contract_key = %contract.key(),
                        state_size = state.as_ref().len(),
                        "received PUT contract request"
                    );
                }
                ContractRequest::Get { key, return_contract_code } => {
                    tracing::debug!(
                        contract_key = %key,
                        return_code = return_contract_code,
                        "received GET contract request"
                    );
                }
                _ => tracing::debug!(operation = ?op, "received contract operation request"),
            }
        }
        _ => tracing::debug!(req = %req, "received client request"),
    }
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
) -> anyhow::Result<Option<NewSubscription>> {
    match msg {
        Some(HostCallbackResult::Result { id, result }) => {
            debug_assert_eq!(id, client_id);
            let result = match result {
                Ok(res) => {
                    match &res {
                        HostResponse::ContractResponse(resp) => {
                            match resp {
                                ContractResponse::GetResponse { key, contract, state } => {
                                    tracing::debug!(
                                        contract_key = %key,
                                        has_contract = contract.is_some(),
                                        state_size = state.as_ref().len(),
                                        "sending GET response"
                                    );
                                }
                                ContractResponse::PutResponse { key } => {
                                    tracing::debug!(
                                        contract_key = %key,
                                        "sending PUT response"
                                    );
                                }
                                _ => tracing::debug!(response = ?resp, "sending contract response"),
                            }
                        }
                        _ => tracing::debug!(response = %res, "sending response"),
                    }
                    Ok(res)
                }
                Err(err) => {
                    tracing::debug!(response = %err, cli_id = %id, "sending response error");
                    Err(err)
                }
            };
            let serialized_res = match encoding_protoc {
                EncodingProtocol::Flatbuffers => match result {
                    Ok(res) => res.into_fbs_bytes()?,
                    Err(err) => err.into_fbs_bytes()?,
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
            Err(anyhow::anyhow!(
                "node shut down while handling responses for {client_id}"
            ))
        }
    }
}

impl ClientEventsProxy for WebSocketProxy {
    fn recv(&mut self) -> BoxFuture<Result<OpenRequest<'static>, ClientError>> {
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
