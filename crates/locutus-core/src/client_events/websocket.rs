use axum::extract::ws::{Message, WebSocket};
use axum::extract::WebSocketUpgrade;
use axum::{Extension, Router};
use std::{
    collections::HashMap,
    error::Error,
    future::Future,
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
};
use tower_http::trace::TraceLayer;

use axum::routing::get;
use futures::{future::BoxFuture, stream::SplitSink, SinkExt, StreamExt};
use locutus_runtime::prelude::TryFromTsStd;
use locutus_stdlib::client_api::{ClientRequest, ContractRequest, ErrorKind, HostResponse};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use super::{ClientError, ClientEventsProxy, ClientId, HostResult, OpenRequest};

const PARALLELISM: usize = 10; // TODO: get this from config, or whatever optimal way

pub struct WebSocketProxy {
    server_request: Receiver<StaticOpenRequest>,
    server_response: Sender<(ClientId, HostResult)>,
}

type NewResponseSender = Sender<Result<HostResponse, ClientError>>;

impl WebSocketProxy {
    /// Starts this as an upgrade to an existing HTTP connection at the `/ws-api` URL
    pub fn as_upgrade<T>(
        socket: T,
        server_config: Router,
    ) -> impl Future<Output = Result<Self, Box<dyn Error + Send + Sync + 'static>>>
    where
        T: Into<SocketAddr>,
    {
        Self::start_server_internal(socket.into(), server_config)
    }

    /// Starts the websocket connection at the default `/ws-api` URL
    pub fn start_server<T>(
        socket: T,
    ) -> impl Future<Output = Result<Self, Box<dyn Error + Send + Sync + 'static>>>
    where
        T: Into<SocketAddr>,
    {
        let router = Router::default();
        Self::start_server_internal(socket.into(), router)
    }

    async fn start_server_internal(
        socket: SocketAddr,
        router: Router,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        let (request_sender, server_request) = channel(PARALLELISM);
        let (server_response, response_receiver) = channel(PARALLELISM);
        let (new_client_up, new_clients) = channel(PARALLELISM);

        let server = serve(request_sender, new_client_up, socket, router);
        tokio::spawn(server);
        tokio::spawn(responses(new_clients, response_receiver));

        Ok(Self {
            server_request,
            server_response,
        })
    }
}

// work around for rustc issue #64552
struct StaticOpenRequest(OpenRequest<'static>);

impl From<OpenRequest<'static>> for StaticOpenRequest {
    fn from(val: OpenRequest<'static>) -> Self {
        StaticOpenRequest(val)
    }
}

impl ClientEventsProxy for WebSocketProxy {
    fn recv(&mut self) -> BoxFuture<Result<OpenRequest<'static>, ClientError>> {
        Box::pin(async move {
            let req = self
                .server_request
                .recv()
                .await
                .map(|r| r.0)
                .ok_or(ErrorKind::ChannelClosed)?;
            Ok(req)
        })
    }

    fn send(
        &mut self,
        client: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> BoxFuture<Result<(), ClientError>> {
        Box::pin(async move {
            self.server_response
                .send((client, response))
                .await
                .map_err(|_| ErrorKind::ChannelClosed)?;
            Ok(())
        })
    }
}

async fn serve(
    request_sender: Sender<StaticOpenRequest>,
    new_responses: Sender<ClientHandling>,
    socket: SocketAddr,
    server_config: Router,
) {
    let (req_sender, new_res) = (request_sender.clone(), new_responses.clone());
    let request_receiver = server_config
        .route("/ws-api", get(ws_api_handler))
        .layer(Extension(req_sender))
        .layer(Extension(new_res))
        .layer(TraceLayer::new_for_http());
    axum::Server::bind(&socket)
        .serve(request_receiver.into_make_service())
        .await
        .unwrap();
}

enum ClientHandling {
    NewClient(ClientId, NewResponseSender),
    ClientDisconnected(ClientId),
}

async fn responses(
    mut client_handler: Receiver<ClientHandling>,
    mut response_receiver: Receiver<(ClientId, HostResult)>,
) {
    let mut clients = HashMap::new();
    loop {
        tokio::select! {
            new_client = client_handler.recv() => {
                match new_client {
                    Some(ClientHandling::NewClient(client_id, responses)) => {
                        clients.insert(client_id, responses);
                    }
                    Some(ClientHandling::ClientDisconnected(client_id)) => {
                        clients.remove(&client_id);
                    }
                    None => return,
                }
            }
            host_result = response_receiver.recv() => {
                match host_result {
                    Some((client_id, response)) => {
                        if let Some(ch) = clients.get_mut(&client_id) {
                            if Sender::send(ch, response).await.is_err() {
                                tracing::error!("Tried to send an a response to an unregistered client");
                                return;
                            }
                        } else {
                           return;
                        }
                    }
                    None => return,
                }
            }
        }
    }
}

static CLIENT_ID: AtomicUsize = AtomicUsize::new(0);

async fn ws_api_handler(
    ws: WebSocketUpgrade,
    Extension(request_sender): Extension<Sender<StaticOpenRequest>>,
    Extension(client_sender): Extension<Sender<ClientHandling>>,
) -> axum::response::Response {
    ws.on_upgrade(|socket| handle_socket(socket, request_sender, client_sender))
}

async fn handle_socket(
    socket: WebSocket,
    request_sender: Sender<StaticOpenRequest>,
    client_handler: Sender<ClientHandling>,
) {
    let client_id = ClientId(CLIENT_ID.fetch_add(1, Ordering::SeqCst));
    let (mut client_tx, mut client_rx) = socket.split();
    let (rx, mut host_responses) = channel(1);
    if client_handler
        .send(ClientHandling::NewClient(client_id, rx))
        .await
        .is_err()
    {
        let _ = client_tx.send(Message::Binary(vec![])).await;
        return;
    }
    loop {
        tokio::select! {
            result = client_rx.next() => {
                if new_request(&request_sender, client_id, result).await.is_err() {
                    break;
                }
            }
            response = host_responses.recv() => {
                let send_err = send_reponse_to_client(&mut client_tx, response.unwrap()).await.is_err();
                if send_err && client_handler.send(ClientHandling::ClientDisconnected(client_id)).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn new_request(
    request_sender: &Sender<StaticOpenRequest>,
    id: ClientId,
    result: Option<Result<Message, axum::Error>>,
) -> Result<(), ()> {
    let msg = match result {
        Some(Ok(msg)) => {
            let data = msg.into_data();
            let deserialized: ClientRequest = match ContractRequest::try_decode(&data) {
                Ok(m) => m.into(),
                Err(e) => {
                    let _ = request_sender
                        .send(
                            OpenRequest {
                                id,
                                request: ClientRequest::Disconnect {
                                    cause: Some(format!("{e}")),
                                },
                                notification_channel: None,
                            }
                            .into(),
                        )
                        .await;
                    return Ok(());
                }
            };
            deserialized
        }
        Some(Err(e)) => {
            let _ = request_sender
                .send(
                    OpenRequest {
                        id,
                        request: ClientRequest::Disconnect {
                            cause: Some(format!("{e}")),
                        },
                        notification_channel: None,
                    }
                    .into(),
                )
                .await;
            return Err(());
        }
        None => return Err(()),
    };
    if request_sender
        .send(
            OpenRequest {
                id,
                request: msg,
                notification_channel: None,
            }
            .into(),
        )
        .await
        .is_err()
    {
        return Err(());
    }
    Ok(())
}

async fn send_reponse_to_client(
    response_stream: &mut SplitSink<WebSocket, Message>,
    response: Result<HostResponse, ClientError>,
) -> Result<(), Box<dyn std::error::Error>> {
    let serialize = rmp_serde::to_vec(&response).unwrap();
    response_stream.send(Message::Binary(serialize)).await?;
    Ok(())
}
