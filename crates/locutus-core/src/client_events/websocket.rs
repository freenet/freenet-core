use std::{
    collections::HashMap,
    error::Error,
    future::Future,
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
};

use futures::{future::BoxFuture, stream::SplitSink, SinkExt, StreamExt};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use warp::{filters::BoxedFilter, Filter, Reply};

use super::{ClientError, ClientEventsProxy, ClientId, ErrorKind, HostResult, OpenRequest};
use crate::{ClientRequest, HostResponse};

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
        server_config: BoxedFilter<(impl Reply + 'static,)>,
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
        let filter = warp::filters::path::end().map(warp::reply::reply).boxed();
        Self::start_server_internal(socket.into(), filter)
    }

    async fn start_server_internal(
        socket: SocketAddr,
        filter: BoxedFilter<(impl Reply + 'static,)>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        let (request_sender, server_request) = channel(PARALLELISM);
        let (server_response, response_receiver) = channel(PARALLELISM);
        let (new_client_up, new_clients) = channel(PARALLELISM);

        let server = serve(request_sender, new_client_up, socket, filter);
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
    server_config: BoxedFilter<(impl Reply + 'static,)>,
) {
    let req_channel = warp::any().map(move || (request_sender.clone(), new_responses.clone()));
    let request_receiver = server_config.or(warp::path("ws-api")
        .and(warp::ws())
        .and(req_channel)
        .map(|ws: warp::ws::Ws, (request_sender, new_responses)| {
            ws.on_upgrade(move |socket| handle_socket(socket, request_sender, new_responses))
        })
        .with(warp::trace::request()));
    warp::serve(request_receiver).run(socket).await;
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
                                log::error!("Tried to send an a response to an unregistered client");
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

async fn handle_socket(
    socket: warp::ws::WebSocket,
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
        let _ = client_tx.send(warp::ws::Message::binary(vec![])).await;
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
    result: Option<Result<warp::ws::Message, warp::Error>>,
) -> Result<(), ()> {
    let msg = match result {
        Some(Ok(msg)) if msg.is_binary() => {
            let data = msg.into_bytes();
            let deserialized: ClientRequest = match ClientRequest::decode_mp(&*data) {
                Ok(m) => m,
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
        Some(Ok(_)) => return Ok(()),
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
    response_stream: &mut SplitSink<warp::ws::WebSocket, warp::ws::Message>,
    response: Result<HostResponse, ClientError>,
) -> Result<(), Box<dyn std::error::Error>> {
    let serialize = rmp_serde::to_vec(&response).unwrap();
    response_stream
        .send(warp::ws::Message::binary(serialize))
        .await?;
    Ok(())
}
