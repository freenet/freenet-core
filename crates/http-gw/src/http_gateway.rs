use futures::{stream::SplitSink, SinkExt, StreamExt};

use locutus_node::*;
use locutus_runtime::{locutus_stdlib::web::controller::ControllerState, ContractKey};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
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

pub struct HttpGateway {
    server_request: mpsc::Receiver<ClientConnection>,
    response_channels: HashMap<ClientId, mpsc::UnboundedSender<HostCallbackResult>>,
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
                    tracing::info!(err = %err, "client channel error");
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
                    Ok(res) => tracing::info!(response = %res, cli_id = %client_id, "sending notification"),
                    Err(err) => tracing::info!(response = %err, cli_id = %client_id, "sending notification error"),
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
            tracing::info!(msg = ?m);
            return Ok(None);
        }
        Err(err) => return Err(Some(err.into())),
    };
    let req: ClientRequest = {
        match ClientRequest::decode_mp(msg) {
            Ok(r) => r,
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
    tracing::info!(req = %req, "received client request");
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
            let mut _wrapped_state = None; // avoids copying
            let result = match result {
                Ok(res) => {
                    tracing::info!(response = %res, cli_id = %id, "sending response");
                    match res {
                        HostResponse::GetResponse { contract, state } => {
                            _wrapped_state = Some(state);
                            let borrowed_state = _wrapped_state.as_ref().unwrap();
                            let inner_state = ControllerState::try_from(&**borrowed_state)?;
                            Ok(HostResponse::GetResponse {
                                contract,
                                state: inner_state.controller_data,
                            })
                        }
                        other => Ok(other.try_into()?),
                    }
                }
                Err(err) => {
                    tracing::info!(response = %err, cli_id = %id, "sending response error");
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
                req: ClientRequest::Subscribe { key },
            } => {
                // intercept subscription messages because they require a callback subscription channel
                let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
                if let Some(ch) = self.response_channels.get(&client_id) {
                    ch.send(HostCallbackResult::SubscriptionChannel {
                        key,
                        id: client_id,
                        callback: rx,
                    })
                    .map_err(|_| ErrorKind::ChannelClosed)?;
                    Ok(Some(
                        OpenRequest::new(client_id, ClientRequest::Subscribe { key })
                            .with_notification(tx),
                    ))
                } else {
                    log::warn!("client: {client_id} not found");
                    Err(ErrorKind::UnknownClient(client_id).into())
                }
            }
            ClientConnection::Request { client_id, req } => {
                // just forward the request to the node
                Ok(Some(OpenRequest::new(client_id, req)))
            }
        }
    }
}

#[allow(clippy::needless_lifetimes)]
impl ClientEventsProxy for HttpGateway {
    fn recv<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<OpenRequest, ClientError>> + Send + Sync + '_>> {
        Box::pin(async move {
            loop {
                let msg = self.server_request.recv().await;
                if let Some(msg) = msg {
                    if let Some(reply) = self.internal_proxy_recv(msg).await? {
                        break Ok(reply);
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
                if ch.send(HostCallbackResult::Result { id, result }).is_ok() && !should_rm {
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

#[test]
fn test_handle_update_request() -> Result<(), Box<dyn std::error::Error>> {
    let expected_client_request = ClientRequest::Update {
        key: ContractKey::from_spec("JAgVrRHt88YbBFjGQtBD3uEmRUFvZQqK7k8ypnJ8g6TC".to_string())
            .unwrap(),
        delta: locutus_runtime::StateDelta::from(vec![
            91, 10, 32, 32, 32, 32, 123, 10, 32, 32, 32, 32, 32, 32, 32, 32, 34, 97, 117, 116, 104,
            111, 114, 34, 58, 34, 73, 68, 71, 34, 44, 10, 32, 32, 32, 32, 32, 32, 32, 32, 34, 100,
            97, 116, 101, 34, 58, 34, 50, 48, 50, 50, 45, 48, 54, 45, 49, 53, 84, 48, 48, 58, 48,
            48, 58, 48, 48, 90, 34, 44, 10, 32, 32, 32, 32, 32, 32, 32, 32, 34, 116, 105, 116, 108,
            101, 34, 58, 34, 78, 101, 119, 32, 109, 115, 103, 34, 44, 10, 32, 32, 32, 32, 32, 32,
            32, 32, 34, 99, 111, 110, 116, 101, 110, 116, 34, 58, 34, 46, 46, 46, 34, 10, 32, 32,
            32, 32, 125, 10, 93, 10, 32, 32, 32, 32,
        ]),
    };
    let msg: Vec<u8> = vec![
        130, 163, 107, 101, 121, 130, 164, 115, 112, 101, 99, 196, 32, 255, 17, 144, 159, 194, 187,
        46, 33, 205, 77, 242, 70, 87, 18, 202, 62, 226, 149, 25, 151, 188, 167, 153, 197, 129, 25,
        179, 198, 218, 99, 159, 139, 168, 99, 111, 110, 116, 114, 97, 99, 116, 192, 165, 100, 101,
        108, 116, 97, 196, 134, 91, 10, 32, 32, 32, 32, 123, 10, 32, 32, 32, 32, 32, 32, 32, 32,
        34, 97, 117, 116, 104, 111, 114, 34, 58, 34, 73, 68, 71, 34, 44, 10, 32, 32, 32, 32, 32,
        32, 32, 32, 34, 100, 97, 116, 101, 34, 58, 34, 50, 48, 50, 50, 45, 48, 54, 45, 49, 53, 84,
        48, 48, 58, 48, 48, 58, 48, 48, 90, 34, 44, 10, 32, 32, 32, 32, 32, 32, 32, 32, 34, 116,
        105, 116, 108, 101, 34, 58, 34, 78, 101, 119, 32, 109, 115, 103, 34, 44, 10, 32, 32, 32,
        32, 32, 32, 32, 32, 34, 99, 111, 110, 116, 101, 110, 116, 34, 58, 34, 46, 46, 46, 34, 10,
        32, 32, 32, 32, 125, 10, 93, 10, 32, 32, 32, 32,
    ];
    let result_client_request = ClientRequest::decode_mp(msg);
    Ok(())
}

#[test]
fn test_handle_get_request() -> Result<(), Box<dyn std::error::Error>> {
    let expected_client_request = ClientRequest::Get {
        key: ContractKey::from_spec("JAgVrRHt88YbBFjGQtBD3uEmRUFvZQqK7k8ypnJ8g6TC".to_string())
            .unwrap(),
        fetch_contract: false,
    };
    let msg: Vec<u8> = vec![
        130, 163, 107, 101, 121, 130, 164, 115, 112, 101, 99, 196, 32, 255, 17, 144, 159, 194, 187,
        46, 33, 205, 77, 242, 70, 87, 18, 202, 62, 226, 149, 25, 151, 188, 167, 153, 197, 129, 25,
        179, 198, 218, 99, 159, 139, 168, 99, 111, 110, 116, 114, 97, 99, 116, 192, 174, 102, 101,
        116, 99, 104, 95, 99, 111, 110, 116, 114, 97, 99, 116, 194,
    ];
    let result_client_request = ClientRequest::decode_mp(msg);
    Ok(())
}
