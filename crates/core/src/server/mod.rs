pub(crate) mod errors;
mod http_gateway;
pub(crate) mod path_handlers;

use freenet_stdlib::{
    client_api::{ClientError, ClientRequest, HostResponse},
    prelude::*,
};

use crate::client_events::{AuthToken, ClientId, HostResult};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ClientConnection {
    NewConnection {
        notifications: tokio::sync::mpsc::UnboundedSender<HostCallbackResult>,
        assigned_token: Option<(AuthToken, ContractInstanceId)>,
    },
    Request {
        client_id: ClientId,
        req: Box<ClientRequest<'static>>,
        auth_token: Option<AuthToken>,
    },
}

#[derive(Debug)]
pub(crate) enum HostCallbackResult {
    NewId {
        id: ClientId,
    },
    Result {
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    },
    SubscriptionChannel {
        id: ClientId,
        key: ContractKey,
        callback: tokio::sync::mpsc::UnboundedReceiver<HostResult>,
    },
}

pub mod local_node {
    use std::net::{IpAddr, SocketAddr};

    use axum::Router;
    use freenet_stdlib::client_api::{ClientRequest, ErrorKind};
    use tokio::sync::mpsc;
    use tower_http::trace::TraceLayer;

    use crate::{
        client_events::{websocket::WebSocketProxy, ClientEventsProxy, OpenRequest},
        contract::{Executor, ExecutorError},
        DynError,
    };

    use super::http_gateway::HttpGateway;

    const PARALLELISM: usize = 10; // TODO: get this from config, or whatever optimal way

    async fn serve(socket: SocketAddr, router: Router) {
        tracing::info!("listening on {}", socket);
        axum::Server::bind(&socket)
            .serve(router.into_make_service())
            .await
            .expect("Server unable to start");
    }

    pub async fn run_local_node(
        mut executor: Executor,
        socket: SocketAddr,
    ) -> Result<(), DynError> {
        match socket.ip() {
            IpAddr::V4(ip) if !ip.is_loopback() => {
                return Err(format!("invalid ip: {ip}, expecting localhost").into())
            }
            IpAddr::V6(ip) if !ip.is_loopback() => {
                return Err(format!("invalid ip: {ip}, expecting localhost").into())
            }
            _ => {}
        }
        let (proxy_request_sender, request_to_server) = mpsc::channel(PARALLELISM);
        let router = HttpGateway::as_router(&socket);
        let (mut ws_proxy, router) =
            WebSocketProxy::as_router(router, request_to_server, proxy_request_sender);

        // FIXME: use combinator
        // let mut all_clients =
        //    ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        serve(socket, router.layer(TraceLayer::new_for_http())).await;

        loop {
            let OpenRequest {
                id,
                request,
                notification_channel,
                token,
                ..
            } = ws_proxy.recv().await?;
            tracing::trace!(cli_id = %id, "got request -> {request}");

            let res = match *request {
                ClientRequest::ContractOp(op) => {
                    executor
                        .contract_requests(op, id, notification_channel)
                        .await
                }
                ClientRequest::DelegateOp(op) => {
                    let attested_contract = token
                        .and_then(|token| ws_proxy.attested_contracts.get(&token).map(|(t, _)| t));
                    executor.delegate_request(op, attested_contract)
                }
                ClientRequest::Disconnect { cause } => {
                    if let Some(cause) = cause {
                        tracing::info!("disconnecting cause: {cause}");
                    }
                    // todo: token must live for a bit to allow reconnections
                    if let Some(rm_token) = ws_proxy
                        .attested_contracts
                        .iter()
                        .find_map(|(k, (_, eid))| (eid == &id).then(|| k.clone()))
                    {
                        ws_proxy.attested_contracts.remove(&rm_token);
                    }
                    continue;
                }
                _ => Err(ExecutorError::other("not supported")),
            };

            match res {
                Ok(res) => {
                    ws_proxy.send(id, Ok(res)).await?;
                }
                Err(err) => {
                    tracing::error!("{err}");
                    ws_proxy
                        .send(
                            id,
                            Err(ErrorKind::Unhandled {
                                cause: format!("{err}"),
                            }
                            .into()),
                        )
                        .await?;
                }
            }
        }
    }
}
