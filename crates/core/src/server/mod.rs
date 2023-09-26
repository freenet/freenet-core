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
        callbacks: tokio::sync::mpsc::UnboundedSender<HostCallbackResult>,
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
    use futures::TryFutureExt;
    use tower_http::trace::TraceLayer;

    use crate::{
        client_events::{websocket::WebSocketProxy, ClientEventsProxy, OpenRequest},
        contract::{Executor, ExecutorError},
        DynError,
    };

    use super::http_gateway::HttpGateway;

    fn serve(socket: SocketAddr, router: Router) {
        tracing::info!("listening on {}", socket);
        tokio::spawn(
            axum::Server::bind(&socket)
                .serve(router.into_make_service())
                .map_err(|e| {
                    tracing::error!("error while running HTTP gateway server: {e}");
                }),
        );
    }

    pub async fn run_local_node(
        mut executor: Executor,
        socket: SocketAddr,
    ) -> Result<(), DynError> {
        crate::config::Config::set_op_mode(crate::local_node::OperationMode::Local);
        match socket.ip() {
            IpAddr::V4(ip) if !ip.is_loopback() => {
                return Err(format!("invalid ip: {ip}, expecting localhost").into())
            }
            IpAddr::V6(ip) if !ip.is_loopback() => {
                return Err(format!("invalid ip: {ip}, expecting localhost").into())
            }
            _ => {}
        }
        let (mut gw, gw_router) = HttpGateway::as_router(&socket);
        let (mut ws_proxy, ws_router) = WebSocketProxy::as_router(gw_router);

        serve(socket, ws_router.layer(TraceLayer::new_for_http()));

        // FIXME: use combinator instead
        // let mut all_clients =
        //    ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        enum Receiver {
            Ws,
            Gw,
        }
        let mut receiver;
        loop {
            let req = tokio::select! {
                req = ws_proxy.recv() => {
                    receiver = Receiver::Ws;
                    req?
                }
                req = gw.recv() => {
                    receiver = Receiver::Gw;
                    req?
                }
            };
            let OpenRequest {
                client_id: id,
                request,
                notification_channel,
                token,
                ..
            } = req;
            tracing::trace!(cli_id = %id, "got request -> {request}");

            let res = match *request {
                ClientRequest::ContractOp(op) => {
                    executor
                        .contract_requests(op, id, notification_channel)
                        .await
                }
                ClientRequest::DelegateOp(op) => {
                    let attested_contract =
                        token.and_then(|token| gw.attested_contracts.get(&token).map(|(t, _)| t));
                    executor.delegate_request(op, attested_contract)
                }
                ClientRequest::Disconnect { cause } => {
                    if let Some(cause) = cause {
                        tracing::info!("disconnecting cause: {cause}");
                    }
                    // todo: token must live for a bit to allow reconnections
                    if let Some(rm_token) = gw
                        .attested_contracts
                        .iter()
                        .find_map(|(k, (_, eid))| (eid == &id).then(|| k.clone()))
                    {
                        gw.attested_contracts.remove(&rm_token);
                    }
                    continue;
                }
                _ => Err(ExecutorError::other("not supported")),
            };

            match res {
                Ok(res) => {
                    match receiver {
                        Receiver::Ws => ws_proxy.send(id, Ok(res)).await?,
                        Receiver::Gw => gw.send(id, Ok(res)).await?,
                    };
                }
                Err(err) if err.is_request() => {
                    let err = ErrorKind::RequestError(err.unwrap_request());
                    match receiver {
                        Receiver::Ws => {
                            ws_proxy.send(id, Err(err.into())).await?;
                        }
                        Receiver::Gw => {
                            gw.send(id, Err(err.into())).await?;
                        }
                    };
                }
                Err(err) => {
                    tracing::error!("{err}");
                    let err = Err(ErrorKind::Unhandled {
                        cause: format!("{err}"),
                    }
                    .into());
                    match receiver {
                        Receiver::Ws => {
                            ws_proxy.send(id, err).await?;
                        }
                        Receiver::Gw => {
                            gw.send(id, err).await?;
                        }
                    };
                }
            }
        }
    }
}
