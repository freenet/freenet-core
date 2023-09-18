pub(crate) mod errors;
mod http_gateway;
pub(crate) mod web_handling;

use freenet_core::{locutus_runtime::ContractKey, AuthToken, ClientId, HostResult};
use freenet_stdlib::{
    client_api::{ClientError, ClientRequest, HostResponse},
    prelude::ContractInstanceId,
};
pub use http_gateway::HttpGateway;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum ClientConnection {
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
enum HostCallbackResult {
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

    use freenet_core::{ClientEventsProxy, Executor, ExecutorError, OpenRequest, WebSocketProxy};
    use freenet_stdlib::client_api::{ClientRequest, ErrorKind};

    use crate::{DynError, HttpGateway};

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
        let (mut http_handle, router) = HttpGateway::as_router(&socket);
        let _ws_handle = WebSocketProxy::as_upgrade(socket, router).await?;
        // FIXME: use combinator
        // let mut all_clients =
        //    ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        loop {
            let OpenRequest {
                id,
                request,
                notification_channel,
                token,
                ..
            } = http_handle.recv().await?;
            tracing::trace!(cli_id = %id, "got request -> {request}");

            let res = match *request {
                ClientRequest::ContractOp(op) => {
                    executor
                        .contract_requests(op, id, notification_channel)
                        .await
                }
                ClientRequest::DelegateOp(op) => {
                    let attested_contract = token.and_then(|token| {
                        http_handle.attested_contracts.get(&token).map(|(t, _)| t)
                    });
                    executor.delegate_request(op, attested_contract)
                }
                ClientRequest::Disconnect { cause } => {
                    if let Some(cause) = cause {
                        tracing::info!("disconnecting cause: {cause}");
                    }
                    if let Some(rm_token) = http_handle
                        .attested_contracts
                        .iter()
                        .find_map(|(k, (_, eid))| (eid == &id).then(|| k.clone()))
                    {
                        http_handle.attested_contracts.remove(&rm_token);
                    }
                    continue;
                }
                _ => Err(ExecutorError::other("not supported")),
            };

            match res {
                Ok(res) => {
                    http_handle.send(id, Ok(res)).await?;
                }
                Err(err) => {
                    tracing::error!("{err}");
                    http_handle
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
