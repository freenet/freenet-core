pub(crate) mod errors;
mod http_gateway;
pub(crate) mod web_handling;

pub use http_gateway::HttpGateway;
use locutus_core::{locutus_runtime::ContractKey, AuthToken, ClientId, HostResult};
use locutus_stdlib::{
    client_api::{ClientError, ClientRequest, HostResponse},
    prelude::ContractInstanceId,
};

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
    use std::net::SocketAddr;

    use locutus_core::{
        either::{self, Either},
        ClientEventsProxy, Executor, OpenRequest, WebSocketProxy,
    };
    use locutus_stdlib::client_api::{ClientRequest, ErrorKind, RequestError};

    use crate::{DynError, HttpGateway};

    pub async fn run_local_node(
        mut executor: Executor,
        socket: SocketAddr,
    ) -> Result<(), DynError> {
        let (mut http_handle, router) = HttpGateway::as_router();
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
                    executor.contract_op(op, id, notification_channel).await
                }
                ClientRequest::DelegateOp(op) => {
                    let attested_contract = token.and_then(|token| {
                        http_handle.attested_contracts.get(&token).map(|(t, _)| t)
                    });
                    executor.delegate_op(op, attested_contract)
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
                    Err(Either::Left(Box::new(RequestError::Disconnect)))
                }
                _ => Err(Either::Right("not supported".into())),
            };

            match res {
                Ok(res) => {
                    http_handle.send(id, Ok(res)).await?;
                }
                Err(either::Left(err)) => {
                    let request_error = *err.clone();
                    match *err {
                        RequestError::Disconnect => {}
                        _ => {
                            tracing::error!("{err}");
                            http_handle
                                .send(id, Err(ErrorKind::from(request_error).into()))
                                .await?;
                        }
                    }
                }
                Err(either::Right(err)) => {
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
