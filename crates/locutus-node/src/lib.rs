pub(crate) mod errors;
mod http_gateway;
pub(crate) mod web_handling;

pub use http_gateway::HttpGateway;
use locutus_core::{locutus_runtime::ContractKey, ClientId, HostResult};
use locutus_stdlib::client_api::{ClientError, ClientRequest, HostResponse};

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum ClientConnection {
    NewConnection(tokio::sync::mpsc::UnboundedSender<HostCallbackResult>),
    Request {
        client_id: ClientId,
        req: Box<ClientRequest<'static>>,
    },
}

#[derive(Debug)]
enum HostCallbackResult {
    NewId(ClientId),
    Result {
        id: ClientId,
        result: Result<HostResponse, ClientError>,
    },
    SubscriptionChannel {
        key: ContractKey,
        id: ClientId,
        callback: tokio::sync::mpsc::UnboundedReceiver<HostResult>,
    },
}

pub mod local_node {
    use std::net::SocketAddr;

    use locutus_core::{either, ClientEventsProxy, Executor, OpenRequest, WebSocketProxy};
    use locutus_stdlib::client_api::{ErrorKind, RequestError};

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
                ..
            } = http_handle.recv().await?;
            tracing::trace!(cli_id = %id, "got request -> {request}");
            match executor
                .handle_request(id, *request, notification_channel)
                .await
            {
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
