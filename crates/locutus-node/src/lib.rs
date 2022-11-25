pub(crate) mod errors;
mod http_gateway;
pub(crate) mod web_handling;

pub use http_gateway::HttpGateway;
use locutus_core::{locutus_runtime::ContractKey, ClientId, HostResult};
use locutus_stdlib::api::{ClientError, ClientRequest, HostResponse};

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug)]
enum ClientConnection {
    NewConnection(tokio::sync::mpsc::UnboundedSender<HostCallbackResult>),
    Request {
        client_id: ClientId,
        req: ClientRequest<'static>,
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
    use std::net::{Ipv4Addr, SocketAddr};

    use locutus_core::{
        either, ClientEventsProxy, Executor, OpenRequest, RequestError, WebSocketProxy,
    };
    use locutus_stdlib::api::{ClientError, ErrorKind};

    use crate::{DynError, HttpGateway};

    pub async fn run_local_node(mut executor: Executor) -> Result<(), DynError> {
        let (mut http_handle, filter) = HttpGateway::as_filter();
        let socket: SocketAddr = (Ipv4Addr::LOCALHOST, 50509).into();
        let _ws_handle = WebSocketProxy::as_upgrade(socket, filter).await?;
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
            tracing::debug!("client {id}, req -> {request}");
            match executor
                .handle_request(id, request, notification_channel)
                .await
            {
                Ok(res) => {
                    http_handle.send(id, Ok(res)).await?;
                }
                Err(either::Left(RequestError::Disconnect)) => {}
                Err(either::Left(err)) => {
                    log::error!("{err}");
                    http_handle
                        .send(
                            id,
                            Err(ClientError::from(ErrorKind::Other(format!("{err}")))),
                        )
                        .await?;
                }
                Err(either::Right(err)) => {
                    log::error!("{err}");
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
