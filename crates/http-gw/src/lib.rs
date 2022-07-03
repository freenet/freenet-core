pub(crate) mod errors;
mod http_gateway;
pub(crate) mod state_handling;
pub(crate) mod web_handling;

pub use http_gateway::HttpGateway;
use locutus_node::{either::Either, ClientError, ClientId, ClientRequest, HostResponse};

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;
type HostResult = (ClientId, Result<HostResponse, ClientError>);
type ClientHandlingMessage = (
    ClientRequest,
    Either<tokio::sync::mpsc::UnboundedSender<HostResult>, ClientId>,
);

#[cfg(feature = "local")]
pub mod local_node {
    use std::net::{Ipv4Addr, SocketAddr};

    use locutus_dev::LocalNode;
    use locutus_node::{
        either, ClientError, ClientEventsProxy, ErrorKind, RequestError, WebSocketProxy,
    };

    use crate::{DynError, HttpGateway};

    pub async fn set_local_node(mut local_node: LocalNode) -> Result<(), DynError> {
        let (mut http_handle, filter) = HttpGateway::as_filter();
        let socket: SocketAddr = (Ipv4Addr::LOCALHOST, 50509).into();
        let _ws_handle = WebSocketProxy::as_upgrade(socket, filter).await?;
        // FIXME: use combinator
        // let mut all_clients =
        //    ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        loop {
            let (id, req) = http_handle.recv().await?;
            tracing::debug!("client {id}, req -> {req}");
            match local_node.handle_request(req).await {
                Ok(res) => {
                    http_handle.send(id, Ok(res)).await?;
                }
                Err(either::Left(RequestError::Disconnect)) => {}
                Err(either::Left(err)) => {
                    log::error!("{err}");
                    http_handle
                        .send(id, Err(ClientError::from(ErrorKind::from(err))))
                        .await?;
                }
                Err(either::Right(err)) => {
                    log::error!("{err}");
                    http_handle
                        .send(id, Err(ErrorKind::Unhandled(format!("{err}")).into()))
                        .await?;
                }
            }
        }
    }
}
