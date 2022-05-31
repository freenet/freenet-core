mod client_proxy;

pub use client_proxy::HttpGateway;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[cfg(feature = "local")]
pub mod local_node {
    use std::net::{Ipv4Addr, SocketAddr};
    use tokio::sync::broadcast;

    use locutus_dev::{ContractStore, LocalNode};
    use locutus_node::{
        either, ClientError, ClientEventsCombinator, ClientEventsProxy, ErrorKind, PeerKey,
        SqlitePool, WebSocketProxy,
    };
    use locutus_runtime::StateStore;

    use crate::{DynError, HttpGateway};

    pub async fn set_local_node(
        mut local_node: LocalNode,
        peer_key: PeerKey,
        contract_store: ContractStore,
        state_store: StateStore<SqlitePool>,
    ) -> Result<(), DynError> {
        let socket: SocketAddr = (Ipv4Addr::LOCALHOST, 50509).into();
        let (mut http_handle, filter) = HttpGateway::as_filter(
            contract_store.clone(),
            state_store.clone(),
            local_node.clone(),
            peer_key.clone(),
        );
        let ws_handle = WebSocketProxy::as_upgrade(socket, filter).await?;
        //let mut all_clients =
        //    ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        loop {
            let (id, req) = http_handle.recv().await?;
            tracing::debug!("client {id}, req -> {req}");
            match local_node.handle_request(req).await {
                Ok(res) => {
                    http_handle.send(id, Ok(res));
                }
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
