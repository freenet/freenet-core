mod client_proxy;

pub use client_proxy::HttpGateway;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[cfg(feature = "local")]
pub mod local_node {
    use std::net::{Ipv4Addr, SocketAddr};

    use locutus_dev::{ContractStore, LocalNode};
    use locutus_node::{
        either, ClientError, ClientEventsCombinator, ClientEventsProxy, ErrorKind, SqlitePool,
        WebSocketProxy,
    };
    use locutus_runtime::StateStore;

    use crate::{DynError, HttpGateway};

    pub async fn config_node(
        contract_store: ContractStore,
        state_store: StateStore<SqlitePool>,
    ) -> Result<LocalNode, DynError> {
        LocalNode::new(contract_store, state_store).await
    }

    pub async fn set_local_node(mut local_node: LocalNode, contract_store: ContractStore, state_store: StateStore<SqlitePool>) -> Result<(), DynError> {
        let socket: SocketAddr = (Ipv4Addr::LOCALHOST, 50509).into();
        let (http_handle, filter) = HttpGateway::as_filter();
        let ws_handle = WebSocketProxy::as_upgrade(socket, filter).await?;
        let mut all_clients =
            ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        loop {
            let (id, req) = all_clients.recv().await?;
            tracing::debug!("client {id}, req -> {req}");
            match local_node.handle_request(req).await {
                Ok(res) => {
                    all_clients.send(id, Ok(res));
                }
                Err(either::Left(err)) => {
                    log::error!("{err}");
                    all_clients
                        .send(id, Err(ClientError::from(ErrorKind::from(err))))
                        .await?;
                }
                Err(either::Right(err)) => {
                    log::error!("{err}");
                    all_clients
                        .send(id, Err(ErrorKind::Unhandled(format!("{err}")).into()))
                        .await?;
                }
            }
        }
    }
}
