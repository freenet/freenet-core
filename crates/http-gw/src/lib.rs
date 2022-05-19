mod client_proxy;

pub use client_proxy::HttpGateway;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[cfg(feature = "local")]
pub mod local_node {
    use std::net::{Ipv4Addr, SocketAddr};

    use locutus_dev::{ContractStore, LocalNode};
    use locutus_node::{
        either, ClientError, ClientEventsCombinator, ClientEventsProxy, ErrorKind, WebSocketProxy,
    };

    use crate::{DynError, HttpGateway};

    const MAX_SIZE: i64 = 10 * 1024 * 1024;

    async fn config_node() -> Result<LocalNode, DynError> {
        let tmp_path = std::env::temp_dir().join("locutus");
        let contract_store = ContractStore::new(tmp_path.join("contracts"), MAX_SIZE);
        LocalNode::new(contract_store).await
    }

    pub async fn set_local_node() -> Result<(), DynError> {
        let mut local_node = config_node().await?;
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
