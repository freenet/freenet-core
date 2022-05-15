#![cfg(feature = "local")]
use std::net::{Ipv4Addr, SocketAddr};

use http_gw::HttpGateway;
use locutus_dev::{ContractStore, LocalNode};
use locutus_node::{
    either, ClientError, ClientEventsCombinator, ClientEventsProxy, ErrorKind, WebSocketProxy,
};
use tracing::metadata::LevelFilter;
use tracing_subscriber::util::SubscriberInitExt;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

fn main() -> Result<(), DynError> {
    let sub = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .with_level(true)
        .finish();
    sub.init();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let mut local_node = set_local_node();

        let socket: SocketAddr = (Ipv4Addr::LOCALHOST, 50509).into();
        let (http_handle, filter) = HttpGateway::as_filter();
        let ws_handle = WebSocketProxy::as_upgrade(socket, filter).await?;
        let mut all_clients =
            ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        loop {
            let (id, req) = all_clients.recv().await?;
            tracing::info!("client {id}, req -> {req}");
            match local_node.handle_request(req) {
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
    })
}

const MAX_SIZE: i64 = 10 * 1024 * 1024;

fn set_local_node() -> LocalNode {
    let tmp_path = std::env::temp_dir().join("locutus");
    let contract_store = ContractStore::new(tmp_path.join("contracts"), MAX_SIZE);
    LocalNode::new(contract_store)
}
