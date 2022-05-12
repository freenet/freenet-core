use std::net::{Ipv4Addr, SocketAddr};

use http_gw::HttpGateway;
use locutus_node::{ClientEventsCombinator, ClientEventsProxy, WebSocketProxy};
use locutus_stdlib::prelude::Contract;
use tracing::metadata::LevelFilter;
use tracing_subscriber::util::SubscriberInitExt;

pub(crate) type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

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
        let sample = Contract::new(vec![1, 2, 3, 4].into(), vec![].into());
        tracing::info!("available contract: {}", sample.key().encode());
        // 8xzpWrEm4bvnYBrneF3fTbNFN2JYpRKAZ2M7QgsmmBLS

        let socket: SocketAddr = (Ipv4Addr::LOCALHOST, 50509).into();
        let (http_handle, filter) = HttpGateway::as_filter();
        let ws_handle = WebSocketProxy::as_upgrade(socket, filter).await?;
        let mut all_clients =
            ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        loop {
            let (id, req) = all_clients.recv().await?;
            tracing::info!("client {id}, req -> {req}");
        }
    })
}
