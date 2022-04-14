use std::net::{Ipv4Addr, SocketAddr};

use http_gw::HttpGateway;
use locutus_node::{ClientEventsCombinator, ClientEventsProxy, WebSocketProxy};
use locutus_stdlib::prelude::Contract;
use tracing_subscriber::util::SubscriberInitExt;

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let sub = tracing_subscriber::fmt().with_level(true).finish();
    sub.init();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let sample = Contract::new(vec![1, 2, 3, 4]);
        tracing::info!("available contract: {}", sample.key().hex_encode());
        // a482fdc4e226d57674e9a9086fc79e97deb5a648922c478e6347b32815d810b1df289553cf6f501c4c230a0b0fc88b58079e7d6798ca3278ecb2ce3db67cb1ab

        let socket: SocketAddr = (Ipv4Addr::LOCALHOST, 50509).into();
        let (http_handle, filter) = HttpGateway::as_filter();
        let ws_handle = WebSocketProxy::as_upgrade(socket, filter).await?;
        let mut all_clients =
            ClientEventsCombinator::new([Box::new(ws_handle), Box::new(http_handle)]);
        loop {
            let (id, req) = all_clients.recv().await?;
            tracing::info!("client {id}, req -> {req:?}");
        }
    })
}
