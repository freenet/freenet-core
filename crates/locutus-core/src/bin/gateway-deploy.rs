use std::error::Error;

use clap::Parser;
use libp2p::identity::Keypair;
use locutus_core::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    #[cfg(feature = "websocket")]
    {
        let ws_interface = WebSocketProxy::start_server(([127, 0, 0, 1], 5000)).await?;
        let key = Keypair::generate_ed25519();
        let mut builder = NodeBuilder::new([Box::new(ws_interface)]);
        builder.with_key(key);
        let config = NodeConfig::parse();
        let node = builder.build(config).await?;
        node.run()
            .await
            .map_err(|_| "failed to start".to_owned().into())
    }
    #[cfg(not(feature = "websocket"))]
    {
        "Need at least the websocket API enabled".to_owned().into()
    }
}
