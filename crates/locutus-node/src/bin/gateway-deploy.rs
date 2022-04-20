use std::error::Error;

use libp2p::identity::Keypair;
use locutus_node::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    #[cfg(feature = "websocket")]
    {
        let ws_interface = WebSocketProxy::start_server(([127, 0, 0, 1], 5000)).await?;
        let key = Keypair::generate_ed25519();
        let mut config = NodeConfig::new([Box::new(ws_interface)]);
        config.with_key(key);
        let node = config.build()?;
        node.run()
            .await
            .map_err(|_| "failed to start".to_owned().into())
    }
    #[cfg(not(feature = "websocket"))]
    {
        "Need at least the websocket API enabled".to_owned().into()
    }
}
