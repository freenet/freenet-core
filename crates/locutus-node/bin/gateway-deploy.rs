use anyhow::anyhow;
use libp2p::identity::Keypair;
use locutus_node::*;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let key = Keypair::generate_ed25519();
    let mut config = NodeConfig::default();
    config.with_key(key);
    let node = config.build_libp2p()?;
    node.run()
        .await
        .map_err(|_| anyhow!("failed to start"))
}
