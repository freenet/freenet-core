use anyhow::anyhow;
use libp2p::identity::Keypair;
use locutus_node::*;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let key = Keypair::generate_ed25519();
    let mut node = NodeConfig::default().with_key(key).build_in_memory()?;
    node.listen_on()
        .await
        .map_err(|_| anyhow!("failed to start"))
}
